import argparse
import asyncio
import configparser
import json
import sqlite3
import traceback
from datetime import timezone

import discord

import util

# See https://discordpy.readthedocs.io/en/v0.16.12/api.html#user
USERS_TABLE_DDL = \
    """
CREATE TABLE IF NOT EXISTS users(
    id INTEGER PRIMARY KEY,
    username TEXT NOT NULL,
    display_name TEXT NOT NULL,
    discriminator TEXT NOT NULL
);"""

# See https://discordpy.readthedocs.io/en/v0.16.12/api.html#channel
CHANNELS_TABLE_DDL = \
    """
CREATE TABLE IF NOT EXISTS channels(
    id INTEGER PRIMARY KEY,
    channel_name NOT NULL
);"""

# See https://discordpy.readthedocs.io/en/v0.16.12/api.html#message
# TODO: attachments table?
MESSAGES_TABLE_DDL = \
    """
CREATE TABLE IF NOT EXISTS messages(
    id INTEGER PRIMARY KEY,
    timestamp INTEGER NOT NULL,
    author_id INTEGER NOT NULL,
    author_username TEXT NOT NULL,
    channel_id INTEGER NOT NULL,
    content TEXT,
    clean_content TEXT,
    attachments BLOB,
    FOREIGN KEY(author_id) REFERENCES users(id),
    FOREIGN KEY(channel_id) REFERENCES channels(id)
);"""


class MyClient(discord.Client):
    def __init__(self, update, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._started = False

        self._conn = sqlite3.connect("discord_archive.sqlite3")
        self._conn.execute("PRAGMA foreign_keys = ON;")
        self._conn.execute(USERS_TABLE_DDL)
        self._conn.execute(CHANNELS_TABLE_DDL)
        self._conn.execute(MESSAGES_TABLE_DDL)
        self._conn.commit()

        # Background task to periodically commit
        self.commit_task = self.loop.create_task(self.commit_task())

        self.update = update

    async def commit_task(self):
        await self.wait_until_ready()
        last_count = int(
            next(self._conn.execute("SELECT COUNT(*) FROM messages;"))[0])

        while not self.is_closed():
            self._conn.commit()
            for row in self._conn.execute("SELECT COUNT(*) FROM messages;"):
                count = int(row[0])
                diff = count - last_count
                print("Got {} messages with total {}".format(diff, count))
                last_count = count
            await asyncio.sleep(5)

    async def user_tuple_generator(self):
        """Yields tuples to be inserted into the users table."""
        for user in self.get_all_members():
            yield (int(user.id), user.name, user.display_name,
                   user.discriminator)

    async def channel_tuple_generator(self):
        """Yields tuples to be inserted into the channels table."""
        for channel in self.get_all_channels():
            if channel.type != discord.ChannelType.text:
                continue
            yield (int(channel.id), channel.name)

    async def archive_permission(self, channel):
        """Return if we have read and history permission for a Channel."""
        perms = channel.permissions_for(channel.guild.me)
        return perms.read_messages and perms.read_message_history

    async def message_tuple_generator(self, update=False):
        """Yields tuples to be inserted into the messages/users table.

        If update is true, grab messages after the latest message in the table
        for the channel. Otherwise, grab messages before the oldest message in
        the table for the channel.

        Yields a tuple of (message tuple, user tuple)."""
        # Archive all channels that we have access to
        channels = (c for c in self.get_all_channels()
                    if await self.archive_permission(c))

        async for channel in channels:
            if channel.type != discord.ChannelType.text:
                continue

            print("Archiving channel {}".format(channel.name))

            before, after = None, None
            if update:
                for row in self._conn.execute(
                        """
                  SELECT id, max(timestamp) FROM messages
                  WHERE channel_id = ?""", (int(channel.id), )):
                    after = discord.Object(row[0]) if row[0] else None

            else:
                for row in self._conn.execute(
                        """
                  SELECT id, min(timestamp) FROM messages
                  WHERE channel_id = ?""", (int(channel.id), )):
                    before = discord.Object(row[0]) if row[0] else None

            async for message in channel.history(before=before,
                                                 after=after,
                                                 limit=None):

                # TODO: fix attachments
                attachments = None
                # Get UTC unix timestamp from a naive datetime object
                ts = message.created_at.replace(
                    tzinfo=timezone.utc).timestamp()
                yield ((int(message.id), ts, int(message.author.id),
                        message.author.name, int(message.channel.id),
                        message.content, message.clean_content, attachments),
                       (message.author.id, message.author.name,
                        message.author.display_name,
                        message.author.discriminator))

    async def on_ready(self):
        if self._started:
            return
        self._started = True

        try:
            # TODO: async-aware sqlite3 library that async generators
            print("Inserting channels")
            async for t in self.channel_tuple_generator():
                self._conn.execute(
                    "INSERT OR IGNORE INTO channels VALUES(?, ?)", t)

            print("Inserting users")
            users = set()  # Keep track of user ids we've inserted
            async for t in self.user_tuple_generator():
                users.add(t[0])
                self._conn.execute(
                    "INSERT OR IGNORE INTO users VALUES(?, ?, ?, ?)", t)

            self._conn.commit()

            print("Inserting messages")

            async for t in self.message_tuple_generator(update=self.update):
                # Users that have left the server won't be in the users table,
                # and this breaks the foreign key constraint. We insert them
                # here if necessary.
                message, user = t
                if user[0] not in users:
                    self._conn.execute(
                        "INSERT OR IGNORE INTO users VALUES (?, ?, ?, ?)",
                        user)
                    users.add(user[0])
                self._conn.execute(
                    "INSERT OR IGNORE INTO messages VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?)", message)
            self._conn.commit()

            print("Done!")
        except Exception as e:
            print(e)
        finally:
            self.commit_task.cancel()
            await self.logout()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c",
                        "--config",
                        help="Config file path",
                        default="config.ini")
    parser.add_argument("-u",
                        "--update",
                        help="Grab newer messages only",
                        action="store_true",
                        default=False)
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.config)
    token = util.try_config(config, "MAIN", "Token")

    client = MyClient(args.update)
    client.run(token)


if __name__ == "__main__":
    main()
