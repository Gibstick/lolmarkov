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
USERS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS users(
    user_id INTEGER PRIMARY KEY,
    username TEXT NOT NULL,
    display_name TEXT NOT NULL,
    discriminator TEXT NOT NULL
);"""

# See https://discordpy.readthedocs.io/en/v0.16.12/api.html#channel
CHANNELS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS channels(
    channel_id INTEGER PRIMARY KEY,
    channel_name NOT NULL
);"""

# See https://discordpy.readthedocs.io/en/v0.16.12/api.html#message
MESSAGES_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS messages(
    message_id INTEGER PRIMARY KEY,
    timestamp INTEGER NOT NULL,
    user_id INTEGER NOT NULL,
    author_username TEXT NOT NULL,
    channel_id INTEGER NOT NULL,
    content TEXT,
    clean_content TEXT
);"""

MENTIONS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS mentions(
    from_user_id INTEGER NOT NULL,
    to_user_id INTEGER NOT NULL,
    message_id INTEGER NOT NULL
);"""


class MyClient(discord.Client):
    def __init__(self, update, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._started = False

        self._conn = sqlite3.connect("discord_archive.sqlite3")
        self._conn.execute(USERS_TABLE_DDL)
        self._conn.execute(CHANNELS_TABLE_DDL)
        self._conn.execute(MESSAGES_TABLE_DDL)
        self._conn.execute(MENTIONS_TABLE_DDL)
        self._conn.commit()

        # Background task to periodically commit
        self.task = self.loop.create_task(self.commit_task())

        self.update = update

    async def commit_task(self):
        await self.wait_until_ready()
        last_count = int(next(self._conn.execute("SELECT COUNT(*) FROM messages;"))[0])

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
            yield (int(user.id), user.name, user.display_name, user.discriminator)

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
        channels = (
            c for c in self.get_all_channels() if await self.archive_permission(c)
        )

        async for channel in channels:
            if channel.type != discord.ChannelType.text:
                continue

            print("Archiving channel {}".format(channel.name))

            before, after = None, None
            if update:
                cursor = self._conn.execute(
                    """
                  SELECT message_id, max(timestamp) FROM messages
                  WHERE channel_id = ?""",
                    (int(channel.id),),
                )
                row = cursor.fetchone() or (None, None)
                if row[0]:
                    after = discord.Object(row[0])
            else:
                cursor = self._conn.execute(
                    """
                  SELECT message_id, min(timestamp) FROM messages
                  WHERE channel_id = ?""",
                    (int(channel.id),),
                )
                row = cursor.fetchone() or (None, None)
                if row[0]:
                    before = discord.Object(row[0])
            async for message in channel.history(
                before=before, after=after, limit=None
            ):

                # Get UTC unix timestamp from a naive datetime object
                ts = message.created_at.replace(tzinfo=timezone.utc).timestamp()
                mentions = [
                    (int(message.id), int(message.author.id), int(user.id))
                    for user in message.mentions
                ]
                yield (
                    (
                        int(message.id),
                        ts,
                        int(message.author.id),
                        message.author.name,
                        int(message.channel.id),
                        message.content,
                        message.clean_content,
                    ),
                    (
                        message.author.id,
                        message.author.name,
                        message.author.display_name,
                        message.author.discriminator,
                    ),
                    (mentions),
                )

    async def on_ready(self):
        if self._started:
            return
        self._started = True

        try:
            # TODO: async-aware sqlite3 library that async generators
            print("Inserting channels")
            async for t in self.channel_tuple_generator():
                self._conn.execute("INSERT OR IGNORE INTO channels VALUES(?, ?)", t)

            print("Inserting users")
            users = set()  # Keep track of user ids we've inserted
            async for t in self.user_tuple_generator():
                users.add(t[0])
                self._conn.execute("INSERT OR IGNORE INTO users VALUES(?, ?, ?, ?)", t)

            self._conn.commit()

            print("Inserting messages")

            async for t in self.message_tuple_generator(update=self.update):
                # Users that have left the server won't be in the users table,
                # and this breaks the foreign key constraint. We insert them
                # here if necessary.
                message, user, mentions = t
                if user[0] not in users:
                    self._conn.execute(
                        "INSERT OR IGNORE INTO users VALUES (?, ?, ?, ?)", user
                    )
                    users.add(user[0])
                self._conn.execute(
                    "INSERT OR IGNORE INTO messages VALUES " "(?, ?, ?, ?, ?, ?, ?)",
                    message,
                )

                for mention in mentions:
                    self._conn.execute(
                        "INSERT INTO mentions VALUES (?, ?, ?)",
                        mention
                    )
            self._conn.commit()

            print("Done!")
        except Exception as e:
            print(e)
        finally:
            self.task.cancel()
            await self.logout()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--config", help="Config file path", default="config.ini")
    parser.add_argument(
        "-u",
        "--update",
        help="Grab newer messages only",
        action="store_true",
        default=False,
    )
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.config)
    token = util.try_config(config, "MAIN", "Token")

    client = MyClient(args.update)
    client.run(token)


if __name__ == "__main__":
    main()
