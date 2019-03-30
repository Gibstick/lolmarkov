import argparse
import asyncio
import configparser
import sqlite3

import discord

USERS_TABLE_DDL = \
    """
CREATE TABLE IF NOT EXISTS users(
    id INTEGER PRIMARY KEY,
    username TEXT NOT NULL,
    display_name TEXT NOT NULL,
    discriminator TEXT NOT NULL
);"""

CHANNELS_TABLE_DDL = \
    """
CREATE TABLE IF NOT EXISTS channels(
    id INTEGER PRIMARY KEY,
    channel_name NOT NULL
);"""

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
    FOREIGN KEY(author_id) REFERENCES users(id),
    FOREIGN KEY(channel_id) REFERENCES channels(id)
);"""


class MyClient(discord.Client):
    def __init__(self, channel, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._started = False
        self._channel = channel
        self._conn = sqlite3.connect("messages.sqlite3")
        self._conn.execute("PRAGMA foreign_keys = ON;")
        self._conn.execute(USERS_TABLE_DDL)
        self._conn.execute(CHANNELS_TABLE_DDL)
        self._conn.execute(MESSAGES_TABLE_DDL)
        self._conn.commit()

        # Background task to periodically commit
        self.commit_task = self.loop.create_task(self.commit_task())

    async def commit_task(self):
        await self.wait_until_ready()
        last_count = 0
        while not self.is_closed:
            self._conn.commit()
            for row in self._conn.execute("SELECT COUNT(*) FROM messages;"):
                count = int(row[0])
                diff = count - last_count
                if diff:
                    print("Got {} messages with total {}".format(
                          diff, count))
                else:
                    print("Got nothing with total {}".format(count))
                last_count = count
            await asyncio.sleep(5)

    async def user_tuple_generator(self):
        """Yields tuples to be inserted into the users table."""
        for user in self.get_all_members():
            yield (
                int(user.id),
                user.name,
                user.display_name,
                user.discriminator
            )

    async def channel_tuple_generator(self):
        """Yields tuples to be inserted into the channels table."""
        for channel in self.get_all_channels():
            yield (int(channel.id), channel.name)

    async def message_tuple_generator(self, before=None):
        """Yields tuples to be inserted into the messages/users table.

        Yields a tuple of (message tuple, user tuple)."""
        channel = discord.Object(self._channel)
        async for message in self.logs_from(channel, before=before,
                                            limit=99999999):
            yield (
                (int(message.id),
                 message.timestamp.timestamp(),
                 int(message.author.id),
                 message.author.name,
                 int(message.channel.id),
                 message.content,
                 message.clean_content),
                (message.author.id,
                 message.author.name,
                 message.author.display_name,
                 message.author.discriminator)
            )

    async def on_ready(self):
        if self._started:
            return
        self._started = True

        try:
            # TODO: async-aware sqlite3 library that async generators
            print("Inserting channels")
            async for t in self.channel_tuple_generator():
                self._conn.execute(
                    "INSERT OR IGNORE INTO channels VALUES(?, ?)", t
                )

            print("Inserting users")
            users = set()  # Keep track of user ids we've inserted
            async for t in self.user_tuple_generator():
                users.add(t[0])
                self._conn.execute(
                    "INSERT OR IGNORE INTO users VALUES(?, ?, ?, ?)", t
                )

            self._conn.commit()

            print("Inserting messages")
            for row in self._conn.execute(
                "SELECT id, min(timestamp) FROM messages "
                "GROUP BY channel_id "
                "HAVING channel_id = ?",
                (self._channel,)
            ):
                earliest_message = discord.Object(row[0])
                print("Resuming")
                break
            else:
                earliest_message = None

            async for t in \
                    self.message_tuple_generator(before=earliest_message):
                # Users that have left the server won't be in the users table,
                # and this breaks the foreign key constraint. We insert them
                # here if necessary.
                message, user = t
                if user[0] not in users:
                    self._conn.execute(
                        "INSERT OR IGNORE INTO users VALUES (?, ?, ?, ?)", user
                    )
                    users.add(user[0])
                self._conn.execute(
                    "INSERT OR IGNORE INTO messages VALUES "
                    "(?, ?, ?, ?, ?, ?, ?)",
                    message
                )
            self._conn.commit()

            print("Done!")
        finally:
            self.commit_task.cancel()
            await self.logout()


def try_config(config, heading, key):
    """Attempt to extract config[heading][key], with error handling.

    This function wraps config access with a try-catch to print out informative
    error messages and then exit."""
    try:
        section = config[heading]
    except KeyError:
        exit("Missing config section [{}]".format(heading))

    try:
        value = section[key]
    except KeyError:
        exit("Missing config key '{}' under section '[{}]'".format(
             key, heading))

    return value


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-c", "--config", help="Config file path", default="config.ini")
    args = parser.parse_args()

    config = configparser.ConfigParser()
    config.read(args.config)
    token = try_config(config, "MAIN", "Token")
    channel = try_config(config, "MAIN", "Channel")

    client = MyClient(channel)
    client.run(token)


if __name__ == "__main__":
    main()
