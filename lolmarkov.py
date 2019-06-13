import argparse
import configparser
import functools
import sqlite3 #TODO: aiosqlite3
import sys
from collections import namedtuple

import discord
import markovify
from discord.ext import commands

import util


@functools.lru_cache(maxsize=32)
async def create_model(author_id: int, conn):
    # TODO: async
    c = conn.cursor()

    messages = c.execute("SELECT content FROM messages WHERE author_id is ?",
                         (author_id, )).fetchall()

    if len(messages) < 25:
        return None

    return markovify.NewlineText("\n".join(m[0] for m in messages))


DuckUser = namedtuple("DuckUser", ["id", "name", "discriminator"])


async def database_user(conn, argument):
    """"Look up a user in the database and return a NamedTuple mimicking a discord.User"""
    c = conn.cursor()
    users = c.execute(
        "SELECT id, username, discriminator FROM users WHERE username||'#'||discriminator == ? LIMIT 1",
        (argument, )).fetchall()

    if not users:
        raise commands.CommandError(
            f"User {argument} not found in data set.")

    user = users[0]
    member = DuckUser(id=user[0], name=user[1], discriminator=user[2])
    return member


class MarkovCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self._conn = sqlite3.connect("file:./discord_archive.sqlite3?mode=ro",
                                     uri=True)
        self._model = None
        self._user_converter = commands.UserConverter()

    @commands.Cog.listener()
    async def on_ready(self):
        print("Ready!")

    @commands.command()
    async def switch(self, ctx, *, arg: str):
        """Switch to a model for a different user."""
        # Due to the complicated fallback logic and the shared database
        # connection, we convert the argmuent manually.
        try:
            member = await self._user_converter.convert(ctx, arg)
        except commands.CommandError:
            member = await database_user(self._conn, arg)

        async with ctx.typing():
            new_model = await create_model(member.id, self._conn)

        if new_model is None:
            await ctx.send(f"Not enough data for user {member.name}")
            return

        self._model = new_model
        await ctx.send(
            f"Switched model to {member.name}#{member.discriminator}")

    @switch.error
    async def switch_error(self, ctx, error):
        if isinstance(error, commands.BadArgument):
            await ctx.send(str(error))
        else:
            await ctx.send("Unable to switch data set.\n{}".format(str(error)))
        await ctx.message.add_reaction("❌")

    @commands.command()
    async def talk(self, ctx, *, start: str = None):
        """Get one sentence from the model, with optional start parameter."""
        if self._model is None:
            await ctx.message.add_reaction("❌")
            await ctx.send("No model is active.")
            return

        TRIES = 500
        async with ctx.typing():
            try:
                if start:
                    sentence = self._model.make_sentence_with_start(
                        start, tries=TRIES, strict=False)
                else:
                    sentence = self._model.make_sentence(start, tries=TRIES)
            except KeyError:
                sentence = None

        if sentence:
            await ctx.send(sentence)
        else:
            await ctx.message.add_reaction("❌")
            await ctx.send("Unable to get sentence.", delete_after=3)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c",
                        "--config",
                        help="Config file path",
                        default="config_lolmarkov.ini")

    args = parser.parse_args()
    config = configparser.ConfigParser()
    config.read(args.config)
    token = util.try_config(config, "MAIN", "Token")

    bot = commands.Bot(command_prefix="$")
    bot.add_cog(MarkovCog(bot))
    bot.run(token)


if __name__ == "__main__":
    main()
