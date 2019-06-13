import argparse
import configparser
import functools
import sqlite3
import sys

import discord
import markovify

import util

from discord.ext import commands


@functools.lru_cache(maxsize=32)
def create_model(author_id: int, conn):
    c = conn.cursor()

    messages = c.execute("SELECT content FROM messages WHERE author_id is ?",
                         (author_id, )).fetchall()

    if len(messages) < 25:
        return None

    return markovify.NewlineText("\n".join(m[0] for m in messages))


class MarkovCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self._conn = sqlite3.connect("file:./discord_archive.sqlite3?mode=ro",
                                     uri=True)
        self._model = None

    @commands.Cog.listener()
    async def on_ready(self):
        print("Ready!")

    @commands.command()
    async def switch(self, ctx, *, member: discord.User = None):
        """Switch to a model for a different user."""
        async with ctx.typing():
            new_model = create_model(member.id, self._conn)
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
            print(error)
            await ctx.send(f"Unable to switch data set.")
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
                    sentence = self._model.make_sentence_with_start(start,
                                                                    tries=TRIES,
                                                                    strict=False)
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
