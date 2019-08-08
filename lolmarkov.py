import argparse
import asyncio
import concurrent.futures
import configparser
import functools
import json
import os
import sys
import random
import traceback
from collections import namedtuple

import aiosqlite
import discord
import markovify
from async_lru import alru_cache
from discord.ext import commands

import util

DuckUser = namedtuple("DuckUser", ["id", "name", "discriminator"])


async def database_user(conn, argument):
    """"Look up a user in the database and return a NamedTuple mimicking a discord.User"""
    cursor = await conn.execute(
        "SELECT id, username, discriminator FROM users WHERE username||'#'||discriminator == ? LIMIT 1",
        (argument, ))
    users = await cursor.fetchall()

    if not users:
        raise commands.CommandError(f"User {argument} not found in data set.")

    user = users[0]
    member = DuckUser(id=user[0], name=user[1], discriminator=user[2])
    return member


def last_replace(s, old, new):
    li = s.rsplit(old, 1)
    return new.join(li)


def UWU(msg):
    vowels = ['a', 'e', 'i', 'o', 'u', 'A', 'E', 'I', 'O', 'U']
    faces = ["(・`ω´・)", ";;w;;", "owo", "UwU", ">w<", "^w^"]
    msg = msg.replace('L', 'W').replace('l', 'w')
    msg = msg.replace('R', 'W').replace('r', 'w')

    msg = last_replace(msg, '!', '! {}'.format(random.choice(faces)))
    msg = last_replace(msg, '?', '? owo')
    msg = last_replace(msg, '.', '. {}'.format(random.choice(faces)))

    for v in vowels:
        if 'n{}'.format(v) in msg:
            msg = msg.replace('n{}'.format(v), 'ny{}'.format(v))
        if 'N{}'.format(v) in msg:
            msg = msg.replace('N{}'.format(v),
                              'N{}{}'.format('Y' if v.isupper() else 'y', v))

    return msg


class MarkovCog(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self._conn = None  # initialized in on_ready
        self._model = None
        self._model_attrib = None
        self._user_converter = commands.UserConverter()
        self._pool = concurrent.futures.ProcessPoolExecutor(max_workers=1)

    @commands.Cog.listener()
    async def on_ready(self):
        if self._conn is None:
            self._conn = await aiosqlite.connect(
                "file:./discord_archive.sqlite3?mode=ro", uri=True)

        if self._model_attrib is None:
            print("Resetting nickname(s)...")
            for guild in self.bot.guilds:
                await guild.me.edit(nick=None)
        print("Ready!")

    async def set_name(self, ctx, member):
        """Change nickname to indicate that current model is for member."""
        me = ctx.guild.me
        basename = me.name

        # Basename + username + discriminator + # + spaces and parens
        available_length = 32 - len(basename) - 8
        if len(member.name) > available_length:
            member_name = f"{member.name[:available_length-3]}..."
        else:
            member_name = member.name

        trimmed_member = f"{member_name}#{member.discriminator}"
        self._model_attrib = f"{member.name}#{member.discriminator}"

        await me.edit(nick=f"{basename} ({trimmed_member})")

    @alru_cache(maxsize=4)
    async def create_model(self, author_id: int, conn):
        model_path = os.path.join("models", f"{author_id}.json")

        try:
            with open(model_path, mode="r") as f:
                model = markovify.NewlineText.from_json(f.read())
        except (FileNotFoundError, json.decoder.JSONDecodeError):
            cursor = await conn.execute(
                "SELECT clean_content FROM messages WHERE author_id is ?",
                (author_id, ))
            messages = await cursor.fetchall()

            if len(messages) < 25:
                return None

            loop = asyncio.get_running_loop()
            model = await loop.run_in_executor(self._pool,
                                               markovify.NewlineText,
                                               ("\n".join(m[0]
                                                          for m in messages)))
            with open(model_path, mode="w") as f:
                f.write(model.to_json())

        return model

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
            new_model = await self.create_model(member.id, self._conn)

        if new_model is None:
            await ctx.send(f"Not enough data for user {member.name}")
            return

        self._model = new_model
        await self.set_name(ctx, member)
        await ctx.send(
            f"Switched model to {member.name}#{member.discriminator}")

    @switch.error
    async def switch_error(self, ctx, error):
        if isinstance(error, commands.BadArgument):
            await ctx.send(str(error))
        else:
            await ctx.send("Unable to switch data set.")
            print(error)
            traceback.print_exc()
        await ctx.message.add_reaction("❌")

    async def get_sentence(self, start=None):
        """Get one sentence from the model, with optional start parameter.

        Assumes that a model is active."""
        ATTEMPTS = 20
        ATTEMPTS_PER_ITER = 2
        MAX_OVERLAP_RATIO = 0.6
        assert (ATTEMPTS % ATTEMPTS_PER_ITER == 0)

        try:
            for _ in range(ATTEMPTS // ATTEMPTS_PER_ITER):
                if start:
                    sentence = self._model.make_sentence_with_start(
                        start,
                        tries=ATTEMPTS_PER_ITER,
                        strict=False,
                        max_overlap_ratio=MAX_OVERLAP_RATIO)
                else:
                    sentence = self._model.make_sentence(
                        start,
                        tries=ATTEMPTS_PER_ITER,
                        max_overlap_ratio=MAX_OVERLAP_RATIO)
                if sentence:
                    break
                else:
                    await asyncio.sleep(0)
        except KeyError:
            sentence = None

        return sentence

    @commands.command()
    async def talk(self, ctx, *, start: str = None):
        """Talk command with optional start parameter."""
        await self.talk_impl(ctx, uwu=False, start=start)

    @commands.command()
    async def talkuwu(self, ctx, *, start: str = None):
        """Like talk, but with uwu."""
        await self.talk_impl(ctx, uwu=True, start=start)

    async def talk_impl(self, ctx, uwu=False, start=None):
        """Talk command implementation."""
        if self._model is None:
            await ctx.message.add_reaction("❌")
            await ctx.send("No model is active.")
            return

        async with ctx.typing():
            sentence = await self.get_sentence(start)

        if sentence:
            if uwu:
                sentence = UWU(sentence)
            await ctx.send(f"{sentence}\n`{self._model_attrib}`")
        else:
            await ctx.message.add_reaction("❌")
            await ctx.send("Unable to get sentence.", delete_after=3)


def main():
    os.makedirs("models", exist_ok=True)
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
