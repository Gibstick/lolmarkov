import argparse
import asyncio
import concurrent.futures
import configparser
import functools
import json
import logging
import os
import sqlite3
import sys
import random
import traceback
from collections import namedtuple
from datetime import datetime
from typing import Iterable

import aiosqlite
import discord
import markovify
import psutil
from discord.ext import commands

import util

DuckUser = namedtuple("DuckUser", ["id", "name", "discriminator"])


class SentenceText(markovify.Text):
    """Like markovify.Text, but a list of Iterable of sentences can be passed in."""

    def generate_corpus(self, sentences: Iterable[str]):
        return map(self.word_split, filter(self.test_sentence_input,
                                           sentences))


async def database_user(conn, argument):
    """"Look up a user in the database and return a NamedTuple mimicking a discord.User"""
    cursor = await conn.execute(
        """SELECT id, username, discriminator FROM users
        WHERE username||'#'||discriminator == ?
        OR id is ?
        LIMIT 1""", (argument, argument))
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
        self._pool = concurrent.futures.ProcessPoolExecutor()

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

    async def cache_update(self, author_id: int, model_path: str, conn):
        """Invalidate the cache if author_id has newer messages than the json."""
        mtime = os.path.getmtime(model_path)
        cursor = await conn.execute(
            """
            SELECT max(timestamp) FROM messages
            WHERE author_id is ?
            """, (author_id, ))
        latest_timestamp = await cursor.fetchone()

        if latest_timestamp is None:
            return

        if latest_timestamp[0] > mtime:
            os.remove(model_path)

    async def create_model(self, author_id: int, conn):
        model_path = os.path.join("models", f"{author_id}.json")

        try:
            await self.cache_update(author_id, model_path, conn)
            with open(model_path, mode="r") as f:
                model = SentenceText.from_json(f.read())
        except (FileNotFoundError, json.decoder.JSONDecodeError):
            cursor = await conn.execute(
                """
                SELECT clean_content FROM messages
                WHERE author_id is ?
                ORDER BY timestamp DESC
                LIMIT 100000
                """, (author_id, ))
            messages = await cursor.fetchall()

            if len(messages) < 25:
                return None

            model = await self.bot.loop.run_in_executor(
                self._pool, SentenceText, [m[0] for m in messages])
            with open(model_path, mode="w") as f:
                f.write(model.to_json())

        return model

    async def user_lookup(self, ctx, arg):
        try:
            member = await self._user_converter.convert(ctx, arg)
        except commands.CommandError:
            member = await database_user(self._conn, arg)

        return member

    @commands.command()
    async def switch(self, ctx, *, arg: str):
        """Switch to a model for a different user."""
        # Due to the complicated fallback logic and the shared database
        # connection, we convert the argmuent manually.
        member = await self.user_lookup(ctx, arg)

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
        TRIES_PER_RATIO = 10
        make_fn = (functools.partial(self._model.make_sentence_with_start,
                                     start)
                   if start else self._model.make_sentence)

        max_overlap_ratio = 0.7
        while max_overlap_ratio <= 1.0:
            for _ in range(TRIES_PER_RATIO):
                sentence = make_fn(max_overlap_ratio=max_overlap_ratio)
                if sentence:
                    break
            else:
                max_overlap_ratio += 0.1
                await asyncio.sleep(0)
                continue
            break

        # If we still don't get a sentence, just give up and stop testing the
        # output so that we allow total repeats.
        if not sentence:
            sentence = make_fn(test_output=False, strict=False)

        return sentence

    @commands.command()
    async def talk(self, ctx, *, start: str = None):
        """Talk command with optional start parameter."""
        await self.talk_impl(ctx, uwu=False, start=start)

    @commands.command()
    async def talkuwu(self, ctx, *, start: str = None):
        """Like talk, but with uwu."""
        await self.talk_impl(ctx, uwu=True, start=start)

    async def react_and_error(self, ctx, message, reaction="❌",
                              delete_after=8):
        """React to the command with an "X" and print a temporary error message."""
        await ctx.message.add_reaction(reaction)
        await ctx.send(message, delete_after=delete_after)

    async def talk_impl(self, ctx, uwu=False, start=None):
        """Talk command implementation."""
        try:
            if self._model is None:
                return await self.react_and_error(ctx,
                                                  "No active model.",
                                                  delete_after=None)

            async with ctx.typing():
                try:
                    sentence = await self.get_sentence(start)
                except KeyError:
                    return await self.react_and_error(
                        ctx, f"{start} not found in data set.")
                except markovify.text.ParamError:
                    # TODO: Don't hardcode the number of words here.
                    return await self.react_and_error(
                        ctx,
                        "At most two words can be used for the start of a sentence."
                    )

            if sentence:
                if uwu:
                    sentence = UWU(sentence)
                await ctx.send(f"{sentence}\n`{self._model_attrib}`")
            else:
                await self.react_and_error(
                    ctx,
                    "Gave up after too many failures (too much similarity).",
                    "⏲")
        except:
            logging.exception("Unknown exception in get_sentence():")
            return await self.react_and_error(ctx, "Unknown error.")

    @commands.command()
    async def memory(self, ctx):
        available_mb = psutil.virtual_memory().available / 1024 / 1024  # MiB
        await ctx.send(f"Memory available: {available_mb} MiB")

    @commands.command()
    async def quote(self, ctx, target: str, *, keyword: str = ""):
        """Grab a random quote from target, optionally containing keyword."""
        try:
            member = await self.user_lookup(ctx, target)
        except commands.CommandError:
            await self.react_and_error(ctx, f"User {target} found")
            return

        author_id = member.id

        patterns = (f"% {keyword} %", f"{keyword} %", f"% {keyword}")
        query = (
            "SELECT clean_content, timestamp FROM messages WHERE author_id is ?\n"
            "AND (clean_content LIKE ? OR clean_content LIKE ? OR clean_content LIKE ?)\n"
            "ORDER BY RANDOM() LIMIT 1")
        cursor = await self._conn.execute(query, (author_id, ) + patterns)
        row = await cursor.fetchone()

        if not row:
            await self.react_and_error(ctx, "No matching quote found")
            return

        timestamp = datetime.utcfromtimestamp(int(row[1])).strftime("%c")
        random_quote = row[0]

        quote_attrib = f"{member.name}#{member.discriminator}"
        message = f"{random_quote}\n> {quote_attrib} {timestamp}"
        await ctx.send(message)

    @commands.command()
    async def sqlexec(self, ctx, *, query: str):
        """lol"""
        try:
            async with ctx.typing():
                cursor = await self._conn.execute(query)
                rows = await cursor.fetchall()
        except sqlite3.OperationalError as e:
            await self.react_and_error(ctx, f"Error: {str(e)}")
            return
        message = "\n".join(str(row) for row in rows[:10])
        message = f"```\n{message}\n```"
        await ctx.send(message)
        await ctx.send(f"Query returned {len(rows)} rows")


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
