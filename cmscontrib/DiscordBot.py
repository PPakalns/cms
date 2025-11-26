#!/usr/bin/env python3

# Contest Management System - Discord Bot, Telegram-style rewrite
# Polls the DB periodically instead of receiving queue events.

import os
import yaml
import asyncio
import logging
import argparse
from typing import Dict, Callable, Awaitable, Optional

import discord
from discord import TextChannel, Message, Thread

from cms.conf import ConfigError
from cms.db.admin import Admin
from cms.db.contest import Contest
from cms.db.session import SessionGen
from cms.db import ask_for_contest, Announcement, Question, Participation
from cms.db.base import Base
from cms.conf import config
from cms.util import contest_id_from_args

logger = logging.getLogger(__name__)


def sqlalchemy_to_dict(obj: Base) -> dict:
    d = obj.get_attrs()
    d["id"] = obj.id
    return d


def esc(text: str) -> str:
    text = discord.utils.escape_markdown(text)
    text = discord.utils.escape_mentions(text)
    return text


def truncate(s: str, length: int) -> str:
    if len(s) <= length:
        return s
    return s[:length] + "..."

@discord.commands.command()
@discord.guild_only()
async def alive(ctx: discord.commands.context.ApplicationContext):
    if ctx.channel_id != ctx.bot.__getattribute__("channel_id"):
        return
    await ctx.send_response(content = "Bot is alive")


class DiscordBot(discord.Bot):
    def __init__(self, channel_id: int, token: str, contest_id: int | None = None):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.reactions = True
        super().__init__(intents=intents)

        self.token = token
        self.channel_id = channel_id
        self.contest_id = contest_id

        # Storage directories
        self.question_dir = os.path.join(config.global_.data_dir, "discord", "questions")
        self.announcement_dir = os.path.join(config.global_.data_dir, "discord", "announcements")
        os.makedirs(self.question_dir, exist_ok=True)
        os.makedirs(self.announcement_dir, exist_ok=True)

        # Stores loaded YAML
        self.question_state: Dict[int, dict] = self._load_state(self.question_dir)
        self.announcement_state: Dict[int, dict] = self._load_state(self.announcement_dir)

        self.target_channel: TextChannel | None = None

        self.add_application_command(
            alive
        )


    def _load_state(self, directory: str) -> Dict[int, dict]:
        data = {}
        with os.scandir(directory) as it:
            for f in it:
                if not f.is_file():
                    logger.warning(f"Non-file entry in {directory}: {f.name}")
                    continue
                try:
                    with open(os.path.join(directory, f.name)) as file:
                        d = yaml.safe_load(file)
                        data[d["id"]] = d
                except Exception:
                    logger.warning(f"Invalid YAML file in {directory}: {f.name}")
        return data

    async def on_ready(self):
        logger.info(f"Logged in as {self.user}")

        channel = self.get_channel(self.channel_id)

        if not isinstance(channel, TextChannel):
            logger.error(f"Channel {self.channel_id} not found or not a text channel.")
            return

        self.target_channel = channel

        logger.info("Starting DB poll loop")
        asyncio.create_task(self.db_loop())

    async def get_target_channel(self) -> TextChannel | None:
        return self.target_channel

    async def _store(
        self,
        obj: dict,
        store: Dict[int, dict],
        directory: str,
        changed_callback: Callable[[dict, dict], Awaitable[dict]],
    ):
        """Compare old/new DB object; call callback if changed; write YAML."""
        existing = store.get(obj["id"], dict())
        has_changed = False
        for (k, v) in obj.items():
            if existing.get(k, None) != v:
                has_changed = True

        if has_changed:
            new_obj = await changed_callback(existing, obj)
            store[obj["id"]] = new_obj
            with open(os.path.join(directory, f"{obj['id']}.yaml"), "w") as f:
                f.write(yaml.safe_dump(new_obj))
            logger.info(f"Saved {obj['id']} yaml")


    async def store_question(self, q: dict):
        await self._store(q, self.question_state, self.question_dir, self.question_callback)

    async def store_announcement(self, ann: dict):
        await self._store(ann, self.announcement_state, self.announcement_dir, self.announcement_callback)

    async def question_callback(self, old: dict, new: dict) -> dict:
        await question_update(self, old, new)
        return new

    async def announcement_callback(self, old: dict, new: dict) -> dict:
        await announcement_update(self, old, new)
        return new

    async def db_loop(self):
        while True:
            with SessionGen() as ses:
                query = ses.query(Question).join(Participation).join(Participation.contest).outerjoin(Question.admin)

                if self.contest_id is not None:
                    query = query.filter(Participation.contest_id == self.contest_id)

                qs = []
                for q in query.all():
                    d = sqlalchemy_to_dict(q)
                    d["user"] = q.participation.user.username
                    d["contest"] = q.participation.contest.name
                    d["admin"] = q.admin.name if q.admin else ""
                    qs.append(d)

                anns = []
                query = ses.query(Announcement).outerjoin(Admin).join(Contest)
                if self.contest_id is not None:
                    query = query.filter(Announcement.contest_id == self.contest_id)

                for a in query.all():
                    d = sqlalchemy_to_dict(a)
                    d["contest"] = a.contest.name
                    d["admin"] = a.admin.name if a.admin else ""
                    anns.append(d)

            # Process questions
            for q in qs:
                await self.store_question(q)

            # Process announcements
            for ann in anns:
                await self.store_announcement(ann)

            await asyncio.sleep(10)

def has_replied(question: Dict) -> bool:
    return question["reply_text"] or question["reply_subject"]

def esc_question_status_text(question: Dict, full=False) -> str:
    by_admin = ""
    if admin_name := question["admin"]:
        by_admin = f" by {esc(admin_name)}"

    if has_replied(question):
        if full:
            return f":white_check_mark: Replied{by_admin}\n\n{esc_reply_text(question)}"
        else:
            return f":white_check_mark: Replied{by_admin}"

    if question["ignored"]:
        return f":green_circle: Ignored{by_admin}"

    if by_admin:
        return f":yellow_circle: Claimed{by_admin}"

    return ":red_circle: Waiting for reply"

def esc_reply_text(question: Dict) -> str:
    reply = (
        f"### Reply: {esc(question['reply_subject'])}\n"
        f"{esc(question['reply_text'])}"
    )
    return reply

def prepare_message_content(question: Dict):
    content = (
        f"### Question (contest: {esc(question['contest'])}, user: {esc(question['user'])})\n"
        f"## {truncate(esc(question['subject']), 200)}\n"
        f"{truncate(esc(question['text']), 1000)}"
        "\n"
        f"### State: {esc_question_status_text(question)}"
    )
    if has_replied(question):
        content += (
            "\n\n"
            f"{esc_reply_text(question)}"
        )
    return content


async def question_update(client: DiscordBot, old: dict, question: dict):
    channel = await client.get_target_channel()
    if not channel:
        logger.warning("Channel not found")
        return

    if "message_id" not in old:
        new_message: Message = await channel.send(
            content = prepare_message_content(question)
        )
        logger.info("Question message created")

        question["message_id"] = new_message.id

        thread = await new_message.create_thread(name=thread_name(question))
        await thread.send(content="Discuss here!")
        logger.info("Thread for question created")

        return

    message_id = old["message_id"]

    message: Optional[Message] = None
    if message_id:
        message = client.get_message(message_id)
    if message is None:
        try:
            message = await channel.fetch_message(message_id)
        except Exception:
            logger.warning("Chat message not found, ignoring")
            return

    thread = client.get_channel(message.id)
    if thread is None:
        thread = await client.fetch_channel(message.id)
    if thread is None:
        thread = await message.create_thread(name=thread_name(question))

    if thread is None or not isinstance(thread, Thread):
        logger.error("Incorrect state for thread")
        return

    await message.edit(content=prepare_message_content(question))
    logger.info("Question message edited")

    await thread.send(content=esc_question_status_text(question, full=True))
    logger.info("Added question state message to thread")


async def announcement_update(client: DiscordBot, old: Dict, new: Dict):
    if old:
        logger.warning("Announcement update for already existing alert")
        return

    channel = await client.get_target_channel()
    if not channel:
        return

    await channel.send(content=truncate(
            f"### Announcement (contest: {esc(new['contest'])}, admin: {esc(new['admin'])})\n"
            f"### {esc(new['subject'])}\n"
            f"{esc(new['text'])}\n"
        , 1000)
    )
    logger.info("Announcement message sent to channel")


def thread_name(question: Dict):
    return f"{question['id']}-{esc(question['user'])}-{esc(question['contest'])}"


def main():
    """Parse arguments and launch process."""
    parser = argparse.ArgumentParser(description="Discord bot.")

    # unsed, but passed by ResourceService
    parser.add_argument("shard", default="", help="unused", nargs="?")
    contest_id_help = (
        "id of the contest to post questions and announcements for, "
        "or ALL to serve all contests and ignore announcements"
    )
    parser.add_argument("-c", "--contest-id", type=str, help=contest_id_help)

    args = parser.parse_args()

    contest_id = contest_id_from_args(args.contest_id, ask_for_contest)

    if contest_id == "ALL":
        contest_id = None

    if config.discord_bot is None:
        raise ConfigError(
            "Need to configure the Telegram bot before starting it")

    dconfig = config.discord_bot

    bot = DiscordBot(dconfig.channel_id, dconfig.token, contest_id)
    bot.run(bot.token)


if __name__ == "__main__":
    main()
