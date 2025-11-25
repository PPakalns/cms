#!/usr/bin/env python3

# Contest Management System - http://cms-dev.github.io/
# Copyright © 2023 Pēteris Pakalns <peterispakalns@gmail.com>
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os
import sys
import json
import time
import logging
import asyncio
import discord
import queue as pqueue
import multiprocessing as mp

from typing import Dict, Optional

from discord.channel import TextChannel
from discord.message import Message
from simplekv.fs import FilesystemStore

from cms.conf import config
from cms.db.admin import Admin
from cms.db.contest import Announcement, Contest
from cms.db.session import SessionGen
from cms.db.user import Question, User
from cms.io.priorityqueue import QueueEntry
from cms.util import mkdir
from cms.log import ServiceFilter, DetailedFormatter

from cms.service.EventService import EventExecutor, EventOperation

logger = logging.getLogger(__name__)

def truncate(s, length):
    if len(s) <= length:
        return s
    else:
        return s[:length] + "..."

class KeyValueStore:
    def __init__(self, storage_path: str) -> None:
        self.lock = asyncio.Lock()
        self.storage_path = storage_path
        self.store = FilesystemStore(storage_path)

    async def read_question_state(self, question_id: int) -> Optional[Dict]:
        return await self.read_state(f"question_{question_id}")

    async def read_state(self, key: str) -> Optional[Dict]:
        async with self.lock:
            try:
                value = self.store.get(key)
                return json.loads(value.decode('utf-8'))
            except KeyError:
                return None

    async def store_question_state(self, question_id: int, data: Dict):
        await self.write_state(f"question_{question_id}", data)

    async def write_state(self, key: str, value: Dict):
        async with self.lock:
            self.store.put(key, json.dumps(value).encode())

@discord.commands.command()
@discord.guild_only()
async def alive(ctx: discord.commands.context.ApplicationContext):
    if ctx.channel_id != ctx.bot.__getattribute__("channel_id"):
        return
    await ctx.send_response(content = "Bot is alive")

class DiscordBot(discord.Bot):
    def __init__(self,
                 *,
                 channel_id: Optional[int] =None,
                 store: KeyValueStore,
                 **options
                 ):
        super().__init__(
            description="""Contest Management System notification bot""",
            loop=asyncio.get_running_loop(),
            **options
        )
        self.channel_id = channel_id
        self.store = store

        self.add_application_command(
            alive
        )

    async def get_target_channel(self) -> Optional[TextChannel]:
        if self.channel_id:
            channel = self.get_channel(self.channel_id)
            if isinstance(channel, TextChannel):
                return channel
        return None

    async def on_ready(self):
        logger.info(f'Logged on as {self.user}!')
        channel = await self.get_target_channel()

        if channel is None:
            if self.channel_id is None:
                logger.info(f'Channel not assigned')
            else:
                logger.info(f'Channel with id {self.channel_id} not found')
        else:
            logger.info(f'Channel found')

async def process_queue(queue: mp.Queue, client: DiscordBot):
    logger.info("Processing queue")
    while True:
        try:
            item = queue.get_nowait()
        except pqueue.Empty:
            await asyncio.sleep(5)
            continue

        call_type, args, kwargs = item
        logger.info(f"Processing {call_type} item in discord thread!")

        try:
            if call_type == "announcement_update":
                await announcement_update(client, *args, *kwargs)
            elif call_type == "question_update":
                await question_update(client, *args, *kwargs)
            else:
                logger.warn(f"Unknown call type: {call_type}")
        except:
            logger.error(f"Error while processing: {call_type}")

async def start_client(client: DiscordBot, token: str):
    logger.info("Starting discord client")
    await client.start(token)
    logger.info("Stopping discord client")

async def sleeper():
    while True:
        await asyncio.sleep(1)     # never blocks the loop

async def discord_process_async(queue: mp.Queue, token: str, channel_id: int, storage_path: str):
    logger.info("Starting discord thread")
    intents = discord.Intents.default()
    intents.message_content = True
    intents.reactions = True
    store = KeyValueStore(storage_path)
    client = DiscordBot(intents=intents, channel_id=channel_id, store=store)

    await asyncio.gather(start_client(client, token), process_queue(queue, client), sleeper())

def discord_process(
        queue: mp.Queue,
        token: str,
        channel_id: int,
        storage_path: str,
    ):
    logger.info("Setting up discord process loggers")

    name = "Discord"
    shard = 0
    root_logger = logging.getLogger()

    log_dir = os.path.join(config.global_.log_dir, "%s-%d" % (name, shard))
    mkdir(config.global_.log_dir)
    mkdir(log_dir)

    log_filename = time.strftime("%Y-%m-%d-%H-%M-%S.log")

    # Install a file handler.
    file_handler = logging.FileHandler(os.path.join(log_dir, log_filename),
                                       mode='w', encoding='utf-8')
    if config.global_.file_log_debug:
        file_log_level = logging.DEBUG
    else:
        file_log_level = logging.INFO

    file_handler.setLevel(file_log_level)
    file_handler.setFormatter(DetailedFormatter(False))
    root_logger.addHandler(file_handler)

    # Provide a symlink to the latest log file.
    try:
        os.remove(os.path.join(log_dir, "last.log"))
    except OSError:
        pass
    os.symlink(log_filename, os.path.join(log_dir, "last.log"))

    _filter = ServiceFilter(name, shard)
    for handler in root_logger.handlers:
        handler.addFilter(_filter)

    logger.info(f"Handlers: {logging.getLogger().handlers}")
    logger.info("Set up discord process logging")
    asyncio.run(discord_process_async(queue, token, channel_id, storage_path))
    logger.info("Discrod process exited")

class ThreadHandle:
    def __init__(self, token: str, channel_id: int, storage_path: str):
        ctx = mp.get_context('spawn')
        self.queue = ctx.Queue()
        self.process = ctx.Process(target=discord_process, kwargs={
            "queue": self.queue,
            "token":token,
            "channel_id":channel_id,
            "storage_path":storage_path,
        })
        logger.info("Starting discord process")
        self.process.start()
        logger.info("Thread created")

    def send(self, call_type: str, *args, **kwargs):
        logger.info(f"Sending {call_type} discord action")
        self.queue.put((call_type, args, kwargs))

class DiscordEventExecutor(EventExecutor):

    def __init__(self, params):
        super().__init__()

        token = params.token
        if token is None:
            raise Exception("Discord event executor missing token parameter in configuration.")
        channel_id = params.channel_id
        storage_path = params.storage_path
        if storage_path is None:
            raise Exception("Storage path not provided")

        self.handle = ThreadHandle(token, channel_id, storage_path)


    @staticmethod
    def codename():
        return "Discord"

    def execute(self, entry: QueueEntry[EventOperation]):
        """Process events
        """
        item = entry.item

        with SessionGen() as session:
            if item.type == EventOperation.REFRESH:
                pass
            elif item.type in [EventOperation.QUESTION_NEW, EventOperation.QUESTION_REPLIED, EventOperation.QUESTION_CLAIMED, EventOperation.QUESTION_IGNORED]:
                question_id = item.data["question_id"]
                question: Optional[Question] = Question.get_from_id(question_id, session)
                if not question:
                    logger.warn(f"Question {question_id} doesn't exist anymore")
                    return
                self.handle.send("question_update", question_id, get_question_desc(question))

            elif item.type == EventOperation.ANNOUNCEMENT_NEW:
                announcement_id = item.data["announcement_id"]
                announcement: Optional[Announcement] = Announcement.get_from_id(announcement_id, session)
                if not announcement:
                    logger.warn(f"Announcement {announcement_id} doesn't exist anymore")
                    return
                self.handle.send("announcement_update", announcement_id, get_announcement_desc(announcement))


            elif item.type == EventOperation.ANNOUNCEMENT_DELETED:
                announcement_id = item.data["announcement_id"]
                self.handle.send("announcement_update", announcement_id, None)

            else:
                logging.warning("Unhandled event in Discord event handler")

def get_question_desc(question: Question):
    user: User = question.participation.user
    contest: Contest = question.participation.contest

    admin: Optional[Admin] = question.admin
    if admin:
        admin = admin.name

    return {
        "id": question.id,
        "subject": question.subject,
        "text": question.text,

        "reply_subject": question.reply_subject,
        "reply_text": question.reply_text,
        "ignored": question.ignored,

        "admin": admin,

        "user": user.username,
        "contest": contest.name,
    }

def get_announcement_desc(announcement: Announcement):
    contest: Contest = announcement.contest
    admin: Optional[Admin] = announcement.admin
    if admin:
        admin = admin.name

    return {
        "id": announcement.id,
        "subject": announcement.subject,
        "text": announcement.text,
        "admin": admin,
        "contest": contest.name,
    }

def esc(text: str) -> str:
    text = discord.utils.escape_markdown(text)
    text = discord.utils.escape_mentions(text)
    return text

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

def thread_name(question: Dict):
    return f"{question['id']}-{esc(question['user'])}-{esc(question['contest'])}"

async def question_update(client: DiscordBot, question_id: int, question: Optional[Dict]):
    if not question:
        return

    state = await client.store.read_question_state(question_id)

    channel = await client.get_target_channel()
    if not channel:
        logger.warn("Channel not found")
        return

    if state is None:
        state = {
            "message_id": None,
            "question": question
        }
        await client.store.store_question_state(question_id, state)

        new_message: Message = await channel.send(
            content = prepare_message_content(question)
        )
        logger.info("Question message created")

        state["message_id"] = new_message.id
        await client.store.store_question_state(question_id, state)

        thread = await new_message.create_thread(name=thread_name(question))
        await thread.send(content="Discuss here!")
        logger.info("Thread for question created")

        return

    if state["question"] == question:
        # Nothing has changed, ignore
        return

    message_id = state["message_id"]
    message: Optional[Message] = None
    if message_id:
        message = client.get_message(message_id)
    if message is None:
        message = await channel.fetch_message(message_id)
    if message is None:
        logger.warn("Chat message not found, ignoring")
        return

    thread = client.get_channel(message.id)
    if thread is None:
        thread = await client.fetch_channel(message.id)
    if thread is None:
        thread = await message.create_thread(name=thread_name(question))

    if thread is None:
        logger.error("Incorrect state")
        return

    await message.edit(content=prepare_message_content(question))
    logger.info("Question message edited")

    state["question"] = question
    await client.store.store_question_state(question_id, state)

    await thread.send(content=esc_question_status_text(question, full=True))
    logger.info("Added question state message to thread")


async def announcement_update(client: DiscordBot, announcement_id: int, announcement: Optional[Dict]):
    channel = await client.get_target_channel()
    if not channel:
        return
    if announcement:
        await channel.send(content=(
                f"### Announcement (contest: {esc(announcement['contest'])}, admin: {esc(announcement['admin'])})\n"
                f"### {esc(announcement['subject'])}\n"
                f"{esc(announcement['text'])}\n"
            )
        )
        logger.info("Announcement message sent to channel")

