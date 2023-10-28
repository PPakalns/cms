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

import json
import logging
import asyncio
from typing import Dict, Optional
import asyncio_gevent

import discord
from discord.channel import TextChannel
from discord.message import Message
from simplekv.fs import FilesystemStore

from cms.db.admin import Admin
from cms.db.contest import Announcement, Contest
from cms.db.session import SessionGen
from cms.db.user import Question, User

from cms.service.EventService import EventExecutor, EventOperation

logger = logging.getLogger(__name__)

class KeyValueStore:
    def __init__(self, storage_path: str) -> None:
        self.lock = asyncio.Lock()
        self.storage_path = storage_path
        self.store = FilesystemStore(storage_path)

    async def read_question_state(self, question_id: int) -> Optional[Dict]:
        await self.read_state(f"question_{question_id}")

    async def read_state(self, key: str) -> Optional[Dict]:
        async with self.lock:
            value = self.store.get(key)
            if value:
                return json.loads(value)
            return None

    async def store_question_state(self, question_id: int, data: Dict):
        await self.write_state(f"question_{question_id}", data)

    async def write_state(self, key: str, value: Dict):
        async with self.lock:
            value = self.store.put(key, json.dumps(value))

@discord.commands.command()
async def alive(ctx: discord.commands.context.ApplicationContext):
    if ctx.channel_id != ctx.bot.__getattribute__("channel_id"):
        return
    ctx.send_response(content = "Bot is alive")

class DiscordBot(discord.Bot):
    def __init__(self,
                 *,
                 loop: asyncio.AbstractEventLoop | None = None,
                 channel_id: Optional[int] =None,
                 store: KeyValueStore,
                 **options
                 ):

        self.channel_id = channel_id
        self.store = store
        self.add_application_command(alive)

        super().__init__(loop=loop, **options)

    async def get_target_channel(self) -> TextChannel | None:
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

async def start_discord_client(client: DiscordBot, token: str) -> None:
    await client.start(token)


class DiscordEventExecutor(EventExecutor):

    def __init__(self, params):
        super().__init__()

        asyncio.set_event_loop_policy(asyncio_gevent.EventLoopPolicy())

        intents = discord.Intents.default()
        intents.message_content = True
        intents.reactions = True

        token = params.get("token", None)
        if token is None:
            raise Exception("Discord event executor missing token parameter in configuration.")
        channel_id = params.get("channel_id", None)
        storage_path = params.get("storage_path", None)
        if storage_path is None:
            raise Exception("Storage path not provided")
        store = KeyValueStore(storage_path)

        self.client = DiscordBot(intents=intents, command_prefix="/", channel_id=channel_id, store=store)
        self.discord_bot_greenlet = asyncio_gevent.future_to_greenlet(start_discord_client(self.client, token))
        self.discord_bot_greenlet.start()

    @staticmethod
    def codename():
        return "Discord"

    def execute(self, item: EventOperation):
        """Process events
        """
        future = None

        with SessionGen() as session:
            if item.type == EventOperation.REFRESH:
                pass
            elif item.type in [EventOperation.QUESTION_NEW, EventOperation.QUESTION_REPLIED, EventOperation.QUESTION_CLAIMED, EventOperation.QUESTION_IGNORED]:
                question_id = item.data["question_id"]
                question: Optional[Question] = Question.get_from_id(question_id, session)
                if not question:
                    logger.warn(f"Question {question_id} doesn't exist anymore")
                    return
                future = question_update(self.client, question_id, get_question_desc(question))

            elif item.type == EventOperation.ANNOUNCEMENT_NEW:
                announcement_id = item.data["announcement_id"]
                announcement: Optional[Announcement] = Announcement.get_from_id(announcement_id, session)
                if not announcement:
                    logger.warn(f"Announcement {announcement_id} doesn't exist anymore")
                    return
                future = announcement_update(self.client, announcement_id, get_announcement_desc(announcement))

            elif item.type == EventOperation.ANNOUNCEMENT_DELETED:
                announcement_id = item.data["announcement_id"]
                future = announcement_update(self.client, announcement_id, None)

            else:
                logging.warning("Unhandled event in Discord event handler")

            if future:
                greenlet = asyncio_gevent.future_to_greenlet(future)
                greenlet.start()
                greenlet.join()

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
    text = discord.utils.utils.escape_markdown(text)
    text = discord.utils.utils.escape_mentions(text)
    return text

def has_replied(question: Dict) -> bool:
    return question["reply_text"] or question["reply_subject"]

def esc_question_status_text(question: Dict, full=False) -> str:
    by_admin = ""
    if admin_name := question["admin"]:
        by_admin = f" by {esc(admin_name)}"

    if has_replied(question):
        if full:
            return f"Replied{by_admin}\n\n{esc_reply_text(question)}"
        else:
            return f"Replied{by_admin}"

    if question["ignored"]:
        return f"Ignored{by_admin}"

    if by_admin:
        return f"Claimed{by_admin}"

    return "Waiting for reply"

def esc_reply_text(question: Dict) -> str:
    reply = (
        f"###### Reply: {esc(question['reply_subject'])}\n"
        f"{esc(question['reply_text'])}"
    )
    return reply

def prepare_message_content(question: Dict):
    content = (
        f"##### Jautājums (sacensības: {esc(question['contest'])}, dalībnieks: {esc(question['user'])})\n"
        f"#### {esc(question['subject'])}\n"
        "\n"
        f"{esc(discord.utils.utils.escape_markdown(question['text']))}"
        "\n"
        f"State: {esc_question_status_text(question)}"
    )
    if has_replied(question):
        content += (
            "\n\n"
            f"{esc_reply_text(question)}"
        )
    return content

def thread_name(question: Dict):
    return f"{esc(question['id'])}-{esc(question['user'])}-{esc(question['contest'])}"

async def question_update(client: DiscordBot, question_id: int, question: Optional[Dict]):
    if not question:
        return

    state = await client.store.read_question_state(question_id)

    if state is None:
        channel = await client.get_target_channel()
        if not channel:
            logger.warn("Channel not found")
            return

        state = {
            "message_id": None,
            "question": question
        }
        await client.store.store_question_state(question_id, state)

        new_message: Message = await channel.send(
            content = prepare_message_content(question)
        )

        state["message_id"] = new_message.id
        await client.store.store_question_state(question_id, state)

        thread = await new_message.create_thread(name=thread_name(question))
        await thread.send(content="Discuss the question here!")

        return

    if state["question"] == question:
        # Nothing has changed, ignore
        return

    message_id = state["message_id"]
    message: Optional[Message] = None
    if message_id:
        message = client.get_message(message_id)
    if message is None:
        logger.warn("Chat message not found, ignoring")
        return

    if message.thread is None:
        await message.create_thread(name=thread_name(question))

    if message.thread is None:
        logger.error("Incorrect state")
        return

    await message.edit(content=prepare_message_content(question))

    state["question"] = question
    await client.store.store_question_state(question_id, state)

    await message.thread.send(content=esc_question_status_text(question, full=True))


async def announcement_update(client: DiscordBot, announcement_id: int, announcement: Optional[Dict]):
    channel = await client.get_target_channel()
    if not channel:
        return
    if announcement:
        await channel.send(content=(
                f"Announcement (contest: {esc(announcement["contest"])}, admin: {esc(announcement["admin"])})\n"
                f"*{esc(announcement["subject"])}*\n"
                f"{esc(announcement["text"])}\n"
            )
        )


