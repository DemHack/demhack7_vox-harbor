import asyncio
import logging
import random
from typing import Iterable

import cachetools
import pyrogram.errors.exceptions
from aiolimiter import AsyncLimiter
from pyrogram import Client, raw, types, utils
from pyrogram.types.messages_and_media.message import Message as PyrogramMessage

from vox_harbor.big_bot import structures
from vox_harbor.big_bot.chats import ChatsManager
from vox_harbor.big_bot.configs import Config, Mode
from vox_harbor.big_bot.exceptions import AlreadyJoinedError
from vox_harbor.common.db_utils import session_scope
from vox_harbor.common.exceptions import format_exception


class Bot(Client):
    def __init__(self, *args, bot_index, **kwargs):
        super().__init__(*args, **kwargs)

        self.index = bot_index

        self._invites_callback: dict[str, asyncio.Future[int]] = {}
        self._subscribed_chats: set[int] = set()

        self.logger = logging.getLogger(f'vox_harbor.big_bot.bots.bot.{bot_index}')

        self.history_limiter = AsyncLimiter(2, 1)

    async def resolve_invite_callback(self, chat_title: str, channel_id: int):
        if chat_title not in self._invites_callback:
            return

        self._invites_callback[chat_title].set_result(channel_id)

    async def update_subscribed_chats(self):
        new_chats = set()

        async for dialog in self.get_dialogs():
            new_chats.add(dialog.chat.id)

        self._subscribed_chats = new_chats
        return self._subscribed_chats

    async def get_subscribed_chats(self):
        if not self._subscribed_chats:
            await self.update_subscribed_chats()

        return self._subscribed_chats

    def add_subscribed_chat(self, chat_id: int):
        self._subscribed_chats.add(chat_id)

    async def leave_chat(self, chat_id: int, delete: bool = False):
        self.logger.info('leaving %s', chat_id)
        await super().leave_chat(chat_id, delete)
        self._subscribed_chats.remove(chat_id)

    async def join_chat(self, join_string: str | int):
        # todo: fix this bullshit
        if join_string == 777000 or join_string == '777000':
            return

        if len(self._subscribed_chats) > Config.MAX_CHATS_FOR_BOT:
            raise ValueError('Too many chats')

        self.logger.info('joining %s', join_string)
        chat = await super().join_chat(join_string)
        self._subscribed_chats.add(chat.id)
        return chat

    async def discover_chat(self, join_string: str, with_linked: bool = True, join_no_check: bool = False):
        # todo: fix this bullshit
        if join_string == 777000 or join_string == '777000':
            return

        try:
            join_string = int(join_string)
        except ValueError:
            pass

        self.logger.info('discovering chat %s', join_string)
        preview = await self.get_chat(join_string)
        self.logger.info('chat title %s', preview.title)

        if preview.members_count < Config.MIN_CHAT_MEMBERS_COUNT:
            self.logger.info('not enough members to join, skip')
            return

        if isinstance(preview, types.Chat):
            chat = preview
        else:
            try:
                chat = await super().join_chat(join_string)
                chat = await self.get_chat(chat.id)  # thank you, telegram
            except pyrogram.errors.exceptions.bad_request_400.InviteRequestSent:
                self.logger.info('waiting for an approval')
                future = asyncio.get_running_loop().create_future()

                try:
                    self._invites_callback[preview.title] = future

                    async with asyncio.timeout(10):
                        chat_id = await future
                        chat = await self.get_chat(chat_id)
                finally:
                    del self._invites_callback[preview.title]

        self.logger.info('discovered chat with id %s', chat.id)

        if join_no_check:  # direct join to the chat in case if we are loading from ChatsManager
            await self.join_chat(chat.id)
        else:
            await self.try_join_discovered_chat(chat, str(join_string))
            if with_linked and chat.linked_chat:
                linked_join_string = chat.linked_chat.username or str(chat.linked_chat.id)
                await self.discover_chat(linked_join_string, with_linked=False)

    async def try_join_discovered_chat(self, chat: types.Chat, join_string: str):
        chats = await ChatsManager.get_instance(await BotManager.get_instance())

        if known_chat := chats.known_chats.get(chat.id):
            if known_chat.shard == Config.SHARD_NUM and known_chat.bot_index == self.index:
                if chat.id not in self._subscribed_chats:
                    await self.join_chat(chat.id)

                return

            if chat.id in self._subscribed_chats:
                self.logger.info('this chat is already handled by another bot, leaving')
                try:
                    await self.leave_chat(chat.id)
                except Exception as e:
                    self.logger.error('failed to leave chat %s: %s', chat.id, format_exception(e))
        else:
            if chat.id not in self._subscribed_chats:
                await self.join_chat(chat.id)

            await chats.register_new_chat(self.index, chat.id, join_string)

    async def get_message_witch_cache(self, chat_id: int, message_id: int):
        if (chat_id, message_id) in self.message_cache.store:
            return self.message_cache[chat_id, message_id]

        return await self.get_messages(chat_id=chat_id, message_ids=message_id, replies=0)

    async def get_history(self, chat_id: int, start: int, end: int, limit: int) -> list[types.Message]:
        await self.history_limiter.acquire()
        raw_messages = await self.invoke(
            raw.functions.messages.GetHistory(
                peer=await self.resolve_peer(chat_id),
                offset_id=start,
                offset_date=0,
                add_offset=0,
                limit=limit,
                max_id=0,
                min_id=end,
                hash=0,
            ),
            sleep_threshold=60,
        )

        return await utils.parse_messages(self, raw_messages, replies=0)


class BotManager:
    """
    Universal multi-bot manager. On creation loads available bots from ClickHouse table.
    """

    logger = logging.getLogger('vox_harbor.big_bot.bots')
    lock = asyncio.Lock()

    def __init__(self, bots: list[Bot]):
        self.bots = bots

        self._discover_cache = cachetools.TTLCache(maxsize=500, ttl=60)

    def __getitem__(self, item):
        return self.bots[item]

    def __iter__(self):
        return iter(self.bots)

    async def start(self):
        for bot in self.bots:
            await bot.start()

    async def stop(self):
        for bot in self.bots:
            await bot.stop()

    def register_handler(self, handler, group_id: int = 0):
        for bot in self.bots:
            bot.add_handler(handler, group_id)

    async def update_subscribe_chats(self):
        for bot in self.bots:
            await bot.update_subscribed_chats()

    async def discover_chat(self, join_string: str):
        async with self.lock:
            if join_string in self._discover_cache:
                raise AlreadyJoinedError('chat is being discovered')

            self._discover_cache[join_string] = True

        total = sum([len(await bot.get_subscribed_chats()) for bot in self.bots])
        weights = [total / len(await bot.get_subscribed_chats()) for bot in self.bots]
        bot: Bot = random.choices(self.bots, weights=weights)[0]
        return await bot.discover_chat(join_string)

    async def get_messages(self, bot_index: int, chat_id: int, message_ids: Iterable[int]) -> list[PyrogramMessage]:
        return await self.bots[bot_index].get_messages(chat_id, message_ids=message_ids)  # type: ignore

    @classmethod
    async def get_instance(cls, shard: int = Config.SHARD_NUM) -> 'BotManager':
        global _manager

        if _manager is not None:
            return _manager

        if Config.MODE == Mode.PROD:
            target_table = 'bots'
        elif Config.MODE == Mode.DEV_1:
            target_table = 'bots_dev_1'
        elif Config.MODE == Mode.DEV_2:
            target_table = 'bots_dev_2'
        else:
            raise ValueError(f'Unknown mode {Config.MODE}')

        cls.logger.info(f'loading bots from table {target_table}')
        async with session_scope() as session:
            # noinspection SqlResolve
            await session.execute(
                f'SELECT * FROM {target_table} WHERE shard == %(shard)s ORDER BY id', dict(shard=shard)
            )
            bots_data = structures.Bot.from_rows(await session.fetchall())

            if len(bots_data) < Config.ACTIVE_BOTS_COUNT:
                raise ValueError('Not enough bots to start up')

            await session.execute('SELECT * FROM broken_bots')
            broken_bots_data = structures.BrokenBot.from_rows(await session.fetchall())

        broken_bots_set = {b.id for b in broken_bots_data}

        j = Config.ACTIVE_BOTS_COUNT
        for i, bot in enumerate(bots_data):
            if i >= Config.ACTIVE_BOTS_COUNT:
                break

            if bot.id in broken_bots_set:
                while j < len(bots_data) and bots_data[j].id in broken_bots_set:
                    j += 1

                if j >= len(bots_data):
                    raise ValueError('Not enough active bots to startup')

                bots_data[i] = bots_data[j]

        bots_data = bots_data[: Config.ACTIVE_BOTS_COUNT]

        bots: list[Bot] = []
        for i, bot in enumerate(bots_data):
            bots.append(Bot(bot.name, session_string=bot.session_string, bot_index=i))
            cls.logger.info('loaded bot %s', bot.name)

        _manager = cls(bots)
        return _manager


_manager: BotManager | None = None
