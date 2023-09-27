import asyncio
import datetime
import logging

from pyrogram import enums, types

import vox_harbor.big_bot
from vox_harbor.big_bot import structures
from vox_harbor.common.config import config
from vox_harbor.common.db_utils import db_fetchall, db_fetchone, session_scope
from vox_harbor.common.exceptions import format_exception


class ChatsManager:
    logger = logging.getLogger('vox_harbor.big_bot.chats')
    lock = asyncio.Lock()

    INTERVAL = 60

    def __init__(self, bot_manager: 'vox_harbor.big_bot.bots.BotManager'):
        self.bots = bot_manager
        self.last_updated = 0

        self.known_chats: dict[int, structures.Chat] = {}

    @staticmethod
    def get_chat_type(chat: types.Chat):
        if chat.type == enums.ChatType.CHANNEL:
            return structures.Chat.Type.CHANNEL
        if chat.type == enums.ChatType.PRIVATE:
            return structures.Chat.Type.PRIVATE
        return structures.Chat.Type.CHAT

    async def register_new_chat(self, bot_index: int, chat_id: int, join_string: str = ''):
        self.logger.info('registering new chat (%s, %s) for bot %s', chat_id, join_string, bot_index)
        bot = self.bots[bot_index]
        chat = await bot.get_chat(chat_id)
        if not join_string and (chat.username or chat.invite_link):
            join_string = chat.username or chat.invite_link

        async with session_scope() as session:
            chat_model = structures.Chat(
                id=chat.id,
                name=f'{chat.title or ""}' + (f' ({chat.username})' if chat.username else ''),
                join_string=join_string,
                shard=config.SHARD_NUM,
                bot_index=bot_index,
                added=datetime.datetime.utcnow().timestamp(),
                type=self.get_chat_type(chat),
            )

            session.set_settings(dict(async_insert=True))
            await session.execute('INSERT INTO chats VALUES', [chat_model.model_dump()])
            self.logger.info('added new chat %s', chat_model.name)

        self.known_chats[chat_model.id] = chat_model

        await bot.generate_history_task(self, chat_id, with_from_earliest=False)
        return True

    async def update(self):
        self.logger.info('updating chats')

        join_count = 0
        leave_count = 0
        chats = await db_fetchall(structures.Chat, 'SELECT * FROM chats', raise_not_found=False)

        new_known_chats = {chat.id: chat for chat in chats}
        self.known_chats = new_known_chats

        for chat in chats:
            for i, bot in enumerate(self.bots):
                if (
                    chat.id in await bot.get_subscribed_chats()
                    and (chat.shard != config.SHARD_NUM or chat.bot_index != i)
                    and chat.type != structures.Chat.Type.PRIVATE
                ):
                    # Wrong bot index or shard
                    try:
                        await bot.leave_chat(chat.id)
                        leave_count += 1
                    except Exception as e:
                        self.logger.error('failed to leave chat %s: %s', chat.name, format_exception(e))

            if chat.shard != config.SHARD_NUM:
                continue

            bot = self.bots[chat.bot_index]
            if chat.id not in await bot.get_subscribed_chats():
                try:
                    if chat.join_string:
                        await bot.discover_chat(chat.join_string, join_no_check=True)
                    else:
                        await bot.join_chat(chat.id)

                    join_count += 1
                except Exception as e:
                    self.logger.error('failed to join chat %s: %s', chat.name, format_exception(e))

        self.known_chats = new_known_chats
        self.logger.info('joined %s, left %s', join_count, leave_count)

    async def run_once(self):
        update = await db_fetchone(
            structures.ChatUpdate,
            'SELECT * FROM chat_updates\n' 
            'WHERE shard = %(shard)s\n' 
            'ORDER BY added DESC\n' 
            'LIMIT 1',
            dict(shard=config.SHARD_NUM),
            raise_not_found=False,
        )

        if update.added.timestamp() > self.last_updated:
            await self.bots.update_subscribe_chats()
            await self.update()
            self.last_updated = datetime.datetime.now().timestamp()

    async def loop(self):
        while True:
            try:
                await asyncio.sleep(self.INTERVAL)
                # todo: fix timezones issue
                # await self.run_once()

                await self.update()
            except Exception as e:
                self.logger.error('failed to update chats. %s', format_exception(e, with_traceback=True))

    @classmethod
    async def get_instance(cls, bot_manager: 'vox_harbor.big_bot.bots.BotManager' = None):
        async with cls.lock:
            global _manager
            if _manager is not None:
                return _manager

            assert bot_manager is not None  # fixme pufit
            _manager = cls(bot_manager)

            await _manager.update()
            asyncio.create_task(_manager.loop())
            return


_manager: ChatsManager | None = None
