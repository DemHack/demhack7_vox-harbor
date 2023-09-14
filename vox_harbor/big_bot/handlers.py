import asyncio
import datetime
import logging
from pyrogram import types, enums, raw, utils

from vox_harbor.big_bot import structures
from vox_harbor.big_bot.bots import Bot, BotManager
from vox_harbor.big_bot.chats import ChatsManager
from vox_harbor.big_bot.configs import Config
from vox_harbor.big_bot.exceptions import AlreadyJoinedError
from vox_harbor.common.db_utils import session_scope
from vox_harbor.common.exceptions import format_exception

logger = logging.getLogger('vox_harbor.handlers')


class BlockInserter:
    BLOCK_SIZE = 100
    BLOCK_TTL = 5

    def __init__(self):
        self.block = []
        self.lock = asyncio.Lock()
        self.last_flush = datetime.datetime.now()

    async def flush(self):
        async with self.lock:
            async with session_scope() as session:
                count = len(self.block)
                session.set_settings({'async_insert': True})
                await session.execute('INSERT INTO comments VALUES', self.block)
                self.block.clear()
                self.last_flush = datetime.datetime.now()
                logger.info('flushed %s records', count)

    async def insert(self, message: types.Message, bot_index: int, channel_id: int | None, post_id: int | None):
        async with self.lock:
            self.block.append(
                structures.Comment(
                    user_id=message.from_user.id,
                    date=message.date,
                    chat_id=message.chat.id,
                    message_id=message.id,
                    channel_id=channel_id,
                    post_id=post_id,
                    username=message.from_user.username,
                    first_name=message.from_user.first_name,
                    last_name=message.from_user.last_name,
                    bot_index=bot_index,
                    shard=Config.SHARD_NUM,
                ).model_dump()
            )

        if (
            len(self.block) > self.BLOCK_SIZE or
            datetime.datetime.now() - self.last_flush > datetime.timedelta(seconds=self.BLOCK_TTL)
        ):
            await self.flush()


async def process_message(bot: Bot, message: types.Message):
    bots = await BotManager.get_instance()
    chats = await ChatsManager.get_instance(bots)

    # This will handle scenario if our bot were added to the chat by another user
    # In other cases this will do nothing.
    bot.add_subscribed_chat(message.chat.id)
    await bot.try_join_discover_chat(message.chat, '')

    possible_chats = []
    if message.sender_chat and message.sender_chat.id not in chats.known_chats:
        possible_chats.append(message.sender_chat)

    if (
        message.forward_from_chat and
        message.forward_from_chat.type not in (enums.ChatType.PRIVATE, enums.ChatType.BOT) and
        message.forward_from_chat.id not in chats.known_chats
    ):
        possible_chats.append(message.forward_from_chat)

    for chat in possible_chats:
        try:
            await bots.discover_chat(chat.username or str(chat.id))
        except AlreadyJoinedError:
            pass
        except Exception as e:
            logger.error('found new chat, but failed to join: %s', format_exception(e))

    if message.chat.type == enums.ChatType.CHANNEL:
        # todo: here will be reactions statistic
        return

    channel_id = None
    post_id = None
    if message.reply_to_top_message_id:
        top_message = await bot.get_messages(message.chat.id, message.reply_to_top_message_id)
        if top_message.sender_chat and top_message.sender_chat.type == enums.ChatType.CHANNEL:
            channel_id = top_message.sender_chat.id
            post_id = top_message.forward_from_message_id

    if not message.from_user:
        # anon user
        return

    await inserter.insert(message, bot.index, channel_id, post_id)


async def channel_confirmation_handler(client: Bot, update, _, chats):
    if isinstance(update, raw.types.UpdateChannel):
        channel: raw.types.Channel = chats[update.channel_id]
        logger.info('got confirmation for %s', channel.title)
        await client.resolve_invite_callback(channel.title, utils.get_channel_id(channel.id))


inserter = BlockInserter()
