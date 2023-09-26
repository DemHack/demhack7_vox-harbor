import asyncio
import collections
import datetime
import logging

from pyrogram import enums, raw, types, utils

import vox_harbor.big_bot
from vox_harbor.big_bot import structures
from vox_harbor.big_bot.chats import ChatsManager
from vox_harbor.common.config import config
from vox_harbor.common.db_utils import session_scope
from vox_harbor.common.exceptions import format_exception

logger = logging.getLogger('vox_harbor.handlers')


class BlockInserter:
    BLOCK_SIZE = 10000
    BLOCK_TTL = 10

    def __init__(self):
        self.comments = []
        self.users = []
        self.chats = []
        self.posts = []

        self.lock = asyncio.Lock()
        self.last_flush = datetime.datetime.now()

    async def flush(self):
        async with self.lock:
            block_comments = self.comments.copy()
            block_users = self.users.copy()
            block_chats = self.chats.copy()
            block_posts = self.posts.copy()
            self.comments.clear()
            self.users.clear()
            self.chats.clear()
            self.posts.clear()

        async with session_scope() as session:
            count = len(block_comments)
            session.set_settings(dict(async_insert=True))
            if block_comments:
                await session.execute('INSERT INTO comments VALUES', block_comments)

            if block_users:
                await session.execute('INSERT INTO users VALUES', block_users)

            if block_chats:
                await session.execute('INSERT INTO discovered_chats VALUES', block_chats)

            if block_posts:
                await session.execute('INSERT INTO posts VALUES', block_posts)

            self.last_flush = datetime.datetime.now()
            logger.info('flushed %s records', count)

    async def loop(self):
        while True:
            try:
                await self.flush()
                await asyncio.sleep(self.BLOCK_TTL)
            except Exception as e:
                logger.error('failed to flush a block: %s', format_exception(e, with_traceback=True))

    def start(self):
        asyncio.create_task(self.loop())

    async def insert(self, message: types.Message, bot_index: int, channel_id: int | None, post_id: int | None):
        async with self.lock:
            self.comments.append(
                structures.Comment(
                    user_id=message.from_user.id,
                    date=message.date.astimezone(datetime.timezone.utc),
                    chat_id=message.chat.id,
                    message_id=message.id,
                    channel_id=channel_id,
                    post_id=post_id,
                    bot_index=bot_index,
                    shard=config.SHARD_NUM,
                ).model_dump()
            )

            name = ' '.join(filter(None, (message.from_user.first_name, message.from_user.last_name)))
            self.users.append(
                structures.User(
                    user_id=message.from_user.id,
                    username=message.from_user.username or '',
                    name=name,
                ).model_dump()
            )

    async def insert_chat(self, chat: types.Chat):
        async with self.lock:
            self.chats.append(
                structures.DiscoveredChat(
                    id=chat.id,
                    name=chat.title or ' '.join((chat.first_name, chat.last_name)),
                    join_string=chat.username,
                    subscribers_count=chat.members_count,
                    sign=1,
                ).model_dump()
            )

    async def insert_post(self, post: types.Message, bot_index: int):
        data = collections.defaultdict(int)
        data['@views'] = post.views or 0

        if post.reactions is not None:
            # noinspection PyUnresolvedReferences
            for reaction in post.reactions.reactions:  # reactions.reactions ??? thank you, pyrogram
                if reaction.emoji:
                    data[reaction.emoji] += reaction.count

                elif reaction.custom_emoji_id:
                    data[f'@custom_emoji_{reaction.custom_emoji_id}'] += reaction.count

        if post.poll:
            if not post.poll.chosen_option_id:
                if post.poll.is_anonymous and not post.poll.is_closed:
                    try:
                        logger.info('voting for the first time (%s, %s)', post.chat.id, post.id)
                        await post.vote(0)
                    except Exception as e:
                        logger.error('voting failed: %s', format_exception(e, with_traceback=True))
                return

            for option in post.poll.options:
                data[f'@option_{option.text}'] = option.voter_count

        async with self.lock:
            post_json = structures.Post(
                id=post.id,
                channel_id=post.chat.id,
                post_date=post.date.astimezone(datetime.timezone.utc),
                bot_index=bot_index,
                shard=config.SHARD_NUM,
                point_date=datetime.datetime.utcnow(),
                keys=list(data.keys()),
                values=list(data.values()),
            ).model_dump()

            post_json['data.key'] = post_json.pop('keys')
            post_json['data.value'] = post_json.pop('values')

            self.posts.append(post_json)


async def process_message(bot: 'vox_harbor.big_bot.bots.Bot', message: types.Message):
    if message.chat.id not in await bot.get_subscribed_chats():
        logger.info('durov moment for chat %s bot %s', message.chat.id, bot.index)
        return

    chats = await ChatsManager.get_instance()

    # This will handle scenario if our bot were added to the chat by another user
    # In other cases this will do nothing.
    bot.add_subscribed_chat(message.chat.id)
    await bot.try_join_discovered_chat(message.chat, '')

    if (
        message.forward_from_chat
        and message.forward_from_chat.type not in (enums.ChatType.PRIVATE, enums.ChatType.BOT)
        and message.forward_from_chat.id not in chats.known_chats
        and message.forward_from_chat.username
    ):
        chat = message.forward_from_chat

        if chat.members_count is None:
            chat.members_count = await bot.get_chat_members_count_with_cache(chat.id)

        if (chat.type == enums.ChatType.CHANNEL and chat.members_count >= config.MIN_CHANNEL_MEMBERS_COUNT) or (
            chat.type != enums.ChatType.CHANNEL and chat.members_count >= config.MIN_CHAT_MEMBERS_COUNT
        ):
            await inserter.insert_chat(chat)

    if message.chat.type == enums.ChatType.CHANNEL:
        if datetime.datetime.now() - message.date < datetime.timedelta(weeks=1):
            await inserter.insert_post(message, bot.index)

        return

    channel_id = None
    post_id = None
    if message.reply_to_top_message_id:
        top_message = await bot.get_message_witch_cache(message.chat.id, message.reply_to_top_message_id)
        if top_message.sender_chat and top_message.sender_chat.type == enums.ChatType.CHANNEL:
            channel_id = top_message.sender_chat.id
            post_id = top_message.forward_from_message_id

    if not message.from_user:
        # anon user
        return

    await inserter.insert(message, bot.index, channel_id, post_id)


async def channel_confirmation_handler(client: 'vox_harbor.big_bot.bots.Bot', update, _, chats):
    if isinstance(update, raw.types.UpdateChannel):
        channel: raw.types.Channel = chats[update.channel_id]
        await client.resolve_invite_callback(channel.title, utils.get_channel_id(channel.id))


inserter = BlockInserter()
