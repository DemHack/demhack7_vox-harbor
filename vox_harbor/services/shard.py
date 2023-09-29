import asyncio
import logging
from itertools import chain, groupby
from operator import attrgetter

import uvicorn
from fastapi import FastAPI
from pyrogram import types
from pyrogram.types.messages_and_media.message import Message as PyrogramMessage

from vox_harbor.big_bot.bots import Bot, BotManager
from vox_harbor.big_bot.structures import (
    Comment,
    EmptyResponse,
    Message,
    Post,
    PostText,
    User,
)
from vox_harbor.common.config import config

shard = FastAPI()
logger = logging.getLogger(f'vox_harbor.services.shard.{config.SHARD_NUM}')


@shard.post('/messages')
async def get_messages(sorted_comments: list[Comment]) -> list[Message]:
    bot_manager = await BotManager.get_instance(config.SHARD_NUM)
    tasks = []

    for bot_index, comments_by_bot_index in groupby(sorted_comments, attrgetter('bot_index')):
        for chat_id, comments_by_chat_id in groupby(comments_by_bot_index, attrgetter('chat_id')):
            message_ids = [comment.message_id for comment in comments_by_chat_id]
            logger.debug('get_messages: bot_manager.get_messages args: %s', (bot_index, chat_id, message_ids))
            get_msgs = bot_manager.get_messages(bot_index, chat_id, message_ids)
            tasks.append(get_msgs)

    pyrogram_messages = list(chain.from_iterable(await asyncio.gather(*tasks)))

    logger.debug('get_messages: pyrogram_messages: %s\n\n', pyrogram_messages)

    messages_zipped = zip(pyrogram_messages, sorted_comments, strict=True)

    msg: types.Message
    return [
        Message(text=msg.text, chat=msg.chat.title or ' '.join((msg.chat.first_name, msg.chat.last_name)), comment=cmt)
        for msg, cmt in messages_zipped
        if msg is not None and msg.chat is not None
    ]


@shard.get('/known_chats_count')
async def get_known_chats_count() -> int:
    bots = await BotManager.get_instance(config.SHARD_NUM)

    chats_count = 0
    for bot in bots:
        chats_count += len(await bot.get_subscribed_chats())

    return chats_count


@shard.post('/discover')
async def discover(join_string: str, ignore_protection: bool = False) -> None:
    bot_manager = await BotManager.get_instance(config.SHARD_NUM)
    logger.info('discovering chat. join_string: %s', join_string)
    await bot_manager.discover_chat(join_string, ignore_protection=ignore_protection)


@shard.get('/user_from_comment')
async def get_user_from_comment(chat_id: int | str, message_id: int) -> User | EmptyResponse:
    # todo
    bot_manager = await BotManager.get_instance(config.SHARD_NUM)
    bot = bot_manager[0]

    message = await bot.get_messages(chat_id=chat_id, message_ids=[message_id])
    if message is None:
        return EmptyResponse()

    return User(
        user_id=message.from_user.id,
        username=message.from_user.username,
        name=' '.join(filter(None, (message.from_user.first_name, message.from_user.last_name))),
    )


@shard.get('/post')
async def get_post(channel_id: int, post_id: int, bot_index: int) -> PostText | EmptyResponse:
    bot_manager = await BotManager.get_instance(config.SHARD_NUM)

    message: list[PyrogramMessage] = await bot_manager.get_messages(
        bot_index=bot_index, chat_id=channel_id, message_ids=[post_id]
    )
    if not message:
        return EmptyResponse()

    return PostText(text=message[0].text)


def main():
    server_config = uvicorn.Config(shard, host=config.shard_host, port=config.shard_port, log_config=None)
    server = uvicorn.Server(server_config)
    return server.serve()


if __name__ == '__main__':
    from vox_harbor.big_bot.main import big_bots_main
    from vox_harbor.cli import _main  # type: ignore

    asyncio.run(_main(big_bots_main))
