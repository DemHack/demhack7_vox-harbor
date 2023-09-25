import asyncio
import typing as tp
import uvicorn
from itertools import chain, groupby
from operator import attrgetter

from fastapi import FastAPI

from vox_harbor.big_bot import structures
from vox_harbor.big_bot.bots import BotManager
from vox_harbor.big_bot.structures import Comment, Message, ShardLoad
from vox_harbor.common.config import config

shard = FastAPI()


@shard.post('/messages')
async def get_messages(sorted_comments: list[Comment]) -> list[Message]:
    bot_manager = await BotManager.get_instance(config.SHARD_NUM)
    tasks = []

    for bot_index, comments_by_bot_index in groupby(sorted_comments, attrgetter('bot_index')):
        for chat_id, comments_by_chat_id in groupby(comments_by_bot_index, attrgetter('chat_id')):
            message_ids = [msg.message_id for msg in comments_by_chat_id]
            get_msgs = bot_manager.get_messages(bot_index, chat_id, message_ids)
            tasks.append(get_msgs)

    pyrogram_messages = chain.from_iterable(await asyncio.gather(*tasks))
    # fixme len(msgs) < len(comments) ; use fields of pyrogram_messages
    messages_zipped = zip(pyrogram_messages, sorted_comments, strict=True)
    return [Message(text=msg.text, comment=cmt) for msg, cmt in messages_zipped]


@shard.get('/load')
async def get_load() -> ShardLoad:
    ...  # todo


@shard.post('/discover')
async def discover(request: structures.DiscoverRequest) -> None:
    bot_manager = await BotManager.get_instance(config.SHARD_NUM)
    await bot_manager.discover_chat(request.join_string)


def main() -> tp.Awaitable:
    server_config = uvicorn.Config(shard, host=config.shard_host, port=config.shard_port, log_config=None)
    server = uvicorn.Server(server_config)
    return server.serve()
