import asyncio
import logging
import typing as tp
from itertools import chain, groupby
from operator import attrgetter

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from vox_harbor.big_bot.structures import Chat, Comment, Message, Post, User, UserInfo
from vox_harbor.common.config import config
from vox_harbor.common.db_utils import (
    clickhouse_default,
    db_execute,
    db_fetchall,
    db_fetchone,
    rows_to_unique_column,
)
from vox_harbor.common.exceptions import BadRequestError, NotFoundError
from vox_harbor.services.auto_discover import AutoDiscover
from vox_harbor.services.shard_client import ShardClient

logger = logging.getLogger('vox_harbor.big_bot.services.controller')

controller = FastAPI()
controller.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)


@controller.get('/user')
async def get_user(user_id: int) -> UserInfo:
    """Web UI (consumer)"""
    return _users_to_user_info(await _get_users_by_user_ids(user_ids=[user_id]))


@controller.get('/user_by_msg_link')
async def get_user_by_msg_link(msg_link: str) -> UserInfo:
    """Web UI (consumer)"""
    # user_id = USER.user_id

    # todo

    # return await get_user(user_id=user_id)


@controller.get('/users')
async def get_users(username: str, limit: int = 10) -> list[UserInfo]:
    """Web UI (consumer)"""
    username_query = """--sql
        SELECT *
        FROM users
        WHERE username ILIKE %(username)s
        LIMIT %(limit)s
    """
    username_rows = await db_fetchall(
        User, username_query, dict(username=username + '%', limit=limit), name=f'Users w/ {username=}'
    )

    username_to_user_ids: dict[str, list[int]] = {}
    for username, rows_by_username in groupby(username_rows, attrgetter('username')):
        username_to_user_ids[username] = rows_to_unique_column(rows_by_username, 'user_id')

    all_user_ids = chain.from_iterable(username_to_user_ids.values())

    return _users_to_users_info(await _get_users_by_user_ids(all_user_ids))


async def _get_users_by_user_ids(user_ids: tp.Iterable[int]):
    user_ids = list(user_ids)

    query = f"""--sql
        SELECT *
        FROM users
        WHERE user_id in %(user_ids)s
    """
    return await db_fetchall(User, query, dict(user_ids=user_ids), name=f'Users w/ id in {user_ids}')


def _users_to_user_info(user_rows: list[User]) -> UserInfo:
    return _users_to_users_info(users_rows=user_rows)[0]


def _users_to_users_info(users_rows: list[User]) -> list[UserInfo]:
    users_info: list[UserInfo] = []

    for user_id, user_rows_iter in groupby(users_rows, attrgetter('user_id')):
        user_rows = list(user_rows_iter)
        usernames = rows_to_unique_column(user_rows, 'username')
        names = rows_to_unique_column(user_rows, 'name')
        users_info.append(UserInfo(user_id=user_id, usernames=usernames, names=names))

    return users_info


@controller.get('/messages_by_user_id')
async def get_messages_by_user_id(user_id: int, limit: int = 10) -> list[Message]:
    return await get_messages(await get_comments(user_id, limit))


@controller.get('/comments')
async def get_comments(user_id: int, limit: int = -1) -> list[Comment]:
    """Web UI (consumer). Use with get_messages."""
    query = """--sql
        SELECT *
        FROM comments
        WHERE user_id = %(user_id)s 
        ORDER BY date
    """

    if limit > 0:
        query += f'LIMIT {limit}'

    return await db_fetchall(Comment, query, dict(user_id=user_id), name='messages')


@controller.post('/messages')
async def get_messages(comments: list[Comment]) -> list[Message]:
    """Web UI (consumer)"""

    logger.debug('get_messages received comments: %s', comments)

    if not (messages := await _get_messages(comments)):
        raise NotFoundError('messages')
    return messages


async def _get_messages(comments: list[Comment]) -> list[Message]:
    sorted_comments = sorted(comments)
    messages: list[Message] = []
    tasks: list[tp.Awaitable] = []

    async def _do_request(_shard: int, _comments_by_shard: list[Comment]):
        async with ShardClient(_shard) as shard_client:
            messages.extend(await shard_client.get_messages(_comments_by_shard))

    for shard, comments_by_shard in groupby(sorted_comments, attrgetter('shard')):
        tasks.append(_do_request(shard, list(comments_by_shard)))
    await asyncio.gather(*tasks)

    messages.sort(key=lambda m: m.comment.date)
    return messages


@controller.post('/discover')
async def discover(join_string: str, ignore_protection: bool = False) -> None:
    """Web UI (consumer)"""
    shards_chats_count: list[int] = []
    tasks: list[tp.Awaitable] = []

    async def _do_request(_shard: int):
        async with ShardClient(_shard) as _shard_client:
            shards_chats_count.append(await _shard_client.get_known_chats_count())

    for shard in range(len(config.SHARD_ENDPOINTS)):
        tasks.append(_do_request(shard))

    await asyncio.gather(*tasks)
    lazy_shard = shards_chats_count.index(min(shards_chats_count))

    async with ShardClient(lazy_shard) as shard_client:
        await shard_client.discover(join_string, ignore_protection)


@controller.post('/add_bot')
async def add_bot(name: str, session_string: str) -> None:
    """Web UI (admin)"""
    # todo


@controller.post('/remove_bot')
async def remove_bot(bot_id: int) -> None:
    """Web UI (admin)"""
    query = """--sql
        INSERT INTO test_broken_bots (*)
        VALUES (%(bot_id)s)
    """  # todo remove prefix test_

    await db_execute(query, dict(bot_id=bot_id))


@controller.get('/chat')
async def get_chat(chat_id: int) -> Chat:
    """Web UI"""
    query = """--sql
        SELECT *
        FROM chats
        WHERE id = %(chat_id)s
    """

    return await db_fetchone(Chat, query, dict(chat_id=chat_id))


@controller.get('/reactions')
async def get_reactions(channel_id: int, post_id: int) -> list[Post]:
    """Web UI"""
    query = """--sql
        SELECT *, data.key AS keys, data.value AS values
        FROM posts
        WHERE id = %(id)s AND channel_id = %(channel_id)s
        ORDER BY point_date ASC
    """

    return await db_fetchall(Post, query, dict(id=post_id, channel_id=channel_id), 'Reactions')


@controller.get('/chats')
async def get_chats(name: tp.Optional[str] = None, join_string: tp.Optional[str] = None) -> list[Chat]:
    """Web UI"""
    if not name and not join_string:
        raise BadRequestError('Either name or join_string must be provided')

    query = """--sql
        SELECT *
        FROM chats
        WHERE name ILIKE %(name)s OR join_string ILIKE %(join_string)s 
    """
    name = name and name + '%'
    join_string = join_string and join_string + '%'

    return await db_fetchall(Chat, query, dict(name=name, join_string=join_string), 'Chats')


def main():
    auto_discover = AutoDiscover(discover)
    auto_discover.start()

    server_config = uvicorn.Config(
        controller, host=config.CONTROLLER_HOST, port=config.CONTROLLER_PORT, log_config=None
    )
    server = uvicorn.Server(server_config)
    return server.serve()


async def _dev_main() -> None:
    async with clickhouse_default():
        server_config = uvicorn.Config(controller, host=config.CONTROLLER_HOST, port=config.CONTROLLER_PORT)
        await uvicorn.Server(server_config).serve()


if __name__ == '__main__':
    asyncio.run(_dev_main())
