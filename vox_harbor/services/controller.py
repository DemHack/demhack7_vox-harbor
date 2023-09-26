import asyncio
import logging
from itertools import chain, groupby
from operator import attrgetter
from typing import Iterable

import fastapi
import uvicorn
from fastapi import APIRouter

from tests.testing_data import USER
from vox_harbor.big_bot.structures import Comment, Message, User, UserInfo
from vox_harbor.common.config import config
from vox_harbor.common.db_utils import (
    clickhouse_default,
    db_execute,
    db_fetchall,
    rows_to_unique_column,
)
from vox_harbor.common.exceptions import NotFoundError

logger = logging.getLogger('vox_harbor.big_bot.services.controller')

controller = APIRouter()


@controller.get('/user')
async def get_user(user_id: int) -> UserInfo:
    """Web UI (consumer)"""
    return _users_to_user_info(await _get_users_by_user_ids(user_ids=[user_id]))


@controller.get('/user_by_msg_link')
async def get_user_by_msg_link(msg_link: str) -> UserInfo:
    """Web UI (consumer)"""
    user_id = USER.user_id

    # todo

    return await get_user(user_id=user_id)


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


async def _get_users_by_user_ids(user_ids: Iterable[int]):
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
async def get_messages_by_user_id(user_id: int) -> list[Message]:
    return await get_messages(await get_comments(user_id))


@controller.get('/comments')
async def get_comments(user_id: int) -> list[Comment]:
    """Web UI (consumer). Use with get_messages."""
    # todo later: offset - https://docs.pyrogram.org/api/methods/get_messages#get-messages

    query = """--sql
        SELECT *
        FROM comments
        WHERE user_id = %(user_id)s 
        ORDER BY date
    """
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
    tasks = []

    for shard, comments_by_shard in groupby(sorted_comments, attrgetter('shard')):
        # todo get_msgs: httpx
        # get_msgs = shard_service.get_messages(list(comments_by_shard))
        # tasks.append(get_msgs)
        ...

    return list(chain.from_iterable(await asyncio.gather(*tasks)))


@controller.post('/discover')
async def discover(join_string: str) -> None:
    """Web UI (consumer)"""
    # todo discover: WebUI -> Controller <-> shards
    # Controller -> shard with min known chats
    ...


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


def main():
    server_config = uvicorn.Config(
        controller, host=config.CONTROLLER_HOST, port=config.CONTROLLER_PORT, log_config=None
    )
    server = uvicorn.Server(server_config)
    return server.serve()


async def _dev_main() -> None:
    controller_app = fastapi.FastAPI()
    controller_app.include_router(controller, prefix='/api/controller')

    async with clickhouse_default():
        server_config = uvicorn.Config(controller_app, host=config.CONTROLLER_HOST, port=config.CONTROLLER_PORT)
        await uvicorn.Server(server_config).serve()


if __name__ == '__main__':
    asyncio.run(_dev_main())
