import asyncio
from collections import defaultdict
from itertools import chain, groupby
from operator import attrgetter
from typing import Iterable

from fastapi import APIRouter

from vox_harbor.big_bot.services import shard as shard_service
from vox_harbor.big_bot.structures import Comment, Message, User, UserInfo
from vox_harbor.common.db_utils import db_fetchall, rows_to_unique_column
from vox_harbor.common.exceptions import NotFoundError

controller_router = APIRouter()


@controller_router.get('/user')
async def get_user(user_id: int) -> UserInfo:
    """Web UI (consumer)"""
    return _users_to_user_info(await _get_sorted_users_by_user_ids(user_ids=[user_id]))


@controller_router.get('/users')
async def get_users(username: str, limit: int = 10) -> dict[str, list[UserInfo]]:
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

    users_info: list[UserInfo] = _users_to_users_info(await _get_sorted_users_by_user_ids(all_user_ids))
    id_to_info: dict[int, UserInfo] = {ui.user_id: ui for ui in users_info}

    username_to_users_info: dict[str, list[UserInfo]] = defaultdict(list)

    for username, user_ids in username_to_user_ids.items():
        for user_id in user_ids:
            username_to_users_info[username].append(id_to_info[user_id])

    return username_to_users_info


async def _get_sorted_users_by_user_ids(user_ids: Iterable[int]):
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

    for user_id, user_rows_iter in groupby(users_rows, attrgetter('user_id')):  # todo groupby_attr
        user_rows = list(user_rows_iter)
        usernames = rows_to_unique_column(user_rows, 'username')
        names = rows_to_unique_column(user_rows, 'name')
        users_info.append(UserInfo(user_id=user_id, usernames=usernames, names=names))

    return users_info


@controller_router.get('/messages_by_user_id')
async def get_messages_by_user_id(user_id: int) -> list[Message]:
    return await get_messages(await get_comments((user_id)))


@controller_router.get('/comments')
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


@controller_router.post('/messages')
async def get_messages(comments: list[Comment]) -> list[Message]:
    """Web UI (consumer)"""
    if not (messages := await _get_messages(comments)):
        raise NotFoundError('messages')
    return messages


async def _get_messages(comments: list[Comment]) -> list[Message]:
    sorted_comments = sorted(comments)
    tasks = []

    for shard, comments_by_shard in groupby(sorted_comments, attrgetter('shard')):
        # todo get_msgs: httpx
        get_msgs = shard_service.get_messages(list(comments_by_shard))

        tasks.append(get_msgs)

    return list(chain.from_iterable(await asyncio.gather(*tasks)))


@controller_router.post('/discover')
async def discover(join_string: str) -> None:
    """Web UI (consumer)"""
    # todo discover: WebUI -> Controller <-> shards
    # Controller -> shard with min known chats
    ...


@controller_router.post('/add_bot')
async def add_bot(name: str, session_string: str) -> None:
    """Web UI (admin)"""
    # todo


@controller_router.post('/remove_bot')
async def remove_bot(bot_id: int) -> None:
    """Web UI (admin)"""
    # todo
