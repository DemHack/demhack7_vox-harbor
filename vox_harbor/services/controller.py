import asyncio
import logging
import typing as tp
from itertools import chain, groupby
from operator import attrgetter

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pyrogram import utils

from vox_harbor.big_bot.structures import (
    Chat,
    CheckUserResult,
    Comment,
    CommentCount,
    EmptyResponse,
    Message,
    ParsedMsgURL,
    ParsedPostURL,
    Post,
    PostText,
    Sample,
    User,
    UserInfo,
    UsersAndChats,
)
from vox_harbor.common.config import config
from vox_harbor.common.db_utils import (
    clickhouse_default,
    db_execute,
    db_fetchall,
    db_fetchone,
    rows_to_unique_column,
    session_scope,
)
from vox_harbor.common.exceptions import BadRequestError, NotFoundError
from vox_harbor.gpt.main import Model

# from vox_harbor.services.auto_discover import AutoDiscover
from vox_harbor.services.shard_client import ShardClient
from vox_harbor.services.utils import parse_msg_url, parse_post_url

logger = logging.getLogger('vox_harbor.big_bot.services.controller')

controller = FastAPI()
controller.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)


@controller.get('/users_and_chats')
async def get_users_and_chats(query: str) -> UsersAndChats:
    users = _get_users(query)
    chats = _get_chats(query)
    users, chats = await asyncio.gather(users, chats)

    return UsersAndChats(users=users, chats=chats)


async def _get_chats(query: str) -> list[Chat]:
    getters = ((get_chat, 'chat_id'), (get_chats, 'join_string'), (get_chats, 'name'))
    chats: list[Chat] = []

    for func, arg_name in getters:
        logger.info('_get_chats - arg_name: %s', arg_name)  # todo logs mix due to async :(

        try:
            response = await func(**{arg_name: query})  # todo make truly async
        except Exception as exc:
            logger.info('_get_chats - response exc: %s', exc)
            continue

        if not isinstance(response, list):
            response = [response]
        logger.info('_get_chats - response: %s', response)

        chats += response

    logger.info('_get_chats - chats: %s', chats)

    return chats


async def _get_users(query: str) -> list[UserInfo]:
    getters = ((get_users, 'username'), (get_user, 'user_id'), (get_user_by_msg_url, 'msg_url'))
    users: list[UserInfo] = []

    for func, arg_name in getters:
        logger.info('_get_users - arg_name: %s', arg_name)  # todo logs mix due to async :(

        try:
            response = await func(**{arg_name: query})  # todo make truly async
        except Exception as exc:
            logger.info('_get_users - response exc: %s', exc)
            continue

        if not isinstance(response, list):
            response = [response]
        logger.info('_get_users - response: %s', response)

        users += response

    logger.info('_get_users - users: %s', users)

    return users


@controller.get('/healthcheck')
async def healthcheck() -> str:
    return 'OK'


@controller.get('/user')
async def get_user(user_id: int) -> UserInfo:
    """Web UI (consumer)"""
    return _users_to_user_info(await _get_users_by_user_ids(user_ids=[user_id]))


@controller.get('/user_by_msg_url')
async def get_user_by_msg_url(msg_url: str) -> UserInfo:
    try:
        parsed_url: ParsedMsgURL = parse_msg_url(msg_url)
    except ValueError as exc:
        raise BadRequestError(str(exc)) from exc

    logger.info('parsed_url: %s', repr(parsed_url))

    if isinstance(parsed_url.chat_id, int):
        query = """--sql
            SELECT *
            FROM chats
            WHERE id = %(id)s
            LIMIT 1
        """

        chat_id_100 = utils.get_channel_id(parsed_url.chat_id)
        logger.info('chat_id_100 - %s', chat_id_100)

        chat: Chat = await db_fetchone(Chat, query, dict(id=chat_id_100))
        bot_index, shard = chat.bot_index, chat.shard

    else:  # public chat
        bot_index = shard = 0

    async with ShardClient(shard) as shard_client:
        user = await shard_client.get_user_by_msg(parsed_url.chat_id, parsed_url.message_id, bot_index)

    if isinstance(user, EmptyResponse):
        raise NotFoundError('user')

    try:
        return await get_user(user.user_id)
    except NotFoundError:
        return UserInfo.from_user(user)


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
async def get_comments(user_id: int, offset: int = 0, fetch: int = 10) -> list[Comment]:
    """Web UI (consumer). Use with get_messages."""
    query = """--sql
        SELECT *
        FROM comments
        WHERE user_id = %(user_id)s
        ORDER BY date OFFSET %(offset)s ROW
        FETCH FIRST %(fetch)s ROWS ONLY;
    """

    return await db_fetchall(Comment, query, dict(user_id=user_id, offset=offset * fetch, fetch=fetch), name='comments')


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


@controller.get('/reactions_by_url')
async def get_reactions_by_url(post_url: str) -> list[Post]:
    """Web UI"""
    try:
        parsed_url: ParsedPostURL = parse_post_url(post_url)
    except ValueError as exc:
        raise BadRequestError(str(exc)) from exc

    query = """--sql
        SELECT *, data.key AS keys, data.value AS values
        FROM posts
        WHERE id = %(post_id)s
            AND channel_id = (
                SELECT id
                FROM chats
                WHERE join_string = %(channel_nick)s
            )
        ORDER BY point_date ASC
    """

    return await db_fetchall(
        Post, query, dict(post_id=parsed_url.post_id, channel_nick=parsed_url.channel_nick), 'Reactions'
    )


@controller.get('/reactions')
async def get_reactions(channel_id: int, post_id: int) -> list[Post]:
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


@controller.get('/post')
async def get_post(channel_id: int, post_id: int) -> PostText | EmptyResponse:
    """Web UI"""
    query = """--sql
        SELECT *, data.key AS keys, data.value AS values
        FROM posts
        WHERE id = %(id)s AND channel_id = %(channel_id)s
        LIMIT 1
    """

    post: Post = await db_fetchone(Post, query, dict(id=post_id, channel_id=channel_id), 'Post')

    async with ShardClient(post.shard) as shard_client:
        return await shard_client.get_post(post.channel_id, post.id, post.bot_index)


@controller.get('/random_users')
async def get_random_users() -> list[int]:
    async with session_scope() as session:
        await session.execute(
            'SELECT user_id FROM comments\n'
            'GROUP BY user_id\n'
            'HAVING count() > 20\n'
            'ORDER BY rand()\n'
            'LIMIT 100\n'
        )
        # await session.execute(
        #     'SELECT user_id, count(DISTINCT chat_id) as c FROM comments\n'
        #     'GROUP BY user_id\n'
        #     'HAVING c > 5\n'
        #     'ORDER BY rand()\n'
        # )
        data = await session.fetchall()

        return [x['user_id'] for x in data]


@controller.get('/sample')
async def get_sample(user_id: int) -> Sample:
    user = await get_user(user_id)

    channels = await db_fetchall(
        Sample.ChannelCommentsCount,
        'SELECT name as channel_name, count() as count FROM comments\n'
        'INNER JOIN chats ON comments.chat_id = chats.id\n'
        'WHERE comments.user_id = %(user_id)s\n'
        'GROUP BY chat_id, name\n'
        'ORDER BY count DESC',
        dict(user_id=user_id),
        raise_not_found=False,
    )

    comments = await get_comments(user_id, limit=1000)
    comments.reverse()

    recent_comments = comments[:10]
    recent_messages = [
        Sample.Comment(
            chat_name=m.chat,
            date=m.comment.date,
            text=m.text or '<no text>',
            post_id=m.comment.post_id,
        )
        for m in await get_messages(recent_comments)
    ]

    old_comments_count = max(min(len(comments) - 5, 5), 0)
    if old_comments_count > 0:
        old_comments = comments[-old_comments_count:]
        old_messages = [
            Sample.Comment(
                chat_name=m.chat,
                date=m.comment.date,
                text=m.text or '<no text>',
                post_id=m.comment.post_id,
            )
            for m in await get_messages(old_comments)
        ]
    else:
        old_messages = []

    return Sample(user=user, most_recent_comments=recent_messages, most_old_comments=old_messages, channels=channels)


@controller.get('/check_user')
async def check_user(user_id: int) -> CheckUserResult.Type | None:
    model = await Model.get_instance()
    return await model.check_user(user_id)


@controller.get('/comment_count')
async def get_comment_count(user_id: int) -> CommentCount:
    query = """--sql
        SELECT COUNT(*) comment_count
        FROM comments
        WHERE user_id = %(user_id)s
    """
    return await db_fetchone(CommentCount, query, dict(user_id=user_id), 'Comments')


def main():
    # auto_discover = AutoDiscover(discover)
    # if not config.READ_ONLY:
    #     auto_discover.start()

    server_config = uvicorn.Config(
        controller, host=config.CONTROLLER_HOST, port=config.CONTROLLER_PORT, log_config=None
    )
    server = uvicorn.Server(server_config)
    return server.serve()


async def _dev_main() -> None:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)6s - %(name)s - %(message)s')

    async with clickhouse_default():
        server_config = uvicorn.Config(controller, host=config.CONTROLLER_HOST, port=config.CONTROLLER_PORT)
        await uvicorn.Server(server_config).serve()


if __name__ == '__main__':
    asyncio.run(_dev_main())
