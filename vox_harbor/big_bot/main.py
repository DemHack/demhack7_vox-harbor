import asyncio
import logging

import fastapi
import uvicorn
from pyrogram.handlers import MessageHandler, RawUpdateHandler
from pyrogram.methods.utilities.idle import idle

from vox_harbor.big_bot import handlers, structures
from vox_harbor.big_bot.bots import Bot, BotManager
from vox_harbor.big_bot.chats import ChatsManager
from vox_harbor.big_bot.configs import Config
from vox_harbor.big_bot.tasks import HistoryTask, TaskManager
from vox_harbor.common.db_utils import session_scope, with_clickhouse

logger = logging.getLogger('vox_harbor.big_bot.main')

app = fastapi.FastAPI(on_startup=[lambda: asyncio.create_task(big_bots_main())])


async def generate_task(tasks: TaskManager, bot: Bot, chat_id: int):
    async with session_scope() as session:
        await session.execute(
            'SELECT chat_id, min(min_message_id) as min_message_id, max(max_message_id) as max_message_id FROM comments_range_mv WHERE chat_id = %(chat_id)s\n'
            'GROUP BY chat_id',
            {'chat_id': chat_id},
        )

        try:
            comment = structures.CommentRange.from_row(await session.fetchone())
        except AttributeError:
            comment = structures.CommentRange(chat_id=chat_id, min_message_id=0, max_message_id=0)
            # todo: check if this chat is channels.

    if comment.max_message_id:
        await tasks.add_task(
            HistoryTask(
                bot=bot,
                chat_id=chat_id,
                start_id=0,
                end_id=comment.max_message_id,
            )
        )

    # todo: check if necessary
    await tasks.add_task(
        HistoryTask(
            bot=bot,
            chat_id=chat_id,
            start_id=comment.min_message_id,
            end_id=0,
        )
    )


async def generate_tasks():
    tasks = await TaskManager.get_instance()
    bots = await BotManager.get_instance()

    coro = []
    for bot in bots:
        for chat_id in await bot.get_subscribed_chats():
            coro.append(generate_task(tasks, bot, chat_id))

    await asyncio.gather(*coro)
    tasks.start()


async def _big_bots_main():
    handlers.inserter.start()
    manager = await BotManager.get_instance()

    manager.register_handler(RawUpdateHandler(handlers.channel_confirmation_handler), 0)
    manager.register_handler(MessageHandler(handlers.process_message), 1)

    await manager.start()

    # todo: turn it on.
    # asyncio.create_task(generate_tasks())
    try:
        await ChatsManager.get_instance(manager)
        await idle()

    finally:
        await manager.stop()


async def big_bots_main():
    async with with_clickhouse(
        host=Config.CLICKHOUSE_HOST,
        port=Config.CLICKHOUSE_PORT,
        database='default',
        user='default',
        password=Config.CLICKHOUSE_PASSWORD,
        secure=True,
        echo=False,
        minsize=10,
        maxsize=50,
    ):
        await _big_bots_main()


def main():
    uvicorn.run(app, workers=1)
