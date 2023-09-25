import asyncio
import logging

from pyrogram.handlers import MessageHandler, RawUpdateHandler

from vox_harbor.big_bot import handlers
from vox_harbor.big_bot.bots import BotManager
from vox_harbor.big_bot.chats import ChatsManager
from vox_harbor.big_bot.tasks import TaskManager
from vox_harbor.services.shard import main as shard_main

logger = logging.getLogger('vox_harbor.big_bot.main')


async def generate_tasks():
    tasks = await TaskManager.get_instance()
    bots = await BotManager.get_instance()
    chats = await ChatsManager.get_instance()

    coro = []
    for bot in bots:
        for chat_id in await bot.get_subscribed_chats():
            coro.append(bot.generate_history_task(chats, chat_id))

    await asyncio.gather(*coro)
    tasks.start()


async def big_bots_main():
    handlers.inserter.start()
    manager = await BotManager.get_instance()

    manager.register_handler(RawUpdateHandler(handlers.channel_confirmation_handler), 0)
    manager.register_handler(MessageHandler(handlers.process_message), 1)

    await manager.start()

    asyncio.create_task(generate_tasks())
    try:
        await ChatsManager.get_instance(manager)
        await shard_main()

    finally:
        await manager.stop()

