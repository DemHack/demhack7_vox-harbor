import asyncio
import fastapi
import logging
import uvicorn
from pyrogram.handlers import MessageHandler, RawUpdateHandler
from pyrogram.methods.utilities.idle import idle

from vox_harbor.big_bot import handlers
from vox_harbor.big_bot.bots import BotManager
from vox_harbor.big_bot.chats import ChatsManager
from vox_harbor.big_bot.configs import Config
from vox_harbor.big_bot.services.models import models_router
from vox_harbor.common.db_utils import with_clickhouse

logger = logging.getLogger('vox_harbor.main')


async def _big_bots_main():
    manager = await BotManager.get_instance()

    manager.register_handler(RawUpdateHandler(handlers.channel_confirmation_handler), 0)
    manager.register_handler(MessageHandler(handlers.process_message), 1)

    await manager.start()
    await ChatsManager.get_instance(manager)

    try:
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
        echo=False
    ):
        await _big_bots_main()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)6s - %(name)s - %(message)s',
    )

    app = fastapi.FastAPI(on_startup=[lambda: asyncio.create_task(big_bots_main())])
    app.include_router(models_router)

    uvicorn.run(app, workers=1)


if __name__ == '__main__':
    main()
