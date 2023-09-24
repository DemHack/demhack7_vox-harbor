import asyncio
import logging

from vox_harbor.big_bot.configs import Config
from vox_harbor.big_bot.main import big_bots_main
from vox_harbor.big_bot.services.main import main as server_main
from vox_harbor.common.db_utils import with_clickhouse
from vox_harbor.common.logging_utils import ClickHouseHandler

handler = ClickHouseHandler()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)6s - %(name)s - %(message)s',
    handlers=[handler, logging.StreamHandler()]
)


async def main():
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
        try:
            handler.start()
            await big_bots_main(server_main)
        finally:
            await handler.batch_flush()


if __name__ == '__main__':
    asyncio.run(main())
