import logging
import asyncio

from vox_harbor.big_bot.main import big_bots_main
from vox_harbor.big_bot.services.main import main as server_main

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)6s - %(name)s - %(message)s',
)

if __name__ == '__main__':
    asyncio.run(big_bots_main(server_main))
