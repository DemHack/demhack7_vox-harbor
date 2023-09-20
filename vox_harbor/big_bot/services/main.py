import asyncio
import logging

import uvicorn
from fastapi import FastAPI

from vox_harbor.big_bot.services.controller import controller_router
from vox_harbor.big_bot.services.shard import shard_router

logging.basicConfig(  # todo remove later
    level=logging.INFO,
    format='%(asctime)s %(levelname)6s - %(name)s - %(message)s',
)
logger = logging.getLogger('vox_harbor.big_bot.services.main')

app = FastAPI()
app.include_router(controller_router, prefix='/api/controller')
app.include_router(shard_router, prefix='/api/shard')


def _uvicorn_server(config: uvicorn.Config) -> asyncio.Task[None]:
    server = uvicorn.Server(config)
    return asyncio.create_task(server.serve())


async def main() -> None:
    config_bots = uvicorn.Config("vox_harbor.big_bot.main:app", host="0.0.0.0", port=8001)
    config_controller = uvicorn.Config("vox_harbor.big_bot.services.main:app", host="0.0.0.0", port=8002)
    configs = (config_bots, config_controller)

    logger.info('servers started')

    await asyncio.gather(*map(_uvicorn_server, configs))


if __name__ == '__main__':
    asyncio.run(main())
