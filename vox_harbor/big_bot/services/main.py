import asyncio
import logging
import typing as tp

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


def main() -> tp.Awaitable:
    config = uvicorn.Config("vox_harbor.big_bot.services.main:app", host="0.0.0.0", port=8002)
    server = uvicorn.Server(config)
    return server.serve()
