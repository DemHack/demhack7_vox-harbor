import asyncio
import contextlib
import datetime
import logging
import socket

from vox_harbor.big_bot import structures
from vox_harbor.common.config import config
from vox_harbor.common.db_utils import session_scope


class ClickHouseHandler(logging.Handler):
    INTERVAL = 5

    def __init__(self, max_size: int = 100_000):
        super().__init__()
        self.queue = asyncio.Queue(max_size)
        self.fqdn = socket.getfqdn()

    def process_record(self, record: logging.LogRecord) -> structures.Log:
        return structures.Log(
            created=datetime.datetime.fromtimestamp(record.created, datetime.timezone.utc),
            filename=record.filename,
            func_name=record.funcName,
            levelno=record.levelno,
            lineno=record.lineno,
            message=self.format(record),
            name=record.name,
            shard=config.SHARD_NUM,
            fqdn=self.fqdn,
        )

    async def batch_flush(self):
        batch = []

        size = self.queue.qsize()
        for _ in range(size):
            batch.append(self.process_record(self.queue.get_nowait()))

        async with session_scope() as session:
            await session.execute('INSERT INTO logs VALUES', [obj.model_dump() for obj in batch])

    async def loop(self):
        while True:
            await asyncio.sleep(self.INTERVAL)
            await self.batch_flush()

    def start(self):
        asyncio.create_task(self.loop())

    def emit(self, record: logging.LogRecord) -> None:
        self.queue.put_nowait(record)


@contextlib.asynccontextmanager
async def clickhouse_logger():
    handler = ClickHouseHandler()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)6s - %(name)s - %(message)s',
        handlers=[handler, logging.StreamHandler()],
    )

    try:
        handler.start()
        yield
    finally:
        await handler.batch_flush()
