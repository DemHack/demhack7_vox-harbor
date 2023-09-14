from asynch.pool import Pool
from asynch.cursors import DictCursor
import asynch
import contextlib

pool: Pool | None = None


@contextlib.asynccontextmanager
async def with_clickhouse(**kwargs):
    global pool

    pool = await asynch.create_pool(**kwargs)
    yield pool

    pool.close()
    await pool.wait_closed()
    pool = None


@contextlib.asynccontextmanager
async def session_scope(cursor_type=DictCursor) -> DictCursor:
    if pool is None:
        raise RuntimeError('out of `with_clickhouse()` scope')

    async with pool.acquire() as conn:
        async with conn.cursor(cursor_type) as cursor:
            yield cursor
