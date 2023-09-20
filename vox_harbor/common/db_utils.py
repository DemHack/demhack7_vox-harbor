import contextlib
from functools import partial
from operator import attrgetter
from typing import Any, Iterable, Optional

import asynch
import pydantic
from asynch.cursors import DictCursor
from asynch.pool import Pool

from vox_harbor.big_bot.configs import Config
from vox_harbor.common.exceptions import NotFoundError

pool: Pool | None = None


@contextlib.asynccontextmanager
async def with_clickhouse(**kwargs):
    global pool

    pool = await asynch.create_pool(**kwargs)
    yield pool

    pool.close()
    await pool.wait_closed()
    pool = None


clickhouse_default = partial(
    with_clickhouse,
    host=Config.CLICKHOUSE_HOST,
    port=Config.CLICKHOUSE_PORT,
    database='default',
    user='default',
    password=Config.CLICKHOUSE_PASSWORD,
    secure=True,
    echo=False,
    minsize=10,
    maxsize=50,
)


@contextlib.asynccontextmanager
async def session_scope(cursor_type=DictCursor) -> DictCursor:
    if pool is None:
        raise RuntimeError('out of `with_clickhouse()` scope')

    async with pool.acquire() as conn:
        async with conn.cursor(cursor_type) as cursor:
            yield cursor


async def _db_fetch(
    model: Any,
    query: str,
    query_args: dict[str, Any],
    name: Optional[str] = None,
    *,
    fetch_all: bool,
) -> Any:
    async with session_scope() as session:
        await session.execute(query, query_args)

        if fetch_all:
            fetch, convert = session.fetchall, model.from_rows
        else:
            fetch, convert = session.fetchone, model.from_row

        try:
            return convert(await fetch())
        except AttributeError as exc:
            raise NotFoundError(name or model.__name__) from exc


db_fetchone = partial(_db_fetch, fetch_all=False)
db_fetchall = partial(_db_fetch, fetch_all=True)


def rows_to_unique_column(rows: Iterable[pydantic.BaseModel], column: str) -> list[Any]:
    """Extract unique values from a column in an iterable of database rows."""
    return list(dict.fromkeys(map(attrgetter(column), rows)).keys())
