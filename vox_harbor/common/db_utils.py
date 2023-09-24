import contextlib
import typing as tp
from operator import attrgetter

import asynch
import pydantic
from asynch.cursors import DictCursor
from asynch.pool import Pool

from vox_harbor.big_bot import structures
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


@contextlib.asynccontextmanager
async def session_scope(cursor_type=DictCursor) -> DictCursor:
    if pool is None:
        raise RuntimeError('out of `with_clickhouse()` scope')

    async with pool.acquire() as conn:
        async with conn.cursor(cursor_type) as cursor:
            yield cursor


async def db_fetchone(
    model: tp.Type[structures._Base],
    query: str,
    query_args: dict[str, tp.Any] | None = None,
    name: str | None = None,
    *,
    raise_not_found: bool = True,
) -> tp.Any:
    if query_args is None:
        query_args = {}

    async with session_scope() as session:
        await session.execute(query, query_args)
        try:
            return model.from_row(await session.fetchone())
        except AttributeError as exc:
            if raise_not_found:
                raise NotFoundError(name or model.__name__) from exc
            return None


async def db_fetchall(
    model: tp.Type[structures._Base],
    query: str,
    query_args: dict[str, tp.Any] | None = None,
    name: str | None = None,
    *,
    raise_not_found: bool = True,
) -> tp.Any:
    if query_args is None:
        query_args = {}

    async with session_scope() as session:
        await session.execute(query, query_args)
        try:
            return model.from_rows(await session.fetchall())
        except AttributeError as exc:
            if raise_not_found:
                raise NotFoundError(name or model.__name__) from exc

            return []


def rows_to_unique_column(rows: tp.Iterable[pydantic.BaseModel], column: str) -> list[tp.Any]:
    """Extract unique values from a column in an iterable of database rows."""
    return list(dict.fromkeys(map(attrgetter(column), rows)).keys())
