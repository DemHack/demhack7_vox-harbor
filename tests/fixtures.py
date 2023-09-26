import asyncio

import pytest

from vox_harbor.common import db_utils
from vox_harbor.common.db_utils import clickhouse_default
from vox_harbor.services.controller import _dev_main  # type: ignore


@pytest.fixture(scope='session')
async def controller() -> None:
    asyncio.create_task(_dev_main())

    while db_utils.pool is None:
        await asyncio.sleep(1)
    await asyncio.sleep(3)


@pytest.fixture
async def clickhouse():
    async with db_utils.clickhouse_default():
        yield
