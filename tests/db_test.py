import pytest

from tests.fixtures import clickhouse
from tests.testing_data import USER
from vox_harbor.big_bot.structures import User
from vox_harbor.common import db_utils


@pytest.mark.skip
@pytest.mark.usefixtures("clickhouse")
@pytest.mark.asyncio_cooperative
async def test_clickhouse() -> None:
    query = """--sql
        SELECT *
        FROM users
        WHERE user_id = %(user_id)s
    """
    assert USER in (await db_utils.db_fetchall(User, query, dict(user_id=USER.user_id)))
