import pytest

from tests.fixtures import controller  # type: ignore
from tests.testing_data import COMMENTS, MESSAGES, USER, USER_INFO
from tests.utils import is_sub_iterable
from vox_harbor.services.controller import (
    get_comments,
    get_messages,
    get_messages_by_user_id,
    get_user,
    get_users,
)


@pytest.mark.usefixtures("controller")
@pytest.mark.asyncio_cooperative
async def test_user_s() -> None:
    assert USER_INFO == (await get_user(USER.user_id))
    assert USER_INFO in (await get_users(USER.username))


@pytest.mark.usefixtures("controller")
@pytest.mark.asyncio_cooperative
async def test_comments() -> None:
    assert is_sub_iterable(COMMENTS, await get_comments(USER.user_id))


@pytest.mark.usefixtures("controller")
@pytest.mark.asyncio_cooperative
async def test_messages() -> None:
    assert is_sub_iterable(MESSAGES, await get_messages(COMMENTS))
    assert is_sub_iterable(MESSAGES, await get_messages_by_user_id(USER.user_id))
