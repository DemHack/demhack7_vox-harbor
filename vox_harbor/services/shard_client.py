from typing import Any, Iterable

import httpx

from vox_harbor.big_bot.structures import Comment, Message
from vox_harbor.common.config import config


class ShardClient(httpx.AsyncClient):
    def __init__(self, shard: int, **kwargs: Any) -> None:
        kwargs['base_url'] = config.shard_url(shard)
        super().__init__(**kwargs)

    def _concat_url(self, postfix: str) -> str:
        return str(self.base_url) + postfix

    async def get_messages(self, sorted_comments: Iterable[Comment]) -> list[Message]:
        url = self._concat_url('/messages')
        json: list[dict[str, Any]] = [c.model_dump(mode='json') for c in sorted_comments]
        return [Message(**m) for m in (await self.post(url, json=json)).json()]

    async def get_known_chats_count(self) -> int:
        url = self._concat_url('/known_chats_count')
        return (await self.get(url)).json()

    async def discover(self, join_string: str) -> httpx.Response:
        url = self._concat_url('/discover')
        return await self.post(url, params=dict(join_string=join_string))
