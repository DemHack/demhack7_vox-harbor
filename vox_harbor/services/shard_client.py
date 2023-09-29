from typing import Any, Iterable

import httpx

from vox_harbor.big_bot.structures import Comment, EmptyResponse, Message, PostText
from vox_harbor.common.config import config


class ShardClient(httpx.AsyncClient):
    def __init__(self, shard: int, **kwargs: Any) -> None:
        kwargs['base_url'] = config.shard_url(shard)
        super().__init__(**kwargs)

    async def get_messages(self, sorted_comments: Iterable[Comment]) -> list[Message]:
        json: list[dict[str, Any]] = [c.model_dump(mode='json') for c in sorted_comments]
        messages = (await self.post('/messages', json=json)).json()
        return [Message.model_validate(m) for m in messages]

    async def get_known_chats_count(self) -> int:
        return (await self.get('/known_chats_count')).json()

    async def discover(self, join_string: str, ignore_protection: bool = False) -> None:
        await self.post('/discover', params=dict(join_string=join_string, ignore_protection=ignore_protection))

    async def get_post(self, channel_id: int, post_id: int, bot_index: int) -> PostText | EmptyResponse:
        return PostText.model_validate(
            (await self.get('/post', params=dict(channel_id=channel_id, post_id=post_id, bot_index=bot_index))).json()
        )
