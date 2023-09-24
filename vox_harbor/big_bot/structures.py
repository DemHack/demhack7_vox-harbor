import datetime
import enum
import pydantic
import typing as tp


def model_to_tuple(pydantic_obj: pydantic.BaseModel) -> tuple[tp.Any, ...]:
    return tuple(pydantic_obj.model_dump().values())


class _Base(pydantic.BaseModel):
    @classmethod
    def from_row(cls, row: dict[tp.Any, tp.Any]) -> tp.Self:
        return cls.model_validate(row)

    @classmethod
    def from_rows(cls, rows: list[dict[tp.Any, tp.Any]]) -> list[tp.Self]:
        return [cls.from_row(x) for x in rows]


class Bot(_Base):
    id: int
    shard: int

    name: str
    session_string: str


class BrokenBot(_Base):
    id: int


class Chat(_Base):
    class Type(enum.StrEnum):
        CHAT = 'CHAT'
        CHANNEL = 'CHANNEL'

    id: int
    name: str
    join_string: str

    shard: int
    bot_index: int
    added: datetime.datetime
    type: Type


class DiscoveredChat(_Base):
    id: int
    name: str
    join_string: str
    subscribers_count: int
    sign: int


class ChatUpdate(_Base):
    shard: int
    bot_index: int
    added: datetime.datetime


class Comment(_Base):
    user_id: int
    date: datetime.datetime
    chat_id: int
    message_id: int

    channel_id: int | None
    post_id: int | None

    bot_index: int
    shard: int

    def __lt__(self, other: tp.Self) -> bool:
        to_tuple: tp.Callable[[tp.Self], tuple[int, ...]] = lambda c: (c.shard, c.bot_index, c.chat_id)
        return to_tuple(self) < to_tuple(other)


class CommentRange(_Base):
    chat_id: int
    min_message_id: int
    max_message_id: int


class Message(pydantic.BaseModel):
    text: str | None
    comment: Comment


class User(_Base):
    user_id: int
    username: str
    name: str


class UserInfo(_Base):
    user_id: int
    usernames: list[str]
    names: list[str]


class Log(_Base):
    created: datetime.datetime
    filename: str
    func_name: str
    levelno: int
    lineno: int
    message: str
    name: str
    shard: int
    fqdn: str


class DiscoverRequest(_Base):
    join_string: str


class ShardLoad(pydantic.BaseModel):
    """
    Args:
        shard: Shard ID
        known_chats: The sum of all chats known by bots on the shard
        lazy_bot_index: The index of the bot with the least known chats
    """

    shard: int
    known_chats: int
    lazy_bot_index: int

    def __lt__(self, other: tp.Self) -> bool:
        return self.known_chats < other.known_chats
