import datetime
import enum
import typing as tp

import pydantic


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
        PRIVATE = 'PRIVATE'

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

    def __eq__(self, other: tp.Self):
        return vars(self) == vars(other)


class CommentRange(_Base):
    chat_id: int
    min_message_id: int
    max_message_id: int


class Message(pydantic.BaseModel):
    text: str | None
    comment: Comment

    def __eq__(self, other: tp.Self):
        return self.text == other.text and self.comment == other.comment


class User(_Base):
    user_id: int
    username: str
    name: str


class UserInfo(_Base):
    user_id: int
    usernames: list[str]
    names: list[str]

    def __eq__(self, other: tp.Self):
        return (
            self.user_id == other.user_id
            and set(self.usernames) == set(other.usernames)
            and set(self.names) == set(other.names)
        )


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


class NewPost(_Base):
    id: int
    channel_id: int
    post_date: datetime.datetime

    bot_index: int
    shard: int


class Post(NewPost):
    point_date: datetime.datetime
    keys: list[str]
    values: list[int]


class DiscoverRequest(_Base):
    join_string: str
