import pydantic
import typing as tp
import datetime


class _Base(pydantic.BaseModel):

    @classmethod
    def from_row(cls, row: dict) -> tp.Self:
        return cls.model_validate(row)

    @classmethod
    def from_rows(cls, rows: list[dict]) -> list[tp.Self]:
        return [cls.from_row(x) for x in rows]


class Bot(_Base):
    id: int
    shard: int

    name: str
    session_string: str


class BrokenBot(_Base):
    id: int


class Chat(_Base):
    id: int
    name: str
    join_string: str

    shard: int
    bot_index: int
    added: datetime.datetime


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

    username: str | None
    first_name: str | None
    last_name: str | None

    bot_index: int
    shard: int


class EmptyResponse(_Base):
    pass


class Message(_Base):
    text: str
