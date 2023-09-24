from enum import StrEnum, auto
from pprint import pprint
from typing import Any, Optional

import pydantic as pd
import pydantic_settings as pds


class Mode(StrEnum):
    PROD = auto()
    DEV_1 = auto()
    DEV_2 = auto()


class _Config(pds.BaseSettings):
    model_config = pds.SettingsConfigDict(env_file='.env', case_sensitive=True, extra='forbid')

    MODE: Mode

    CLICKHOUSE_HOST: str
    CLICKHOUSE_PORT: int = 9440
    CLICKHOUSE_PASSWORD: str

    CONTROLLER_HOST: str = '127.0.0.1'
    CONTROLLER_PORT: int = 8002

    SHARD_NUM: int = 0
    SHARD_HOST: Optional[str] = None
    SHARD_PORT: Optional[int] = None
    SHARD_HOSTS: str | list[str] = '127.0.0.1'
    SHARD_PORTS: str | list[int] = '8001'

    ACTIVE_BOTS_COUNT: int = 3
    MAX_CHATS_FOR_BOT: int = 200
    MIN_CHAT_MEMBERS_COUNT: int = 300
    MIN_CHANNEL_MEMBERS_COUNT: int = 5000
    AUTO_DISCOVER: bool = False

    @pd.field_validator('MODE', mode='before')
    @classmethod
    def _1(cls, MODE: str) -> str:
        return MODE.lower()

    @pd.field_validator('SHARD_HOSTS', mode='before')
    @classmethod
    def _2(cls, SHARD_HOSTS: str) -> list[str]:
        return SHARD_HOSTS.split(',')

    @pd.field_validator('SHARD_PORTS', mode='before')
    @classmethod
    def _3(cls, SHARD_PORTS: str) -> list[int]:
        return list(map(int, SHARD_PORTS.split(',')))

    @property
    def controller_url(self) -> str:
        return f'http://{self.CONTROLLER_HOST}:{self.CONTROLLER_PORT}'

    @property
    def shard_host(self) -> str:
        return self.SHARD_HOST or self.SHARD_HOSTS[self.SHARD_NUM]

    @shard_host.setter
    def shard_host(self, host: str) -> None:
        self.SHARD_HOST = host  # type: ignore

    @property
    def shard_port(self) -> int:
        return self.SHARD_PORT or self.SHARD_PORTS[self.SHARD_NUM]  # type: ignore

    @shard_port.setter
    def shard_port(self, port: int) -> None:
        self.SHARD_PORT = host  # type: ignore

    @property
    def shard_url(self) -> str:
        return f'http://{self.shard_host}:{self.shard_port}'


def override_config(cfg: dict[str, Any]) -> None:
    """Only use in CLI."""
    cfg = {k.upper(): v for k, v in cfg.items()}

    if extra_vars := set(cfg.keys()) - set(config.model_dump().keys()):
        raise ValueError(f'Invalid vars: {", ".join(extra_vars)}')

    vars(config).update(cfg)


config = _Config()  # type: ignore

if __name__ == '__main__':
    pprint(vars(config))
