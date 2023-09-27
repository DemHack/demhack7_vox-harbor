from enum import StrEnum, auto
from pprint import pprint
from typing import Any, Optional

import pydantic as pd
import pydantic_settings as pds


class Mode(StrEnum):
    PROD = 'PROD'
    DEV_1 = 'DEV_1'
    DEV_2 = 'DEV_2'


class _Config(pds.BaseSettings):
    model_config = pds.SettingsConfigDict(env_file='.env', case_sensitive=True, extra='forbid')

    MODE: Mode

    CLICKHOUSE_HOST: str
    CLICKHOUSE_PORT: int = 9440
    CLICKHOUSE_PASSWORD: str

    CONTROLLER_HOST: str = '0.0.0.0'
    CONTROLLER_PORT: int = 8002

    SHARD_NUM: int = 0
    SHARD_HOST: str = '0.0.0.0'
    SHARD_PORT: int = 8001
    SHARD_ENDPOINTS: str | list[tuple[str, int]] = ''

    ACTIVE_BOTS_COUNT: int = 3
    MAX_CHATS_FOR_BOT: int = 200
    MIN_CHAT_MEMBERS_COUNT: int = 300
    MIN_CHANNEL_MEMBERS_COUNT: int = 5000
    AUTO_DISCOVER: bool = False
    READ_ONLY: bool = False

    # noinspection PyPep8Naming
    @pd.field_validator('SHARD_ENDPOINTS', mode='before')
    @classmethod
    def _2(cls, SHARD_ENDPOINTS: str) -> list[tuple[str, int]]:
        if not SHARD_ENDPOINTS:
            return []

        result = []
        for x in SHARD_ENDPOINTS.split(','):
            if ':' not in x:
                raise pd.ValidationError(f'Expected shard host in format host:port. Got {x}')

            host, port = x.split(':')
            result.append((host, int(port)))

        return result

    @property
    def shard_host(self) -> str:
        return self.SHARD_HOST or self.SHARD_ENDPOINTS[self.SHARD_NUM][0]

    @shard_host.setter
    def shard_host(self, host: str) -> None:
        self.SHARD_HOST = host  # pyright: ignore

    @property
    def shard_port(self) -> int:
        return self.SHARD_PORT or self.SHARD_ENDPOINTS[self.SHARD_NUM][1]  # pyright: ignore

    @shard_port.setter
    def shard_port(self, port: int) -> None:
        self.SHARD_PORT = port  # pyright: ignore

    def shard_url(self, shard: int):
        endpoint = self.SHARD_ENDPOINTS[shard]
        return f'http://{endpoint[0]}:{endpoint[1]}'


def override_config(cfg: dict[str, Any]) -> None:
    """Only use in CLI."""
    cfg = {k.upper(): v for k, v in cfg.items()}

    if extra_vars := set(cfg.keys()) - set(config.model_dump().keys()):
        raise ValueError(f'Invalid vars: {", ".join(extra_vars)}')

    vars(config).update(cfg)


config = _Config()  # type: ignore

if __name__ == '__main__':
    pprint(vars(config))
