import asyncio
import fire
import typing as tp

from vox_harbor.big_bot.main import big_bots_main
from vox_harbor.common.config import override_config, config
from vox_harbor.common.db_utils import with_clickhouse
from vox_harbor.common.logging_utils import clickhouse_logger
from vox_harbor.services.controller import main as controller_main


def _cli(service: str, **cfg: tp.Any) -> None:
    """
    Vox Harbor

    Args:
        service: controller or shard-<SHARD_NUM>
        cfg: host, port, etc.
    """
    match service.split('-'):
        case ['c' | 'controller']:
            task = controller_main
            prefix = 'controller_'

        case ['s' | 'shard', shard_num] if shard_num.isdigit():
            task = big_bots_main
            prefix = 'shard_'
            cfg['shard_num'] = int(shard_num)

        case _:
            raise ValueError(f'Invalid service: {service}')

    for var in ('host', 'port'):
        if var in cfg:
            cfg[prefix + var] = cfg.pop(var)

    override_config(cfg)
    asyncio.run(_main(task))


async def _main(task: tp.Callable):
    async with with_clickhouse(
        host=config.CLICKHOUSE_HOST,
        port=config.CLICKHOUSE_PORT,
        database='default',
        user='default',
        password=config.CLICKHOUSE_PASSWORD,
        secure=True,
        echo=False,
        minsize=10,
        maxsize=50,
    ):
        async with clickhouse_logger():
            await task()


def main():
    fire.Fire(_cli)
