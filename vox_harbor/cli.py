from typing import Any

import fire

from vox_harbor.common.config import override_config

# todo from ... import controller and shard


def _cli(service: str, **cfg: Any) -> None:
    """
    Vox Harbor

    Args:
        service: controller or shard-<SHARD_NUM>
        cfg: host, port, etc.
    """
    match service.split('-'):
        case ['c' | 'controller']:
            service_main = 'controller.main'  # todo
            prefix = 'controller_'

        case ['s' | 'shard', shard_num] if shard_num.isdigit():
            service_main = 'shard.main'  # todo
            prefix = 'shard_'
            cfg['shard_num'] = int(shard_num)

        case _:
            raise ValueError(f'Invalid service: {service}')

    for var in ('host', 'port'):
        if var in cfg:
            cfg[prefix + var] = cfg.pop(var)

    override_config(cfg)
    # todo service_main()


def main():
    fire.Fire(_cli)
