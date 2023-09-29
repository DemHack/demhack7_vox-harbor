import asyncio
import logging
import random
import typing as tp

from vox_harbor.big_bot import structures
from vox_harbor.common.exceptions import format_exception
from vox_harbor.common.db_utils import db_fetchone, session_scope


class AutoDiscover:

    logger = logging.getLogger('vox_harbor.big_bot.services.controller.auto_discover')

    def __init__(self, discover: tp.Callable[[str], tp.Awaitable]):
        self.discover = discover

    async def run_once(self):
        async with session_scope() as session:
            await session.execute('SELECT count() FROM discovered_chats')
            size = await session.fetchone()

        offset = random.randint(0, next(iter(size.values())))
        new_chat: structures.DiscoveredChat | None = await db_fetchone(
            structures.DiscoveredChat,
            'SELECT id, name, join_string\n'
            'FROM discovered_chats\n'
            'GROUP BY id, name, join_string\n'
            'HAVING sum(sign) > 0\n'
            'LIMIT 1 OFFSET %(offset)s',
            dict(offset=offset),
            raise_not_found=False,
        )

        if new_chat is None:
            self.logger.info('no chats')
            return

        self.logger.info('starting autodiscover for chat %s', new_chat)

        new_chat.sign = -1
        async with session_scope() as session:
            await session.execute('INSERT INTO discovered_chats VALUES', [new_chat.model_dump()])

        await self.discover(new_chat.join_string)

    async def loop(self):
        while True:
            try:
                await self.run_once()
            except Exception as e:
                self.logger.error('failed to run auto discover step: %s', format_exception(e, with_traceback=True))

            await asyncio.sleep(60)

    def start(self):
        asyncio.create_task(self.loop())
