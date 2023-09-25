import abc
import asyncio
import logging

import vox_harbor.big_bot
from vox_harbor.big_bot.handlers import process_message
from vox_harbor.common.exceptions import format_exception


class Task(abc.ABC):
    logger = logging.getLogger('vox_harbor.big_bot.tasks')

    MAX_RETRIES = 10
    TIMEOUT = 60

    def __init__(self):
        self._retries_count = 0

    async def do_step(self):
        if self.failed:
            return

        try:
            async with asyncio.timeout(self.TIMEOUT):
                await self.step()
        except Exception as e:
            self._retries_count += 1
            if self.failed:
                self.logger.error('max retires reached for task %s: %s', self, format_exception(e, with_traceback=True))
            else:
                self.logger.error('failed to process step for task %s: %s', self, format_exception(e))

    async def step(self):
        raise NotImplementedError()

    @property
    def progress(self) -> float:
        raise NotImplementedError()

    @property
    def id(self) -> str:
        raise NotImplementedError()

    @property
    def failed(self) -> bool:
        return self._retries_count >= self.MAX_RETRIES

    @property
    def done(self) -> bool:
        return self.failed or self.finished

    @property
    def finished(self) -> bool:
        raise NotImplementedError()

    def __str__(self) -> str:
        return f'{self.__class__.__name__}{{{self.id}}} ({round(self.progress, 1)}%)'

    __repr__ = __str__


class HistoryTask(Task):
    logger = logging.getLogger('vox_harbor.big_bot.tasks.history')

    DELTA = 3

    def __init__(self, bot: 'vox_harbor.big_bot.bots.Bot', chat_id: int, start_id: int = 0, end_id: int = 0, limit: int = 100):
        super().__init__()
        self.bot = bot

        self.chat_id = chat_id
        self.start = start_id
        self.end = end_id
        self.limit = limit

        self._id = f'{self.chat_id}_{self.start}_{self.end}'

        self.count = 0
        self.current_offset = start_id
        self._finished = False

    @property
    def total(self):
        return self.start - self.end

    async def step(self):
        messages = await self.bot.get_history(self.chat_id, self.current_offset, self.end, self.limit)
        if not messages:
            self._finished = True
            return

        if not self.start:
            self.start = messages[0].id

        for message in messages:
            self.count += 1

            await process_message(self.bot, message)
            self.current_offset = message.id

    @property
    def progress(self) -> float:
        return ((self.start - self.current_offset) / self.total) * 100 if self.total != 0 else 100.

    @property
    def finished(self) -> bool:
        if self._finished:
            return True

        if not self.start and not self.end:
            return False

        return self.total - self.start + self.current_offset < self.DELTA

    @property
    def id(self) -> str:
        return self._id


class TaskManager:
    logger = logging.getLogger('vox_harbor.big_bot.tasks')

    def __init__(self):
        self.tasks: dict[str, Task] = {}

    async def add_task(self, task: Task):
        if task.id in self.tasks:
            self.logger.info('already processing this task %s', task)
            return

        self.logger.info('new task %s', task)
        self.tasks[task.id] = task

    async def loop(self):
        while True:
            if not self.tasks:
                await asyncio.sleep(10)

            while self.tasks:
                await asyncio.gather(*(
                    task.do_step()
                    for task in self.tasks.values()
                ))

                self.logger.info(self.tasks)
                for task in self.tasks.copy().values():
                    if task.done:
                        del self.tasks[task.id]

    def start(self):
        asyncio.create_task(self.loop())

    @classmethod
    async def get_instance(cls):
        global _task_manager
        if _task_manager is not None:
            return _task_manager

        _task_manager = cls()
        return _task_manager


_task_manager: TaskManager | None = None
