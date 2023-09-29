import asyncio
import datetime
import logging

import vox_harbor.big_bot
from vox_harbor.big_bot import structures
from vox_harbor.big_bot.handlers import inserter
from vox_harbor.common.config import config
from vox_harbor.common.db_utils import db_fetchall, db_fetchone
from vox_harbor.common.exceptions import format_exception


class PostManager:
    logger = logging.getLogger('vox_harbor.big_bot.posts')

    def __init__(self, bots: 'vox_harbor.big_bot.bots.BotManager'):
        self._last_post_point = {}
        self.bots = bots

    @staticmethod
    def _get_update_interval(post_date: datetime.datetime) -> int:
        delta = datetime.datetime.utcnow() - post_date
        if delta < datetime.timedelta(hours=1):
            return 60
        if delta < datetime.timedelta(hours=4):
            return 120
        if delta < datetime.timedelta(days=1):
            return 600

        return 3600

    async def process_post(self, post: structures.NewPost):
        if not (last_updated := self._last_post_point.get(post.id)):
            point: structures.Post = await db_fetchone(
                structures.Post,
                'SELECT id, channel_id, post_date, point_date, `data.key` as keys, `data.value` as values, bot_index, shard\n'
                'FROM posts WHERE id = %(id)s ORDER BY point_date DESC LIMIT 1',
                dict(id=post.id),
                raise_not_found=False
            )

            if not point:
                self.logger.critical('logical error: projection record is absense in the original table')
                return

            last_updated = point.point_date
            self._last_post_point[post.id] = last_updated

        if datetime.datetime.utcnow() - last_updated > datetime.timedelta(seconds=self._get_update_interval(post.post_date)):
            bot = self.bots[post.bot_index]

            try:
                message = await bot.get_messages(chat_id=post.channel_id, message_ids=post.id)
                if not message or not message.chat:
                    self._last_post_point[post.id] = datetime.datetime.utcnow()  # post was deleted
                    return

                await inserter.insert_post(message, bot.index)
                self._last_post_point[post.id] = datetime.datetime.utcnow()
            except Exception as e:
                self.logger.error('unable to process a post %s: %s', post, format_exception(e, with_traceback=True))

    async def run_once(self):
        new_posts = await db_fetchall(
            structures.NewPost,
            'SELECT * FROM new_posts_mv\n'
            'WHERE post_date > now() - INTERVAL 3 DAY\n'
            'AND shard = %(shard)s',
            dict(shard=config.SHARD_NUM),
            raise_not_found=False,
        )

        await asyncio.gather(*(self.process_post(post) for post in new_posts))
        self.logger.info('processed %s posts', len(new_posts))

    async def loop(self):
        while True:
            try:
                await self.run_once()
                await asyncio.sleep(30)
            except Exception as e:
                self.logger.error('failed in post manager loop %s', format_exception(e, with_traceback=True))

    def start(self):
        asyncio.create_task(self.loop())

    @classmethod
    async def get_instance(cls, bots: 'vox_harbor.big_bot.bots.BotManager'):
        global _posts
        if _posts is None:
            _posts = cls(bots)

        return _posts


_posts = None
