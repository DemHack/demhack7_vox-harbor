import logging
from typing import Optional

import openai

from vox_harbor.big_bot import structures
from vox_harbor.common.config import config
from vox_harbor.common.exceptions import format_exception

openai.api_key = config.OPENAI_KEY


class Model:
    logger = logging.getLogger('vox_harbor.gpt.model')

    HEADER = (
        'Перед тобой пример комментариев конкретного пользователя в телеграм чатах\n'
        'Определи по этим признакам является ли пользователь кремлеботом (KREMLIN_BOT), трольботом (TROLL_BOT), ботом Кадырова (KADYROV_BOT) или же обычным пользователем (USER).\n'
        'В ответ выведи только одно слово.\n\n'
    )

    TEMPLATE = (
        'user_id: {user_id}\n'
        'usernames: {usernames}\n'
        'names: {names}\n'
        '\n'
        '5 самых свежих комментариев:\n'
        '{recent_comments}'
        '\n\n'
        '5 самых старых комментариев:\n'
        '{old_comments}'
        '\n\n'
        'Кол-во сообщений по чатам:\n'
        '{channels}\n'
    )

    def __init__(self):
        from vox_harbor.services import controller  # circular imports

        self.controller = controller

    def generate_request(self, sample: structures.Sample) -> str:
        recent_comments = []
        for comment in sample.most_recent_comments:
            recent_comments.append(f'<{comment.date}> {comment.chat_name} ({comment.post_id}): {repr(comment.text)}')

        old_comments = []
        for comment in sample.most_old_comments:
            old_comments.append(f'<{comment.date}> {comment.chat_name} ({comment.post_id}): {repr(comment.text)}')

        channels = []
        for c in sample.channels:
            channels.append(f'{c.channel_name} - {c.count}')

        return self.TEMPLATE.format(
            user_id=sample.user.user_id,
            usernames=', '.join(sample.user.usernames),
            names=', '.join(sample.user.names),
            recent_comments='\n'.join(recent_comments),
            old_comments='\n'.join(old_comments),
            channels='\n'.join(channels),
        )

    async def check_user(self, user_id: int) -> Optional[structures.CheckUserResult.Type]:
        value = None
        try:
            sample = await self.controller.get_sample(user_id)
            text = self.generate_request(sample)

            data = [
                {
                    'role': 'system',
                    'content': self.HEADER,
                },
                {
                    'role': 'user',
                    'content': text,
                },
            ]

            completion = await openai.ChatCompletion.acreate(model=config.OPENAI_MODEL, messages=data)

            return structures.CheckUserResult.Type(completion.choices[0].message.content)
        except Exception as e:
            self.logger.error(f'failed to check user, value %s: %s', value, format_exception(e, with_traceback=True))

    @classmethod
    async def get_instance(cls):
        global _model
        if _model is None:
            _model = cls()

        return _model


_model: Model | None = None
