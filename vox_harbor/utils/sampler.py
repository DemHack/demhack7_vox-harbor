import aioconsole
import asyncio
import httpx
import json

queue = asyncio.Queue(maxsize=9)

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
    '10 самых свежих комментариев:\n'
    '{recent_comments}'
    '\n\n'
    '10 самых старых комментариев:\n'
    '{old_comments}'
    '\n\n'
    'Кол-во сообщений по чатам:\n'
    '{channels}\n'
)

try:
    with open('output.json', 'r') as fd:
        data = json.load(fd)
except:
    with open('output.json', 'w') as fd:
        json.dump([], fd)
        data = []

with open('output.json1', 'w') as _fd:
    for x in data:
        messages = {
            'messages': [
                {
                    'role': 'system',
                    'content': HEADER + x['question']
                },
                {
                    'role': 'system',
                    'content': x['answer']
                }
            ]
        }
        _fd.write(json.dumps(messages, ensure_ascii=False) + '\n')


async def get_samples(user_id: int) -> str | None:
    try:
        async with httpx.AsyncClient() as client:
            sample_data = (await client.get('http://localhost:8002/sample', params=dict(user_id=user_id))).json()

        recent_comments = []
        for comment in sample_data['most_recent_comments']:
            recent_comments.append(
                f'<{comment["date"]}> {comment["chat_name"]} ({comment["post_id"]}): {repr(comment["text"])}')

        old_comments = []
        for comment in sample_data['most_old_comments']:
            old_comments.append(
                f'<{comment["date"]}> {comment["chat_name"]} ({comment["post_id"]}): {repr(comment["text"])}')

        channels = []
        for c in sample_data['channels']:
            channels.append(f'{c["channel_name"]} - {c["count"]}')

        return TEMPLATE.format(
            user_id=user_id,
            usernames=', '.join(sample_data['user']['usernames']),
            names=', '.join(sample_data['user']['names']),
            recent_comments='\n'.join(recent_comments),
            old_comments='\n'.join(old_comments),
            channels='\n'.join(channels),
        )

    except:
        return None


async def input_loop():
    while True:
        print('\n' * 10)
        question = await queue.get()
        res = (await aioconsole.ainput(
            question + '\nUSER - 0, KREMLIN_BOT - 1, TROLL_BOT - 2, KADYROV_BOT - 3, SKIP - ANY\n')).strip()
        if res == '3':
            data.append({
                'question': question,
                'answer': 'KADYROV_BOT'
            })
        elif res == '2':
            data.append({
                'question': question,
                'answer': 'TROLL_BOT'
            })
        elif res == '1':
            data.append({
                'question': question,
                'answer': 'KREMLIN_BOT'
            })
        elif res == '0':
            data.append({
                'question': question,
                'answer': 'USER'
            })
        else:
            continue

        with open('output.json', 'w') as _fd:
            json.dump(data, _fd, ensure_ascii=False)

        with open('output.json1', 'w') as _fd:
            for x in data:
                messages = {
                    'messages': [
                        {
                            'role': 'system',
                            'content': HEADER + x['question']
                        },
                        {
                            'role': 'system',
                            'content': x['answer']
                        }
                    ]
                }
                _fd.write(json.dumps(messages, ensure_ascii=False) + '\n')


async def sampler():
    while True:
        async with httpx.AsyncClient() as client:
            ids = (await client.get('http://localhost:8002/random_users')).json()

        while ids:
            sub_ids = []
            for _ in range(5):
                sub_ids.append(ids.pop())

            samples = await asyncio.gather(*(get_samples(user_id) for user_id in sub_ids))

            for sample in samples:
                if sample:
                    await queue.put(sample)


async def main():
    await asyncio.gather(input_loop(), sampler())


asyncio.run(main())
