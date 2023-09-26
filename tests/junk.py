# async def fetch_data_from_api(url: str):
#     async with httpx.AsyncClient() as client:
#         return await client.get(url)


# @pytest.mark.asyncio
# async def test_1():
#     response = await fetch_data_from_api(controller_api_url + '/comments?user_id=6389870200')
#     print(response.json())
#     assert response.status_code == 200

#     # expected_data = '{"userId": 1, "id": 1, "title": "example title", "body": "example body"}'
#     # response_data = await fetch_data_from_api(url)
#     # assert response_data == expected_data


# -------------------------------------------------


# async def f():
#     asyncio.create_task(root_main())
#     await asyncio.sleep(17)

#     bots = (await BotManager.get_instance()).bots

#     for b in bots:
#         await b.send_message(USER.user_id, 'hi')
#         pprint(b)


# -------------------------------------------------


# insert my comments and users
# handlers.py
# async def process_message(bot: Bot, message: types.Message):
#     logger.critical('lol')
#     from config20 import config
#     if message.chat.username != config.TEST_USERNAME:
#         return

#     logger.critical('kek')

#     bots = await BotManager.get_instance()

#     logger.critical('message: %s', vars(message))

#     channel_id = None
#     post_id = None
#     await inserter.insert(message, bot.index, channel_id, post_id)
#     await inserter.flush()
#     logger.critical('flusheddddddddddddddddddddddddd')
#     return
