from fastapi import APIRouter

from vox_harbor.big_bot.structures import EmptyResponse, Message
from vox_harbor.big_bot.bots import BotManager

models_router = APIRouter()


@models_router.post('/discover')
async def discover_chat(join_string: str, bot_index: int | None = None) -> EmptyResponse:
    bots = await BotManager.get_instance()
    if bot_index is not None:
        bots = bots.bots[bot_index]

    await bots.discover_chat(join_string)
    return EmptyResponse()


@models_router.get('/message')
async def get_message(bot_index: int, chat_id: int, message_id: int, ) -> Message:
    bots = await BotManager.get_instance()
    message = await bots.get_message(bot_index, chat_id, message_id)

    return Message(text=message.text)
