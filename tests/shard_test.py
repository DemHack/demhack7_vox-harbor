import pytest

from tests.fixtures import services
from tests.testing_data import COMMENTS, MESSAGES, USER, USER_INFO
from tests.utils import is_sub_iterable
from vox_harbor.big_bot.bots import BotManager
from vox_harbor.big_bot.configs import Config
from vox_harbor.big_bot.services.shard import get_messages

# todo use config
shard_api_url = 'http://127.0.0.1:8001' + '/api/shard'
controller_api_url = 'http://127.0.0.1:8002' + '/api/controller'
