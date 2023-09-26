import json
from pathlib import Path
from pprint import pprint

from vox_harbor.big_bot.structures import Comment, Message, User, UserInfo


def load(file_name: str):
    return json.loads((Path(__file__).parent / 'testing_data' / f'{file_name}.json').read_text())


USER = User(**load('user'))

# controller/user/?user_id=401389749
USER_INFO: UserInfo = UserInfo(**load('user_info'))

# controller/comments?user_id=401389749
COMMENTS: list[Comment] = [Comment(**c) for c in load('comments')]

MESSAGES: list[Message] = [Message(**m) for m in load('messages')]
