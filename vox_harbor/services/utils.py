from typing import NoReturn
from urllib.parse import ParseResult, parse_qs, urlparse

from vox_harbor.big_bot.structures import ParsedMsgURL


def parse_msg_url(url: str) -> ParsedMsgURL:
    def error(err: str) -> NoReturn:
        raise ValueError(f'Invalid {url = }. {err.capitalize()}')

    parsed_url: ParseResult = urlparse(url)

    if not parsed_url.scheme:
        error('scheme must be provided')
    if parsed_url.netloc != 't.me':
        error('netloc must be t.me')

    chat_id, msg_id = parsed_url.path.rstrip('/').split('/')[-2:]
    if comment := parse_qs(parsed_url.query, strict_parsing=True).get('comment', None):
        msg_id: str = comment[0]

    try:
        chat_id = int(chat_id)
    except ValueError:
        pass

    return ParsedMsgURL(chat_id=chat_id, message_id=int(msg_id))
