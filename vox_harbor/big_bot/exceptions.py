

class BigBotException(Exception):
    pass


class AlreadyJoinedError(BigBotException):
    pass


class MessageNotFoundError(BigBotException):
    pass
