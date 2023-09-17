import os


class Config:
    ACTIVE_BOTS_COUNT = int(os.getenv('ACTIVE_BOTS_COUNT', 3))
    SHARD_NUM = int(os.getenv('SHARD_NUM', 0))

    MAX_CHATS_FOR_BOT = int(os.getenv('MAX_CHATS_FOR_BOT', 200))
    MIN_CHAT_MEMBERS_COUNT = int(os.getenv('MIN_CHAT_MEMBERS_COUNT', 1000))

    CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST')
    CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', 9440))
    CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD')

    AUTO_DISCOVER = False
