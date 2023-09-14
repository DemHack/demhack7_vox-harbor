CREATE TABLE chat_updates
(
    shard UInt8,
    bot_index UInt8,
    added DateTime DEFAULT now()
)
ENGINE = SharedMergeTree()
ORDER BY (shard, bot_index, added)
