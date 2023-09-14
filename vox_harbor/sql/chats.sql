CREATE TABLE chats
(
    id Int64,
    name String,
    join_string String,
    bot_index UInt8,
    shard UInt8,
    added DateTime DEFAULT now()
)
ENGINE = SharedReplacingMergeTree()
ORDER BY id
