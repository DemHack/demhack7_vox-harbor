CREATE TABLE chats
(
    id Int64,
    name String,
    join_string String,
    bot_index UInt8,
    shard UInt8,
    type Enum('CHAT' = 1, 'CHANNEL' = 2, 'PRIVATE' = 3),
    added DateTime DEFAULT now()
)
ENGINE = SharedReplacingMergeTree()
ORDER BY id
