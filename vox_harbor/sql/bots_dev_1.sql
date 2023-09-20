CREATE TABLE bots_dev_1
(
    id UInt32,
    name String,
    shard UInt8,
    session_string String
)
ENGINE = SharedReplacingMergeTree()
ORDER BY (shard, id)
