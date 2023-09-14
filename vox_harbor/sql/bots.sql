CREATE TABLE bots
(
    id UInt32,
    name String,
    shard UInt8,
    session_string String
)
ENGINE = SharedMergeTree()
ORDER BY (shard, id)
