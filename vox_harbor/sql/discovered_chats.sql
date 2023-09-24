CREATE TABLE discovered_chats
(
    id Int64,
    name String,
    join_string String,
    subscribers_count UInt64,
    sign Int8
)
ENGINE = CollapsingMergeTree(sign)
ORDER BY id
