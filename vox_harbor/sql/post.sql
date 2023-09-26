CREATE TABLE posts
(
    id Int64,
    channel_id Int64,
    post_date Datetime,
    point_date Datetime,

    data Nested (
        key LowCardinality(String),
        value Int64
    ),
    bot_index UInt8,
    shard UInt8
)
ENGINE = SharedMergeTree()
ORDER BY (channel_id, id, point_date)
