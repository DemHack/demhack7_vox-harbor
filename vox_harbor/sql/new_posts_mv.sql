CREATE MATERIALIZED VIEW new_posts_mv
(
    id Int64,
    channel_id Int64,
    post_date Datetime,

    bot_index UInt8,
    shard UInt8
)
ENGINE = ReplacingMergeTree()
ORDER BY (post_date, channel_id, id)
AS SELECT
       id,
       channel_id,
       post_date,
       bot_index,
       shard
FROM posts
GROUP BY (post_date, channel_id, id, bot_index, shard)
