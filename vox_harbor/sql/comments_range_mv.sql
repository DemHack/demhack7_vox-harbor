CREATE MATERIALIZED VIEW comments_range_mv
(
    chat_id Int64,
    min_message_id SimpleAggregateFunction(min, Int64),
    max_message_id SimpleAggregateFunction(max, Int64)
)
ENGINE = AggregatingMergeTree()
ORDER BY chat_id
AS SELECT
    chat_id,
    min(message_id) AS min_message_id,
    max(message_id) AS max_message_id
FROM comments
GROUP BY chat_id;
