CREATE TABLE comments
(
    user_id Int64,
    date DATETIME,
    chat_id Int64,
    message_id Int64,

    channel_id Nullable(Int64),
    post_id Nullable(Int64),

    username Nullable(String),
    first_name Nullable(String),
    last_name Nullable(String),

    bot_index UInt8,
    shard UInt8
)
ENGINE = SharedReplacingMergeTree()
ORDER BY (user_id, date)
