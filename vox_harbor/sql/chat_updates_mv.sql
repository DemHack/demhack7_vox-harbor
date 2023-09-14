CREATE MATERIALIZED VIEW chat_updates_mv
TO chat_updates
AS
SELECT
    shard,
    bot_index,
    added
FROM chats
