CREATE TABLE broken_bots
(
    time DateTime MATERIALIZED now(),
    id UInt32
)
ENGINE = SharedMergeTree()
ORDER BY time
