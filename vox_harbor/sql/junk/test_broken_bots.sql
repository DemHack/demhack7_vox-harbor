CREATE TABLE test_broken_bots (
    time DateTime MATERIALIZED NOW(),
    id UInt32
) ENGINE = SharedMergeTree()
ORDER BY time