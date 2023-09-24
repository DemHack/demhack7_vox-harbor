CREATE TABLE logs
(
    created Datetime64,
    filename String,
    func_name String,
    levelno UInt8,
    lineno UInt32,
    message String,
    name String,
    shard UInt8,
    fqdn String
)
ENGINE = SharedMergeTree()
ORDER BY created
TTL toDateTime(created) + INTERVAL 30 DAY
