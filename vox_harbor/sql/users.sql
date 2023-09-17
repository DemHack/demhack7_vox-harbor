CREATE TABLE users
(
    user_id Int64,
    username String,
    name String

)
ENGINE = SharedReplacingMergeTree()
ORDER BY (user_id, username, name);
