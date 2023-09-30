CREATE TABLE check_results
(
    user_id Int64,
    date Datetime,
    type Enum('USER' = 1, 'KREMLIN_BOT' = 2, 'TROLL_BOT' = 3, 'KADYROV_BOT' = 4),
    manual_confirmed Bool DEFAULT false
)
ENGINE = SharedReplacingMergeTree()
ORDER BY user_id;
