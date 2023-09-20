--* Misc
SELECT *
FROM broken_bots
LIMIT 10;

SELECT COUNT(*) users
FROM users;

--* Controller
-- User by username
SELECT user_id,
    username,
    name
FROM users
WHERE username = 'pufit'
LIMIT 1;

-- User by user_id
SELECT bot_index,
    chat_id,
    message_id,
    shard
FROM comments
WHERE user_id = 241309761 -- pufit
ORDER BY date;

-- Messages
SELECT bot_index,
    chat_id,
    message_id,
    shard
FROM comments
WHERE user_id = 241309761 -- pufit
ORDER BY date;

-- Messages
SELECT bot_index,
    chat_id,
    message_id,
    shard
FROM comments
WHERE user_id = 6389870200 -- Влад Сотников (кремлебот)
ORDER BY date;