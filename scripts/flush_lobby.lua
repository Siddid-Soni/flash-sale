-- Move a random batch of lobby tokens into the queue, atomically.
-- KEYS: 1 lobby:{p}  2 lobby_order:{p}  3 lobby_published:{p}  4 lobby_flushed:{p}
-- ARGV: 1 batch size  2 random seed
-- Returns {'more', moved} or {'done', 0}.
--
-- SPOP picks which members to pop at random but returns them in the set's
-- internal order (insertion order for small sets), so the batch is shuffled
-- here. A random batch shuffled uniformly gives a uniformly random order.
-- `lobby_order` keeps it for the caller to publish to Kafka;
-- `lobby_published` counts how much of it is published.

local ids = redis.call('SPOP', KEYS[1], ARGV[1])
math.randomseed(tonumber(ARGV[2]))
for i = #ids, 2, -1 do
  local j = math.random(i)
  ids[i], ids[j] = ids[j], ids[i]
end
for _, id in ipairs(ids) do
  local token = 'token:' .. id
  -- EXISTS guards against recreating a token that was deleted
  if redis.call('EXISTS', token) == 1 then
    -- `published = lobby`: the admission worker publishes it (in shuffled
    -- order). Joins and status polls republish QUEUED tokens that were never
    -- published, and must leave these alone, or thousands of polls during
    -- the flush would publish duplicates out of order.
    redis.call('HSET', token, 'status', 'QUEUED', 'published', 'lobby')
    redis.call('RPUSH', KEYS[2], id)
  end
end

-- Done only when the lobby is empty and everyone moved out of it is in Kafka.
-- Joins check `lobby_flushed` in their own atomic call, so none can land in
-- the lobby after this empty SPOP and before the flag is set.
if #ids == 0 and redis.call('LLEN', KEYS[2]) == tonumber(redis.call('GET', KEYS[3]) or '0') then
  redis.call('SET', KEYS[4], 1)
  return {'done', 0}
end
return {'more', #ids}
