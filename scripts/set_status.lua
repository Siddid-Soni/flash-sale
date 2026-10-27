-- Forward-only write of the read-cache entry for a ticket (runs on the read
-- cache, which holds only status:<ticket>).
-- KEYS: 1 status:<ticket>
-- ARGV: 1 status  2 user id  3 TTL secs  4.. field, value, field, value ...
--       (optional fields: payment_url, expires_at, partition, offset, sale)
-- Returns 'written' or 'ignored'. An unknown status is an error.
--
-- Ranks: PENDING 1 < AWAITING_PAYMENT 2 < the final states 3 (CONFIRMED,
-- EXPIRED, FAILED, REFUNDED). A write of a lower rank is ignored, so a
-- delayed write never overwrites a later state. The first final state wins:
-- nothing overwrites one, not even another final state. Same-rank writes of
-- PENDING or AWAITING_PAYMENT are accepted (they refresh the fields).
-- The user is stored so the status endpoint can refuse someone else's ticket.

local rank = { PENDING = 1, AWAITING_PAYMENT = 2, CONFIRMED = 3, EXPIRED = 3, FAILED = 3, REFUNDED = 3 }
local new = rank[ARGV[1]]
if not new then
  return redis.error_reply('unknown status ' .. ARGV[1])
end

local current = redis.call('HGET', KEYS[1], 'status')
if current then
  local cur = rank[current] or 0
  if cur == 3 or cur > new then
    return 'ignored'
  end
end

redis.call('HSET', KEYS[1], 'status', ARGV[1], 'user', ARGV[2])
for i = 4, #ARGV, 2 do
  redis.call('HSET', KEYS[1], ARGV[i], ARGV[i + 1])
end
redis.call('EXPIRE', KEYS[1], ARGV[3])
return 'written'
