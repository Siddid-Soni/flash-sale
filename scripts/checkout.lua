-- Reserve one unit of a sale for a user, atomically (roadmap step 3).
-- KEYS: 1 sale_start:{s}  2 idem:{s}:{user}:{key}  3 user_live:{s}:{user}
--       4 stock:{s}  5 hold:{s}:{ticket}  6 holds:pending:{s}
-- ARGV: 1 new ticket  2 user id  3 idempotency key  4 idempotency TTL secs
-- Returns {'ok', checkout_ms}              a hold was created for ARGV[1]
--         {'existing', ticket}             the idempotency key was seen: a retry
--         {'not_started', sale_start_ms}   before the opening time
--         {'not_found'}                    no sale_start: the sale is unknown
--         {'already'}                      the user already holds or bought a unit
--         {'sold_out'}
--
-- The order of the checks matters. The idempotency check comes before the
-- one-per-user check, or a retry (same user, same key) would be told 409
-- instead of getting its ticket back. It comes after the start check so that
-- a key can never be bound before the sale opens.
-- Touches only inventory keys: the read-cache entry status:<ticket> is
-- written by Rust afterwards.

local start = redis.call('GET', KEYS[1])
if not start then
  return {'not_found'}
end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local now_str = string.format('%d', now)
if now < tonumber(start) then
  return {'not_started', start}
end

local existing = redis.call('GET', KEYS[2])
if existing then
  return {'existing', existing}
end

if redis.call('EXISTS', KEYS[3]) == 1 then
  return {'already'}
end

local stock = tonumber(redis.call('GET', KEYS[4]) or '0')
if stock <= 0 then
  return {'sold_out'}
end
redis.call('DECR', KEYS[4])

redis.call('HSET', KEYS[5], 'state', 'PENDING', 'user', ARGV[2], 'checkout_ms', now_str, 'idem', ARGV[3])
redis.call('ZADD', KEYS[6], now_str, ARGV[1])
redis.call('SET', KEYS[3], ARGV[1])
redis.call('SET', KEYS[2], ARGV[1], 'EX', ARGV[4])
return {'ok', now_str}
