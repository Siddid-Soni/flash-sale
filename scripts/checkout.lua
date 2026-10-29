-- Reserve one unit of a sale for a user, atomically.
-- KEYS: 1 sale_start:{s}  2 idem:{s}:{user}:{key}  3 user_live:{s}:{user}
--       4 stock:{s}
-- ARGV: 1 new ticket  2 one day in seconds (idempotency TTL, and the least
--       TTL of user_live)
-- Returns {'ok', checkout_ms}              the unit is reserved for ARGV[1]
--         {'existing', ticket}             the idempotency key was seen: a retry
--         {'not_started', sale_start_ms}   before the opening time
--         {'not_found'}                    no sale_start: the sale is unknown
--         {'already'}                      the user already holds or bought a unit
--         {'sold_out'}
--
-- Redis gates admission only: it keeps no record of the ticket beyond
-- user_live and the idempotency key. The queue is Kafka, the outcome is
-- PostgreSQL's.
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

-- user_live outlives the sale by a day, so Redis cleans up a confirmed buyer's
-- marker (or one left by a lost unit) on its own: the seconds until the sale
-- starts plus one day, at least one day
local day = tonumber(ARGV[2])
local ttl = day + math.max(math.ceil((tonumber(start) - now) / 1000), 0)
redis.call('SET', KEYS[3], ARGV[1], 'EX', ttl)
redis.call('SET', KEYS[2], ARGV[1], 'EX', ARGV[2])
return {'ok', now_str}
