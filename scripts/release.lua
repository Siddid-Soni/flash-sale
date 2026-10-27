-- Release a hold and return its unit to stock, atomically. One script for
-- immediate compensation (publish failed) and for both expiry loops.
-- KEYS: 1 hold:{s}:{ticket}  2 the sorted set of the expected state
--       (holds:pending:{s} or holds:expiry:{s})  3 stock:{s}
-- ARGV: 1 ticket  2 sale id  3 expected state (PENDING or AWAITING_PAYMENT)
--       4 max score: a number (ms), 'now' (Redis TIME, ms) or '+inf'
--       5 final state to record (FAILED or EXPIRED)
--       6 '1' to delete the hold's idempotency key (compensation only), else '0'
--       7 seconds the released hold stays readable (the status fallback uses it)
-- Returns 'released' or 'not_released'.
--
-- Released only if the hold is in the expected state, is a member of the
-- sorted set with a score <= the max score, and ZREM removes it: whoever
-- removes it first wins, so callers racing on one hold return the unit
-- exactly once. The user's live-ticket marker is cleared only if it still
-- names this ticket. KEYS 4 and 5 are built here (user_live and idem) from
-- the hold's own fields; they share the {sale} hash tag with KEYS 1-3.

local ticket, sale, expected, final = ARGV[1], ARGV[2], ARGV[3], ARGV[5]

local h = redis.call('HMGET', KEYS[1], 'state', 'user', 'idem')
if h[1] ~= expected then
  return 'not_released'
end

local score = redis.call('ZSCORE', KEYS[2], ticket)
if not score then
  return 'not_released'
end
if ARGV[4] ~= '+inf' then
  local max
  if ARGV[4] == 'now' then
    local t = redis.call('TIME')
    max = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
  else
    max = tonumber(ARGV[4])
  end
  if tonumber(score) > max then
    return 'not_released'
  end
end

if redis.call('ZREM', KEYS[2], ticket) == 0 then
  return 'not_released'
end

redis.call('INCR', KEYS[3])

local live = 'user_live:{' .. sale .. '}:' .. h[2]
if redis.call('GET', live) == ticket then
  redis.call('DEL', live)
end
if ARGV[6] == '1' and h[3] then
  local idem = 'idem:{' .. sale .. '}:' .. h[2] .. ':' .. h[3]
  if redis.call('GET', idem) == ticket then
    redis.call('DEL', idem)
  end
end

redis.call('HSET', KEYS[1], 'state', final)
redis.call('EXPIRE', KEYS[1], ARGV[7])
return 'released'
