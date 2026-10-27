-- Move a fake provider session from one state to another.
-- KEYS: 1 fakepay:session:<id>
-- ARGV: 1 from state  2 to state
-- Returns 'moved' (it was in `from`), 'already' (it is already in `to`),
-- 'missing' (no such session) or the current state if it is in neither.
-- Used for expire (OPEN -> EXPIRED) and refund (PAID -> REFUNDED). The payment
-- page's OPEN -> PAID and OPEN -> CANCELED go through fakepay_settle.lua, which
-- queues the webhook event in the same step.

local state = redis.call('HGET', KEYS[1], 'state')
if not state then
  return 'missing'
end
if state == ARGV[1] then
  redis.call('HSET', KEYS[1], 'state', ARGV[2])
  return 'moved'
end
if state == ARGV[2] then
  return 'already'
end
return state
