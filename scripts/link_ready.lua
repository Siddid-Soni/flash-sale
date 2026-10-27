-- The payment link is ready: move a hold from PENDING to AWAITING_PAYMENT
-- (roadmap step 17). Run by the order processor.
-- KEYS: 1 hold:{s}:{ticket}  2 holds:pending:{s}  3 holds:expiry:{s}
-- ARGV: 1 ticket  2 payment session id  3 expires_at (epoch ms: the payment
--       window ends then; this becomes the hold's score in holds:expiry)
-- Returns 'ready'    the hold was PENDING and is now AWAITING_PAYMENT
--         'already'  the hold is already AWAITING_PAYMENT (or PAID) with this
--                    payment session: a redelivered message, success
--         'gone'     anything else: released, expired, or another session
--
-- Like release.lua it is decided by ZREM on the sorted set: whoever removes
-- the hold from holds:pending first wins, so a link and a release racing on
-- one hold have exactly one winner (a released hold is never linked, and a
-- linked hold is never released as PENDING). Touches only inventory keys.

local state = redis.call('HGET', KEYS[1], 'state')

if state == 'PENDING' then
  if redis.call('ZREM', KEYS[2], ARGV[1]) == 1 then
    redis.call('HSET', KEYS[1], 'state', 'AWAITING_PAYMENT', 'payment_session', ARGV[2])
    redis.call('ZADD', KEYS[3], ARGV[3], ARGV[1])
    return 'ready'
  end
  return 'gone'
end

if (state == 'AWAITING_PAYMENT' or state == 'PAID')
    and redis.call('HGET', KEYS[1], 'payment_session') == ARGV[2] then
  return 'already'
end
return 'gone'
