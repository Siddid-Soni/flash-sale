-- Settle a paid hold in Redis once PostgreSQL has decided its order (the
-- expiration worker's unpaid loop runs this for a due hold whose order is not
-- EXPIRED). Inventory keys only.
-- KEYS: 1 hold:{s}:{ticket}  2 holds:expiry:{s}  3 sold:{s}
-- ARGV: 1 ticket  2 mode ('confirmed' or 'dropped')
--       3 seconds the hold stays readable afterwards
-- Returns 'settled' or 'not_settled'.
--
-- Like release.lua it is decided by ZREM on holds:expiry: whoever removes the
-- hold first acts, so several workers racing on one hold act once. No stock is
-- returned in either mode, because the unit is not free:
--   * confirmed: the order is CONFIRMED in PostgreSQL, the unit is sold. INCR
--     sold:{s} (the only place it is counted) and the hold becomes CONFIRMED.
--     The ZREM and the INCR are one step, so sold:{s} counts each hold once.
--     user_live stays: the user bought one.
--   * dropped: the payment was refunded after the PostgreSQL guard refused it
--     (Redis had reserved a unit PostgreSQL does not have). Nothing is counted;
--     the hold becomes REFUNDED, kept only for the status fallback.

local mode = ARGV[2]
if mode ~= 'confirmed' and mode ~= 'dropped' then
  return redis.error_reply('unknown mode ' .. tostring(mode))
end

if redis.call('HGET', KEYS[1], 'state') ~= 'AWAITING_PAYMENT' then
  return 'not_settled'
end
if redis.call('ZREM', KEYS[2], ARGV[1]) == 0 then
  return 'not_settled'
end

if mode == 'confirmed' then
  redis.call('INCR', KEYS[3])
  redis.call('HSET', KEYS[1], 'state', 'CONFIRMED')
else
  redis.call('HSET', KEYS[1], 'state', 'REFUNDED')
end
redis.call('EXPIRE', KEYS[1], ARGV[3])
return 'settled'
