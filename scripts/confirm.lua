-- Payment against a hold (roadmap steps 7-8, 20-21): claim it when the payment
-- arrives, count it once the order is committed. Inventory keys only.
-- KEYS: 1 hold:{s}:{ticket}  2 holds:expiry:{s}  3 sold:{s}
-- ARGV: 1 mode ('claim', 'finalize' or 'refunded')  2 ticket
--       3 payment session id  4 payment id (claim only)
--       5 seconds the hold stays readable afterwards (finalize, refunded)
--
-- claim: returns 'won' or 'gone'.
--   * PAID with this payment session: 'won' (a retry finds its own claim; the
--     claim is recorded in the hold, not implied by a missing key).
--   * AWAITING_PAYMENT with this session, its score in holds:expiry still in
--     the future (Redis TIME) and ZREM removes it: the hold becomes PAID,
--     `paid_by` the payment id, with no TTL: 'won'. Past due counts as expired
--     even before the expiration worker has released it, and release.lua only
--     releases a score that is due, so a payment and an expiry never both win.
--   * anything else (released, another session, PENDING): 'gone', refund.
--
-- finalize: run after the PostgreSQL commit. If the hold is PAID by this
-- session: INCR sold, state CONFIRMED, readable for the status fallback for
-- the TTL; returns 'finalized'. Otherwise 'noop' (already finalized, or not
-- ours). Running it twice counts once, because the state moves in the same
-- step as the counter. `user_live` stays: the user bought one.
--
-- refunded: the payment was refunded after winning the claim (the PostgreSQL
-- guard refused it). If the hold is PAID by this session: state REFUNDED,
-- readable for the TTL, no count; returns 'closed'. Otherwise 'noop'.

local mode, ticket, session = ARGV[1], ARGV[2], ARGV[3]
local h = redis.call('HMGET', KEYS[1], 'state', 'payment_session')
local state = h[1]

if mode == 'claim' then
  if state == 'PAID' then
    if h[2] == session then
      return 'won'
    end
    return 'gone'
  end
  if state ~= 'AWAITING_PAYMENT' or h[2] ~= session then
    return 'gone'
  end
  local score = redis.call('ZSCORE', KEYS[2], ticket)
  if not score then
    return 'gone'
  end
  local t = redis.call('TIME')
  local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
  if tonumber(score) <= now then
    return 'gone'
  end
  if redis.call('ZREM', KEYS[2], ticket) == 0 then
    return 'gone'
  end
  redis.call('HSET', KEYS[1], 'state', 'PAID', 'paid_by', ARGV[4])
  redis.call('PERSIST', KEYS[1])
  return 'won'
end

if mode == 'finalize' then
  if state == 'PAID' and h[2] == session then
    redis.call('INCR', KEYS[3])
    redis.call('HSET', KEYS[1], 'state', 'CONFIRMED')
    redis.call('EXPIRE', KEYS[1], ARGV[5])
    return 'finalized'
  end
  return 'noop'
end

if mode == 'refunded' then
  if state == 'PAID' and h[2] == session then
    redis.call('HSET', KEYS[1], 'state', 'REFUNDED')
    redis.call('EXPIRE', KEYS[1], ARGV[5])
    return 'closed'
  end
  return 'noop'
end

return redis.error_reply('unknown mode ' .. tostring(mode))
