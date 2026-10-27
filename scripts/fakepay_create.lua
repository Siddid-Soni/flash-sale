-- The fake payment provider's own "database": create a checkout session once
-- per ticket.
-- KEYS: 1 fakepay:by_ticket:<ticket>  2 fakepay:session:<id>
-- ARGV: 1 new session id  2 ticket  3 amount_cents  4 expires_at (epoch ms)
--       5 TTL secs
-- Returns {'created', id} or {'exists', id of the session the ticket already has}.
--
-- Idempotent by ticket, as the provider's idempotency key is: a redelivered
-- message finds its session and no second one is made, whatever state the
-- first one is in. Both keys expire together, long after the payment window,
-- so a late payment can still be refunded.

local existing = redis.call('GET', KEYS[1])
if existing then
  return {'exists', existing}
end
redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[5])
redis.call('HSET', KEYS[2], 'ticket', ARGV[2], 'amount_cents', ARGV[3], 'expires_at', ARGV[4], 'state', 'OPEN')
redis.call('EXPIRE', KEYS[2], ARGV[5])
return {'created', ARGV[1]}
