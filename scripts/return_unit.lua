-- Give one unit back to stock and free its user, for a ticket that will never
-- be paid. Run by the Order Service when a publish failed (and it created the
-- FAILED order) and by the expiration worker for each order it expired.
-- KEYS: 1 stock:{s}  2 user_live:{s}:{user}
--       3 idem:{s}:{user}:{key}  (optional: compensation only)
-- ARGV: 1 ticket
-- Returns 1.
--
-- Not idempotent, on purpose: whoever calls it has just won a PostgreSQL
-- decision that happens once per ticket (the FAILED insert, the EXPIRED
-- update), and calls it once, without retry (docs/FAILURE_CASES.md). The
-- user's live-ticket marker and the idempotency key are deleted only if they
-- still name this ticket.

redis.call('INCR', KEYS[1])
if redis.call('GET', KEYS[2]) == ARGV[1] then
  redis.call('DEL', KEYS[2])
end
if KEYS[3] and redis.call('GET', KEYS[3]) == ARGV[1] then
  redis.call('DEL', KEYS[3])
end
return 1
