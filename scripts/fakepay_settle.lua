-- The fake provider's payment page settles a session and queues the webhook
-- for it, in one step (so a crash can never leave a paid session without its
-- event). Provider namespace only.
-- KEYS: 1 fakepay:session:<id>  2 fakepay:outbox  3 fakepay:event:<event id>
-- ARGV: 1 from state (OPEN)  2 to state (PAID or CANCELED)  3 event id
--       4 event body (JSON)  5 now (epoch ms)  6 TTL secs of the event record
-- Returns 'moved' (it was OPEN: now settled, the event is queued),
-- 'already' (it is already in `to`: nothing is queued again), 'missing', or
-- the current state if it is in neither (EXPIRED, REFUNDED, the other outcome).
--
-- The provider does not look at the merchant's hold: a payment after the
-- hold expired but before the session was expired is accepted, and the
-- backend refunds it. The event id is deterministic per session and type, and
-- an existing event record is never overwritten, so nothing queues twice.

local state = redis.call('HGET', KEYS[1], 'state')
if not state then
  return 'missing'
end
if state == ARGV[1] then
  redis.call('HSET', KEYS[1], 'state', ARGV[2])
  if redis.call('EXISTS', KEYS[3]) == 0 then
    redis.call('HSET', KEYS[3], 'body', ARGV[4], 'attempts', 0, 'created_ms', ARGV[5])
    redis.call('EXPIRE', KEYS[3], ARGV[6])
    redis.call('ZADD', KEYS[2], 'NX', ARGV[5], ARGV[3])
  end
  return 'moved'
end
if state == ARGV[2] then
  return 'already'
end
return state
