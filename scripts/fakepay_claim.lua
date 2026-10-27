-- The fake provider's webhook dispatcher takes the events that are due, with
-- a lease so several Order Service replicas can run a dispatcher.
-- KEYS: 1 fakepay:outbox (sorted set: event id, score = next attempt, ms)
-- ARGV: 1 now (ms)  2 lease end (ms)  3 most events to take
-- Returns the event ids.
--
-- Taking an event moves its score to the end of the lease. The dispatcher
-- that took it then sets the real next attempt, or removes it; if it dies
-- first, the event is due again when the lease ends (at-least-once delivery;
-- the receiver stores each event id once). ZADD XX only moves members that
-- exist, so an event removed meanwhile does not come back.

local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[3])
for _, id in ipairs(ids) do
  redis.call('ZADD', KEYS[1], 'XX', ARGV[2], id)
end
return ids
