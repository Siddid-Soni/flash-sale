-- Close a due hold whose order PostgreSQL has already settled for good: the
-- order is CONFIRMED (paid, the unit is sold), or REFUNDED after the
-- PostgreSQL guard found no unit (PostgreSQL has none to sell). The
-- expiration worker's unpaid loop runs this for such a hold. Inventory keys
-- only.
-- KEYS: 1 hold:{s}:{ticket}  2 holds:expiry:{s}
-- ARGV: 1 ticket
-- Returns 'settled' or 'not_settled'.
--
-- Like release.lua it is decided by ZREM on holds:expiry: whoever removes the
-- hold first acts, so several workers racing on one hold act once. The hold
-- has done its job (it only detects a lost Kafka message and times the
-- payment deadline), so it is deleted. Nothing else changes: no counter (sales
-- are counted only in PostgreSQL, products.sold), no stock returned (the unit
-- is sold, or PostgreSQL has none), and user_live stays (the user bought one).

if redis.call('ZREM', KEYS[2], ARGV[1]) == 0 then
  return 'not_settled'
end
redis.call('DEL', KEYS[1])
return 'settled'
