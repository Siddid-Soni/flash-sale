-- Claim a hold for a successful payment, atomically. Phase 1 of 3 (claim,
-- Postgres commit, finalize). Run as soon as the success event is received,
-- and again by the payment worker; the second call finds its own claim.
-- Retry-safe: the winner is recorded in the hold (`paid_by`) instead of being
-- implied by a deleted key, so a worker that crashed before its commit claims
-- the same hold again on retry.
-- KEYS: 1 resv:{id}  2 holds:expiry
-- ARGV: 1 reservation id  2 payment intent id  3 claimed hold TTL ms
-- Returns {'won', resv_json} or {'gone'} (expired, or claimed by another
-- payment): refund.

local raw = redis.call('GET', KEYS[1])
if not raw then
  return {'gone'}
end
local resv = cjson.decode(raw)
if resv.paid_by then
  if resv.paid_by == ARGV[2] then
    return {'won', raw}
  end
  return {'gone'}
end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local expires_at = tonumber(redis.call('ZSCORE', KEYS[2], ARGV[1]))
-- Past due counts as expired even before the cleanup worker releases it, so
-- payment and expiry never both win
if not expires_at or expires_at <= now then
  return {'gone'}
end

resv.paid_by = ARGV[2]
resv.state = 'paid'
raw = cjson.encode(resv)
-- The hold now waits only for the Postgres commit, however far behind the
-- payment worker is: keep it well past its own expiry (finalize deletes it)
redis.call('SET', KEYS[1], raw, 'PX', tonumber(ARGV[3]))
-- Expiry can no longer take it
redis.call('ZREM', KEYS[2], ARGV[1])
return {'won', raw}
