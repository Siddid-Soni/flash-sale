-- Start paying for a hold, atomically: extend it once and mark it `paying`,
-- after which it can no longer be released by the user.
-- KEYS: 1 resv:{id}  2 holds:expiry
-- ARGV: 1 reservation id  2 user id  3 extension ms  4 grace ms
-- Returns {'paying', resv_json, expires_at_ms} (also for a retried pay, with
-- the hold unchanged), {'gone'}, {'forbidden'} or {'paid'}.

local raw = redis.call('GET', KEYS[1])
if not raw then
  return {'gone'}
end
local resv = cjson.decode(raw)
if tostring(resv.user) ~= ARGV[2] then
  return {'forbidden'}
end
local state = resv.state or 'held'
if state == 'paid' then
  return {'paid'}
end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local expires_at = tonumber(redis.call('ZSCORE', KEYS[2], ARGV[1]))
-- Past due: the cleanup worker has not released it yet, but it is over
if not expires_at or expires_at <= now then
  return {'gone'}
end
if state == 'paying' then
  return {'paying', raw, tostring(expires_at)}
end

-- Added to the hold's own expiry: counting from now could shorten it
expires_at = expires_at + tonumber(ARGV[3])
redis.call('ZADD', KEYS[2], 'XX', expires_at, ARGV[1])
resv.state = 'paying'
raw = cjson.encode(resv)
redis.call('SET', KEYS[1], raw, 'PX', expires_at - now + tonumber(ARGV[4]))
return {'paying', raw, tostring(expires_at)}
