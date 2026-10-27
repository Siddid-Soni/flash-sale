-- Admit one queued token if the product has room for another shopper, atomically.
-- KEYS: 1 token:{id}  2 active_tokens:{p}  3 stock:{p}  4 admitted_upto:{p}
--       5 closed:{p}
-- ARGV: 1 token id  2 seq (the record's offset + 1)  3 max active tokens
--       4 token ttl ms
-- Returns admitted | skipped | full | closed.

-- Checked here, not only by the worker, so nobody is admitted after a close
if redis.call('EXISTS', KEYS[5]) == 1 then
  return 'closed'
end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

-- Tokens that expired without being used free their slots
redis.call('ZREMRANGEBYSCORE', KEYS[2], '-inf', now)

-- Only moves forward: a replayed message can carry an older offset
local function advance()
  local seq = tonumber(ARGV[2])
  if seq > tonumber(redis.call('GET', KEYS[4]) or '0') then
    redis.call('SET', KEYS[4], seq)
  end
end

-- Expired, or already admitted (delivery is at-least-once, so a message can
-- come back after a crash and must not re-activate a used token). Advancing
-- past it keeps the positions of the tokens behind it accurate.
if redis.call('HGET', KEYS[1], 'status') ~= 'QUEUED' then
  advance()
  return 'skipped'
end

-- No more shoppers than units in stock (not held or sold): with 0 units
-- nobody is admitted
local limit = math.min(tonumber(ARGV[3]), tonumber(redis.call('GET', KEYS[3]) or '0'))
if redis.call('ZCARD', KEYS[2]) >= limit then
  return 'full'
end

local ttl = tonumber(ARGV[4])
redis.call('HSET', KEYS[1], 'status', 'ACTIVE')
redis.call('PEXPIRE', KEYS[1], ttl)
redis.call('ZADD', KEYS[2], now + ttl, ARGV[1])
advance()
return 'admitted'
