-- Release holds that are past due, atomically, up to a batch. Safe with any
-- number of cleanup workers: each hold leaves `holds:expiry` in the same call
-- that releases it, so exactly one worker gets it.
-- KEYS: 1 holds:expiry
-- ARGV: 1 batch size
-- Returns {scanned, id1, state1, id2, state2, ...}: every released hold with
-- its state (`paying` ones have an order to mark EXPIRED). `scanned` equal
-- to the batch size means more may be due.

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))

local result = {tostring(#ids)}
for _, id in ipairs(ids) do
  redis.call('ZREM', KEYS[1], id)
  local key = 'resv:' .. id
  local raw = redis.call('GET', key)
  if raw then
    local resv = cjson.decode(raw)
    -- A hold claimed by a payment has left the queue already; never release it
    if not resv.paid_by then
      redis.call('DEL', key)
      local p = tostring(resv.product)
      local n = resv.quantity
      redis.call('INCRBY', 'stock:' .. p, n)
      redis.call('DECRBY', 'user_units:' .. p .. ':' .. tostring(resv.user), n)
      -- The token stays USED: the turn was spent. With the hold gone the
      -- user may join the queue again.
      result[#result + 1] = id
      result[#result + 1] = resv.state or 'held'
    end
  end
end
return result
