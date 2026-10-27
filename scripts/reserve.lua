-- Hold units for an admitted user, all or nothing, atomically.
-- KEYS: 1 user_token:{p}:{u}  2 stock:{p}  3 user_units:{p}:{u}
--       4 holds:expiry  5 active_tokens:{p}  6 closed:{p}  7 stock_total:{p}
--       8 max_per_user:{p}
-- ARGV: 1 reservation id  2 user id  3 product id  4 hold ttl ms  5 grace ms
--       6 quantity
-- Returns {'ok', resv_id, expires_at_ms},
-- {'existing', resv_id, expires_at_ms, quantity},
-- {'closed'}, {'not_admitted'}, {'turn_used'}, {'invalid_quantity', limit},
-- {'too_many', units_held} or {'out_of_stock', units_left}.
--
-- A refusal leaves an ACTIVE token ACTIVE, so a user who loses the last units
-- to a faster buyer can ask for fewer, or try again when a hold is released.
-- Only a successful hold uses the token up, and nothing gives it back.

if redis.call('EXISTS', KEYS[6]) == 1 then
  return {'closed'}
end

local token_id = redis.call('GET', KEYS[1])
if not token_id then
  return {'not_admitted'}
end
local token = 'token:' .. token_id
local t = redis.call('HMGET', token, 'status', 'resv')
if t[1] == 'USED' then
  -- A retry after a timeout gets the hold it already made
  local expires_at = t[2] and redis.call('ZSCORE', KEYS[4], t[2])
  if expires_at then
    local raw = redis.call('GET', 'resv:' .. t[2])
    local held_quantity = raw and cjson.decode(raw).quantity or tonumber(ARGV[6])
    return {'existing', t[2], expires_at, tostring(held_quantity)}
  end
  -- The hold was released, expired or paid: the turn is spent
  return {'turn_used'}
end
if t[1] ~= 'ACTIVE' then
  return {'not_admitted'}
end

-- A product Redis does not know (lost, and not rebuilt yet) has nothing to
-- sell: the rebuild relies on no hold being made until it writes stock_total
if redis.call('EXISTS', KEYS[7]) == 0 then
  return {'out_of_stock', '0'}
end

local limit = tonumber(redis.call('GET', KEYS[8]) or '0')
local quantity = tonumber(ARGV[6])
if not quantity or quantity % 1 ~= 0 or quantity < 1 or quantity > limit then
  return {'invalid_quantity', tostring(limit)}
end

local held = tonumber(redis.call('GET', KEYS[3]) or '0')
if held + quantity > limit then
  return {'too_many', tostring(held)}
end

local stock = tonumber(redis.call('GET', KEYS[2]) or '0')
if stock < quantity then
  return {'out_of_stock', tostring(math.max(stock, 0))}
end

-- Everything checked: nothing below can fail halfway
redis.call('DECRBY', KEYS[2], quantity)
redis.call('INCRBY', KEYS[3], quantity)

local now_t = redis.call('TIME')
local now = tonumber(now_t[1]) * 1000 + math.floor(tonumber(now_t[2]) / 1000)
local ttl = tonumber(ARGV[4])
local expires_at = now + ttl
local resv = cjson.encode({user = tonumber(ARGV[2]), product = tonumber(ARGV[3]), token = token_id, quantity = quantity})
-- The key outlives the hold by a grace period: the cleanup worker releases
-- the hold at `expires_at` (holds:expiry), and needs the key to do it
redis.call('SET', 'resv:' .. ARGV[1], resv, 'PX', ttl + tonumber(ARGV[5]))
redis.call('ZADD', KEYS[4], expires_at, ARGV[1])

-- The user's turn is used: free the active slot for the next person in line
redis.call('HSET', token, 'status', 'USED', 'resv', ARGV[1])
redis.call('ZREM', KEYS[5], token_id)
return {'ok', ARGV[1], tostring(expires_at)}
