-- Finish a claimed payment after its Postgres transaction committed: phase 3
-- of 3. Deletes the hold and adjusts one counter in the same call. `stock`
-- stays down: the units are sold (or, for a refund after a conflict, Postgres
-- had already sold them to others).
-- KEYS: 1 resv:{id}
-- ARGV: 1 'sold' or 'refunded'
-- Returns 1, or 0 if already finalized.

local raw = redis.call('GET', KEYS[1])
if not raw then
  return 0
end
local resv = cjson.decode(raw)
redis.call('DEL', KEYS[1])
local p = tostring(resv.product)
local n = resv.quantity
if ARGV[1] == 'sold' then
  redis.call('INCRBY', 'sold:' .. p, n)
else
  -- Refunded: these units no longer count toward the user's limit
  redis.call('DECRBY', 'user_units:' .. p .. ':' .. tostring(resv.user), n)
end
return 1
