-- Rebuild a product's counters after Redis lost them, atomically.
-- Runs only while `stock_total:{p}` is missing: joins and reserves refuse a
-- product without it, so no hold can be made while the inputs are gathered,
-- and it is written last (in this same call), which reopens the product.
-- KEYS: 1 stock:{p}  2 stock_total:{p}  3 sold:{p}  4 max_per_user:{p}
--       5 sale_opens_at:{p}  6 closed:{p}  7 lobby_flushed:{p}
-- ARGV: 1 product id  2 total units  3 sold units  4 sale_opens_at
--       5 closed (0/1)  6 on sale (0/1)  7 max units per user
--       8 JSON {holds: [reservation ids that were live when scanned],
--               user_units: {user id: paid units}}
-- Returns 1 if rebuilt, 0 if the product was already there.

if redis.call('EXISTS', KEYS[2]) == 1 then
  return 0
end
local p = ARGV[1]
local n = tonumber(ARGV[2])
local input = cjson.decode(ARGV[8])

-- Each user's limit counts paid units plus live holds. Set, not added to, in
-- case an old counter survived
local per_user = {}
for user, count in pairs(input.user_units) do
  per_user[tostring(user)] = count
end
local held = 0
for _, id in ipairs(input.holds) do
  -- Re-read: a hold may have been released or expired since the scan
  local raw = redis.call('GET', 'resv:' .. id)
  if raw then
    local resv = cjson.decode(raw)
    held = held + resv.quantity
    local user = tostring(resv.user)
    per_user[user] = (per_user[user] or 0) + resv.quantity
  end
end
for user, count in pairs(per_user) do
  redis.call('SET', 'user_units:' .. p .. ':' .. user, count)
end

redis.call('SET', KEYS[1], n - tonumber(ARGV[3]) - held)
redis.call('SET', KEYS[3], ARGV[3])
redis.call('SET', KEYS[4], ARGV[7])
redis.call('SET', KEYS[5], ARGV[4])
if ARGV[5] == '1' then
  redis.call('SET', KEYS[6], 1)
end
-- Once the sale is open, joins go straight to the queue (the lobby is gone)
if ARGV[6] == '1' then
  redis.call('SET', KEYS[7], 1)
end
redis.call('SET', KEYS[2], n)
return 1
