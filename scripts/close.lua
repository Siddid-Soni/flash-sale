-- Close a product's sale, if the product exists, atomically.
-- KEYS: 1 stock_total:{p}  2 closed:{p}
-- Returns 1 if the product exists (now closed, or already was), 0 if not.

if redis.call('EXISTS', KEYS[1]) == 0 then
  return 0
end
redis.call('SET', KEYS[2], 1)
return 1
