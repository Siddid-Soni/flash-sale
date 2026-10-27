-- Read-only snapshot of a user's place in a product's waiting room, in one
-- round trip.
-- KEYS: 1 user_token:{p}:{u}  2 admitted_upto:{p}  3 stock:{p}  4 sold:{p}
--       5 stock_total:{p}  6 closed:{p}  7 sale_opens_at:{p}  8 admit_samples:{p}
-- Returns {'none'} if the user never joined, {'expired', id, closed} if the
-- token is gone, otherwise {'token', id, status, seq, published, pttl_ms,
-- admitted_upto, stock, sold, stock_total, closed, sale_opens_at,
-- newest_sample, oldest_sample, hold_live}; samples are
-- "{ms}:{admitted_upto}" or '', and hold_live is 1 while a USED token's
-- reservation lasts.

local id = redis.call('GET', KEYS[1])
if not id then
  return {'none'}
end
local key = 'token:' .. id
local t = redis.call('HMGET', key, 'status', 'seq', 'published', 'resv')
if not t[1] then
  return {'expired', id, tostring(redis.call('EXISTS', KEYS[6]))}
end
local hold_live = t[4] and redis.call('EXISTS', 'resv:' .. t[4]) or 0
return {
  'token', id, t[1], t[2] or '', t[3] or '', tostring(redis.call('PTTL', key)),
  redis.call('GET', KEYS[2]) or '0', redis.call('GET', KEYS[3]) or '0',
  redis.call('GET', KEYS[4]) or '0', redis.call('GET', KEYS[5]) or '0',
  tostring(redis.call('EXISTS', KEYS[6])), redis.call('GET', KEYS[7]) or '',
  redis.call('LINDEX', KEYS[8], 0) or '', redis.call('LINDEX', KEYS[8], -1) or '',
  tostring(hold_live),
}
