-- Raise a numeric string key to a value, never lower it.
-- KEYS: 1 the key  ARGV: 1 value (integer)
-- Returns 1 if the key was set, 0 if it already held this value or more.
--
-- For processor progress (processed_upto:{s}:{partition} in the inventory
-- Redis, progress:<topic>:<partition> in the read cache): replicas and
-- successive owners of a partition write it, and checkout times within a
-- partition are not monotonic, so a plain SET could move it backwards.

local current = redis.call('GET', KEYS[1])
if current and tonumber(current) >= tonumber(ARGV[1]) then
  return 0
end
redis.call('SET', KEYS[1], ARGV[1])
return 1
