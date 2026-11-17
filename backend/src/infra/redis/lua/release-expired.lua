--[[
  release-expired.lua — returns expired holds to the general inventory.

  Why not rely on key TTL + keyspace notifications? Notifications are
  fire-and-forget pub/sub: if no subscriber is connected when the key expires,
  the event is lost and the seat leaks forever. A ZSET scored by expiry is
  durable and can be swept by any number of workers safely (script is atomic).

  KEYS[1] ev:{E}:available  SET
  KEYS[2] ev:{E}:holds      ZSET seat -> expiresAt (ms)
  KEYS[3] ev:{E}:holdmeta   HASH seat -> "holdId|userId"
  ARGV[1] max seats to release in one call (bounds script run time)

  Returns the number of seats released.
]]

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local expired = redis.call('ZRANGEBYSCORE', KEYS[2], '-inf', now, 'LIMIT', 0, tonumber(ARGV[1]))
for _, seat in ipairs(expired) do
  redis.call('ZREM', KEYS[2], seat)
  redis.call('HDEL', KEYS[3], seat)
  redis.call('SADD', KEYS[1], seat)
end
return #expired
