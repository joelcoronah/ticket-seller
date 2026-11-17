--[[
  confirm.lua — Held -> Sold, plus an outbox entry, in ONE atomic step.

  Writing the outbox inside the same script means a confirmed sale can never
  exist without a pending persistence record: if the API crashes before
  producing to Kafka, the outbox reconciler republishes it.

  KEYS[1] ev:{E}:holds      ZSET seat -> expiresAt
  KEYS[2] ev:{E}:holdmeta   HASH seat -> "holdId|userId"
  KEYS[3] ev:{E}:sold       SET
  KEYS[4] ev:{E}:user:{U}   STR
  KEYS[5] ev:{E}:outbox     HASH holdId -> order JSON (pending persistence)
  KEYS[6] ev:{E}:confirmed  HASH holdId -> "seat|userId" (idempotent confirm)

  ARGV[1] seatId
  ARGV[2] holdId
  ARGV[3] userId
  ARGV[4] order JSON payload for the outbox

  Returns CONFIRMED | ALREADY_CONFIRMED | HOLD_NOT_FOUND | HOLD_EXPIRED
]]

local prev = redis.call('HGET', KEYS[6], ARGV[2])
if prev then
  if prev == (ARGV[1] .. '|' .. ARGV[3]) then return 'ALREADY_CONFIRMED' end
  return 'HOLD_NOT_FOUND'
end

local meta = redis.call('HGET', KEYS[2], ARGV[1])
if meta ~= (ARGV[2] .. '|' .. ARGV[3]) then return 'HOLD_NOT_FOUND' end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
local expiresAt = tonumber(redis.call('ZSCORE', KEYS[1], ARGV[1]))
if (not expiresAt) or expiresAt <= now then return 'HOLD_EXPIRED' end

redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('SADD', KEYS[3], ARGV[1])
redis.call('DEL', KEYS[4])
redis.call('HSET', KEYS[5], ARGV[2], ARGV[4])
redis.call('HSET', KEYS[6], ARGV[2], ARGV[1] .. '|' .. ARGV[3])
return 'CONFIRMED'
