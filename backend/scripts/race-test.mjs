/**
 * Overselling proof, straight against Redis (no API, no Kafka).
 *
 *   node scripts/race-test.mjs [requests=5000] [seats=100]
 *
 * Fires N concurrent reserve.lua calls over 50 independent connections, mixing
 * "best available" and "fight for seat S-001". Then checks that every seat was
 * held at most once, that holds expire back to stock, and that confirm works.
 */
import { readFileSync } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import Redis from 'ioredis';

const REQUESTS = Number(process.argv[2] ?? 5000);
const SEATS = Number(process.argv[3] ?? 100);
const TTL_MS = 1500;
const E = 999_999; // throwaway event id
const url = process.env.REDIS_URL ?? 'redis://localhost:6379';
const lua = (n) => readFileSync(new URL(`../src/infra/redis/lua/${n}.lua`, import.meta.url), 'utf8');

const k = {
  available: `ev:{${E}}:available`, holds: `ev:{${E}}:holds`, holdMeta: `ev:{${E}}:holdmeta`,
  sold: `ev:{${E}}:sold`, outbox: `ev:{${E}}:outbox`, confirmed: `ev:{${E}}:confirmed`,
  done: `ev:{${E}}:queue:done`, user: (u) => `ev:{${E}}:user:${u}`, req: (r) => `ev:{${E}}:req:${r}`,
};

const conns = Array.from({ length: 50 }, () => {
  const c = new Redis(url);
  c.defineCommand('fsReserve', { numberOfKeys: 6, lua: lua('reserve') });
  c.defineCommand('fsReleaseExpired', { numberOfKeys: 3, lua: lua('release-expired') });
  c.defineCommand('fsConfirm', { numberOfKeys: 6, lua: lua('confirm') });
  return c;
});
const admin = conns[0];
let failures = 0;
const assert = (cond, msg) => { console.log(`${cond ? 'PASS' : 'FAIL'}  ${msg}`); if (!cond) failures++; };

// --- setup -----------------------------------------------------------------
for (const key of await admin.keys(`ev:{${E}}:*`)) await admin.del(key);
const seatCodes = Array.from({ length: SEATS }, (_, i) => `S-${String(i + 1).padStart(3, '0')}`);
await admin.sadd(k.available, ...seatCodes);

// --- 1. the stampede -------------------------------------------------------
const reserve = (i) => {
  const userId = `u${i}`, requestId = randomUUID();
  const seat = i % 10 === 0 ? 'S-001' : ''; // 10% fight for the same seat
  return conns[i % conns.length].fsReserve(
    k.available, k.holds, k.holdMeta, k.user(userId), k.req(requestId), k.done,
    seat, userId, randomUUID(), TTL_MS, 60, i + 1, 50,
  ).then((r) => ({ ...JSON.parse(r), userId, requestId }));
};

const t0 = performance.now();
const results = await Promise.all(Array.from({ length: REQUESTS }, (_, i) => reserve(i)));
const ms = performance.now() - t0;

const held = results.filter((r) => r.status === 'HELD');
const byStatus = Object.groupBy(results, (r) => r.status);
console.log(`\n${REQUESTS} concurrent requests for ${SEATS} seats in ${ms.toFixed(0)} ms (${Math.round(REQUESTS / (ms / 1000))} ops/s)`);
console.log(Object.fromEntries(Object.entries(byStatus).map(([s, l]) => [s, l.length])), '\n');

assert(held.length === SEATS, `exactly ${SEATS} holds granted (got ${held.length})`);
assert(new Set(held.map((h) => h.seatId)).size === held.length, 'no seat held twice');
assert(held.filter((h) => h.seatId === 'S-001').length === 1, 'contested seat S-001 has a single winner');
assert((await admin.scard(k.available)) === 0, 'available set is empty');
assert((await admin.zcard(k.holds)) === SEATS, 'holds zset == capacity');

// --- 2. idempotency: replaying a request returns the cached outcome --------
const first = held[0];
const replay = JSON.parse(await conns[1].fsReserve(
  k.available, k.holds, k.holdMeta, k.user(first.userId), k.req(first.requestId), k.done,
  '', first.userId, randomUUID(), TTL_MS, 60, 1, 50));
assert(replay.holdId === first.holdId, 'redelivered request returns the same hold');

// --- 3. confirm one hold ---------------------------------------------------
const winner = held[1];
const payload = JSON.stringify({ orderId: winner.holdId, seatId: winner.seatId });
const confirm = (h) => admin.fsConfirm(k.holds, k.holdMeta, k.sold, k.user(h.userId), k.outbox, k.confirmed,
  h.seatId, h.holdId, h.userId, payload);
assert((await confirm(winner)) === 'CONFIRMED', 'payment confirms an active hold');
assert((await confirm(winner)) === 'ALREADY_CONFIRMED', 'confirm is idempotent');
assert((await admin.hlen(k.outbox)) === 1, 'outbox entry written atomically with the sale');

// --- 4. TTL: unpaid holds return to stock ----------------------------------
await sleep(TTL_MS + 300);
const loser = held[2];
assert((await confirm(loser)) === 'HOLD_EXPIRED', 'paying after TTL is rejected');
const released = await admin.fsReleaseExpired(k.available, k.holds, k.holdMeta, 1000);
assert(released === SEATS - 1, `reaper released ${SEATS - 1} expired holds (got ${released})`);
const [avail, holds, sold] = await Promise.all([admin.scard(k.available), admin.zcard(k.holds), admin.scard(k.sold)]);
assert(avail + holds + sold === SEATS, `conservation: available(${avail}) + held(${holds}) + sold(${sold}) == ${SEATS}`);
assert(!(await admin.sismember(k.available, winner.seatId)), 'sold seat never returns to stock');

for (const key of await admin.keys(`ev:{${E}}:*`)) await admin.del(key);
await Promise.all(conns.map((c) => c.quit()));
console.log(failures ? `\n${failures} check(s) FAILED` : '\nAll checks passed');
process.exit(failures ? 1 : 0);
