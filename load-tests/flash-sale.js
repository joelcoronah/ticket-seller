/**
 * Flash-sale stress test: N virtual users (default 5,000) hit "buy" at the SAME
 * instant for an event with limited stock (event 1 = 100 seats).
 *
 * What it proves:
 *   1. No overselling   -> teardown audits Redis + PostgreSQL invariants (thresholds = 0 violations)
 *   2. Shock absorption -> the enqueue endpoint stays fast (p95) while the queue drains
 *   3. TTL              -> unpaid holds (ABANDON_RATIO) go back to stock and retrying users get them
 *
 * Run (from the repo root, stack up with the dev overlay):
 *   docker run --rm -i --network ticket-architecture_default -e BASE_URL=http://lb \
 *     -v "$PWD/load-tests:/scripts" grafana/k6 run /scripts/flash-sale.js
 *
 * Env: BASE_URL, EVENT_ID, VUS, SEAT_MODE (best|pick), ABANDON_RATIO, RETRIES, ADMIN_TOKEN,
 *      START_DELAY_S, ENQUEUE_P95_MS
 */
import http from 'k6/http';
import { check, sleep } from 'k6';
import { Counter, Trend } from 'k6/metrics';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';
import { textSummary } from 'https://jslib.k6.io/k6-summary/0.0.2/index.js';

const BASE = __ENV.BASE_URL || 'http://localhost:3000';
const EVENT_ID = __ENV.EVENT_ID || '1';
const VUS = parseInt(__ENV.VUS || '5000', 10);
const SEAT_MODE = __ENV.SEAT_MODE || 'best'; // 'best' = best available, 'pick' = random specific seat
const ABANDON_RATIO = parseFloat(__ENV.ABANDON_RATIO || '0.2'); // holders who never pay
const RETRIES = parseInt(__ENV.RETRIES || '2', 10);
const ADMIN_TOKEN = __ENV.ADMIN_TOKEN || 'dev-admin-token';
const START_DELAY_S = parseInt(__ENV.START_DELAY_S || '5', 10);
// Latency SLO for the admission gate during the burst. 2 s is stable when k6 runs
// on the SAME machine as the stack (k6 itself eats ~7 cores at the spike).
// With the generator on a separate host, tighten it: ENQUEUE_P95_MS=500.
const ENQUEUE_P95_MS = parseInt(__ENV.ENQUEUE_P95_MS || '2000', 10);

const JSON_HEADERS = { 'content-type': 'application/json' };
const ADMIN = { headers: { 'x-admin-token': ADMIN_TOKEN } };

// ---- business metrics ------------------------------------------------------
const enqueued = new Counter('fs_enqueued');
const rejectedAtGate = new Counter('fs_rejected_sold_out_at_gate');
const holdsGranted = new Counter('fs_holds_granted');
const ticketsSold = new Counter('fs_tickets_sold');
const holdsAbandoned = new Counter('fs_holds_abandoned');
const soldOut = new Counter('fs_sold_out_in_queue');
const seatTaken = new Counter('fs_seat_unavailable');
const queueTimeouts = new Counter('fs_queue_timeouts');
const queueWait = new Trend('fs_queue_wait_ms', true);
const oversell = new Counter('fs_oversell_detected');
const invariantViolations = new Counter('fs_invariant_violations');

export const options = {
  setupTimeout: '60s',
  teardownTimeout: '120s',
  scenarios: {
    flash_sale: {
      executor: 'per-vu-iterations', // every VU = one human trying to buy
      vus: VUS,
      iterations: 1,
      maxDuration: '5m',
    },
  },
  thresholds: {
    // Correctness: hard gates. A single violation fails the run.
    fs_oversell_detected: ['count==0'],
    fs_invariant_violations: ['count==0'],
    // Resilience: the gate must stay fast and healthy while 5k users pile up.
    'http_req_failed': ['rate<0.01'],
    'http_req_duration{endpoint:enqueue}': [`p(95)<${ENQUEUE_P95_MS}`],
    'http_req_duration{endpoint:checkout}': ['p(95)<1000'],
    fs_queue_wait_ms: ['p(95)<20000'],
    checks: ['rate>0.99'],
  },
  summaryTrendStats: ['avg', 'med', 'p(90)', 'p(95)', 'p(99)', 'max'],
};

// ---- lifecycle ---------------------------------------------------------------
export function setup() {
  const open = http.post(`${BASE}/admin/events/${EVENT_ID}/open-sale?reset=true`, null, ADMIN);
  if (open.status !== 200) throw new Error(`open-sale failed: ${open.status} ${open.body}`);
  const event = http.get(`${BASE}/events/${EVENT_ID}`).json();
  const startAt = Date.now() + START_DELAY_S * 1000;
  console.log(`Sale opened: ${event.seats.length} seats, ${VUS} buyers, drop at +${START_DELAY_S}s`);
  return { startAt, seats: event.seats.map((s) => s.code) };
}

export default function (data) {
  const userId = `vu${__VU}-${uuidv4().slice(0, 8)}`;
  // Barrier: every VU fires at the same wall-clock instant -> real "flash" spike.
  const wait = (data.startAt - Date.now()) / 1000;
  if (wait > 0) sleep(wait);

  for (let attempt = 0; attempt <= RETRIES; attempt++) {
    if (tryToBuy(data, userId) !== 'RETRY') return;
    sleep(5 + Math.random() * 10); // a frustrated human hitting refresh
  }
}

function tryToBuy(data, userId) {
  const headers = Object.assign({ 'x-user-id': userId }, JSON_HEADERS);
  const requestId = uuidv4();
  const seatId = SEAT_MODE === 'pick' ? data.seats[Math.floor(Math.random() * data.seats.length)] : undefined;

  const res = http.post(
    `${BASE}/events/${EVENT_ID}/purchase-requests`,
    JSON.stringify({ requestId, seatId }),
    { headers, tags: { endpoint: 'enqueue' }, responseCallback: http.expectedStatuses(202, 409) },
  );

  if (res.status === 409) {
    rejectedAtGate.add(1);
    // `held > 0` means seats may come back when unpaid holds expire.
    return res.json('held') > 0 ? 'RETRY' : 'DONE';
  }
  if (!check(res, { 'enqueue accepted (202)': (r) => r.status === 202 })) return 'DONE';
  enqueued.add(1);

  // k6 core has no EventSource: poll the status endpoint (the browser uses SSE).
  const t0 = Date.now();
  let result = null;
  while (Date.now() - t0 < 60000) {
    sleep(0.5 + Math.random() * 0.5);
    const s = http.get(`${BASE}/events/${EVENT_ID}/purchase-requests/${requestId}`, { tags: { endpoint: 'status' } });
    if (s.status === 200 && s.json('state') === 'DONE') {
      result = s.json('result');
      break;
    }
  }
  queueWait.add(Date.now() - t0);
  if (!result) {
    queueTimeouts.add(1);
    return 'DONE';
  }

  switch (result.status) {
    case 'HELD':
    case 'USER_HAS_HOLD':
      holdsGranted.add(1);
      if (Math.random() < ABANDON_RATIO) {
        holdsAbandoned.add(1); // seat must return to stock after HOLD_TTL_MS
        return 'DONE';
      }
      sleep(1 + Math.random() * 3); // filling in the payment form
      return checkout(result, headers);
    case 'SOLD_OUT':
      soldOut.add(1);
      return 'RETRY';
    case 'SEAT_UNAVAILABLE':
      seatTaken.add(1);
      return 'RETRY';
    default:
      return 'DONE';
  }
}

function checkout(hold, headers) {
  const res = http.post(
    `${BASE}/events/${EVENT_ID}/holds/${hold.holdId}/checkout`,
    JSON.stringify({ seatId: hold.seatId }),
    { headers, tags: { endpoint: 'checkout' }, responseCallback: http.expectedStatuses(200, 410) },
  );
  if (check(res, { 'checkout confirmed (200)': (r) => r.status === 200 })) ticketsSold.add(1);
  return 'DONE';
}

export function teardown() {
  // Wait for the outbox to drain so PostgreSQL can be compared with Redis.
  let audit;
  for (let i = 0; i < 60; i++) {
    audit = http.get(`${BASE}/admin/events/${EVENT_ID}/audit`, ADMIN).json();
    if (audit.outboxPending === 0) break;
    sleep(1);
  }
  console.log(`AUDIT ${JSON.stringify(audit)}`);

  const inv = audit.invariants;
  if (!inv.noOversell) oversell.add(1);
  if (!inv.conservation) invariantViolations.add(1);
  if (inv.persisted === false) invariantViolations.add(1);
  check(audit, {
    'no oversell (Redis sold <= capacity, PG tickets unique)': () => inv.noOversell,
    'conservation: available + held + sold == capacity': () => inv.conservation,
    'PostgreSQL tickets == Redis sold': () => inv.persisted === true,
  });
}

export function handleSummary(data) {
  return {
    stdout: textSummary(data, { indent: ' ', enableColors: true }),
    '/scripts/results/summary.json': JSON.stringify(data, null, 2),
  };
}
