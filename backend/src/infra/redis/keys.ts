/**
 * Redis key layout. Every per-event key shares the hash tag `{eventId}` so all
 * keys touched by one Lua script live in the same slot (Redis Cluster-ready).
 */
export const keys = {
  available: (e: number) => `ev:{${e}}:available`, // SET   seat codes free to sell
  holds: (e: number) => `ev:{${e}}:holds`, //         ZSET  seat -> expiresAt ms
  holdMeta: (e: number) => `ev:{${e}}:holdmeta`, //   HASH  seat -> holdId|userId
  sold: (e: number) => `ev:{${e}}:sold`, //           SET   seat codes sold
  prices: (e: number) => `ev:{${e}}:prices`, //       HASH  seat -> price cents
  meta: (e: number) => `ev:{${e}}:meta`, //           HASH  capacity, openedAt
  outbox: (e: number) => `ev:{${e}}:outbox`, //       HASH  holdId -> order JSON
  confirmed: (e: number) => `ev:{${e}}:confirmed`, // HASH  holdId -> seat|userId
  queueSeq: (e: number) => `ev:{${e}}:queue:seq`, //  STR   last position assigned
  queueDone: (e: number) => `ev:{${e}}:queue:done`, // STR  last position processed
  userHold: (e: number, u: string) => `ev:{${e}}:user:${u}`,
  request: (e: number, r: string) => `ev:{${e}}:req:${r}`, //     result JSON
  requestPos: (e: number, r: string) => `ev:{${e}}:reqpos:${r}`, // queue position
  eventPattern: (e: number) => `ev:{${e}}:*`,

  activeSales: 'sales:active', // SET of eventIds currently on sale
};

/** Pub/sub channels (not keys): worker -> API instances -> SSE clients. */
export const channels = {
  updates: (e: number) => `fs:updates:${e}`,
  updatesPattern: 'fs:updates:*',
};
