import { readFileSync } from 'node:fs';
import { join } from 'node:path';
import type { Redis, Result } from 'ioredis';

const lua = (name: string) => readFileSync(join(__dirname, 'lua', `${name}.lua`), 'utf8');

/**
 * Registers the Lua scripts as custom commands. ioredis sends EVALSHA and
 * falls back to EVAL on NOSCRIPT, so scripts survive Redis restarts.
 */
export function defineScripts(client: Redis): void {
  client.defineCommand('fsReserve', { numberOfKeys: 6, lua: lua('reserve') });
  client.defineCommand('fsReleaseExpired', { numberOfKeys: 3, lua: lua('release-expired') });
  client.defineCommand('fsConfirm', { numberOfKeys: 6, lua: lua('confirm') });
}

declare module 'ioredis' {
  interface RedisCommander<Context> {
    fsReserve(...args: (string | number)[]): Result<string, Context>;
    fsReleaseExpired(...args: (string | number)[]): Result<number, Context>;
    fsConfirm(...args: (string | number)[]): Result<string, Context>;
  }
}
