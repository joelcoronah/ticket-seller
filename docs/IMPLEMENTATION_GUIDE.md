# Guía de implementación

Las fases 0–7 **ya están implementadas y verificadas** en este repo. Cada fase indica qué construir, por qué y cómo comprobarlo. Las fases 8–10 son lo siguiente.

## Estructura del proyecto

```
ticket-architecture/
├── backend/                         # NestJS 11 + Fastify · una imagen, dos entrypoints
│   ├── src/
│   │   ├── main.ts                  # entrypoint API (HTTP + SSE)
│   │   ├── worker.ts                # entrypoint workers (consumers + jobs)
│   │   ├── api.module.ts            # módulos que carga la API
│   │   ├── worker.module.ts         # módulos que cargan los workers
│   │   ├── config.ts                # toda la config desde variables de entorno
│   │   ├── domain/types.ts          # contratos: mensajes Kafka, resultados, eventos realtime
│   │   ├── common/                  # @UserId() (auth de demo), AdminGuard
│   │   ├── infra/
│   │   │   ├── redis/
│   │   │   │   ├── lua/             # reserve · confirm · release-expired  ← el núcleo
│   │   │   │   ├── keys.ts          # layout de claves con hash tags {eventId}
│   │   │   │   ├── scripts.ts       # defineCommand + tipado
│   │   │   │   └── redis.service.ts # cliente con auto-pipelining
│   │   │   ├── kafka/               # KafkaService, BatchingProducer, topics
│   │   │   └── database/            # Pool de pg + helper de transacciones
│   │   ├── modules/
│   │   │   ├── inventory/           # única puerta a los scripts Lua
│   │   │   ├── purchase/            # gate de admisión + estado + SSE
│   │   │   ├── realtime/            # publisher (workers) · subscriber/demux (API)
│   │   │   ├── checkout/            # pago (mock) + confirm + publicación
│   │   │   ├── catalog/             # evento, asientos, disponibilidad (con caché)
│   │   │   └── admin/               # open-sale, close-sale, audit
│   │   └── workers/
│   │       ├── purchase.consumer.ts        # FIFO, chunks en pipeline
│   │       ├── order-persister.consumer.ts # Kafka → PostgreSQL idempotente
│   │       └── maintenance.service.ts      # reaper de TTL + reconciler del outbox
│   ├── scripts/race-test.mjs        # prueba de sobreventa directa contra Redis
│   └── Dockerfile
├── db/migrations/                   # 001_schema.sql · 002_seed.sql
├── load-tests/flash-sale.js         # k6, 5.000 VUs
├── deploy/nginx.dev.conf            # balanceador local (Traefik en producción)
├── docker-compose.yml               # stack de producción (Dokploy)
├── docker-compose.dev.yml           # overlay local: puertos, LB, reset habilitado
└── docs/
```

**Principios de la estructura**

- `infra/` sabe *cómo* hablar con Redis, Kafka o PG; `modules/` sabe *qué* significa para el negocio. Ningún módulo construye claves Redis a mano: todo pasa por `keys.ts` e `InventoryService`.
- API y workers comparten código e imagen, pero cargan módulos distintos. Así escalan por separado (`API_REPLICAS`, `WORKER_REPLICAS`).
- La API no importa nada de `workers/`, y los workers no levantan HTTP.

## Fase 0 — Entorno

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build
curl localhost:3000/health
```
Requisitos: Docker y Node 22. PostgreSQL aplica `db/migrations` en el primer arranque. Para reiniciar desde cero: `docker compose down -v`.

## Fase 1 — Infraestructura (`infra/`)

1. **Redis**: `ioredis` con `enableAutoPipelining`. Los comandos de un mismo tick viajan en un solo round-trip, lo que importa mucho con 5.000 peticiones concurrentes. Una conexión duplicada para `PSUBSCRIBE`, porque una conexión suscrita queda bloqueada.
2. **Kafka**: `kafkajs` contra Redpanda. El productor es idempotente y va envuelto en `BatchingProducer`. Los topics se crean al arrancar (`createTopics` es idempotente). Particionador `DefaultPartitioner` (murmur2, compatible con Java).
3. **PostgreSQL**: `pg` con SQL explícito. Las garantías interesantes (`UNIQUE`, `ON CONFLICT`, transacciones) se leen mejor sin ORM.

✅ *Verificación:* el arranque de API y workers registra "Kafka topics created" / "API listening".

## Fase 2 — Inventario atómico (`infra/redis/lua`, `modules/inventory`)

1. Escribe `reserve.lua` en este orden: idempotencia → reloj `TIME` → reclaim perezoso → límite por usuario → `SPOP`/`SREM` → registrar el hold.
2. `confirm.lua`: valida propietario y expiración, mueve el asiento a `sold` y escribe el outbox **en el mismo script**.
3. `release-expired.lua`: barre el ZSET por score.
4. Regístralos con `defineCommand` (EVALSHA con fallback automático a EVAL).

✅ *Verificación:* `cd backend && npm run test:race`. Los 13 checks deben pasar (5.000 → exactamente 100 holds, idempotencia, confirm, TTL, conservación).

## Fase 3 — Gate de admisión (`modules/purchase`)

`POST /events/:id/purchase-requests {requestId, seatId?}` + cabecera `x-user-id`:
1. En paralelo: ¿venta activa?, ¿`requestId` ya visto?, `SCARD available`, `ZCARD holds`.
2. Si `available == 0` → `409 {code: SOLD_OUT, held}`.
3. `INCR queue:seq` → posición; `SET reqpos NX`.
4. Produce en `purchase-requests` con key `eventId` y responde `202 {position, ahead, streamUrl}`.

✅ *Verificación:* `curl` dos veces con el mismo `requestId` → misma posición.

## Fase 4 — Worker FIFO (`workers/purchase.consumer.ts`)

- `eachBatch` con `eachBatchAutoResolve: false`; chunks de 250 → `InventoryService.reserveMany` (un pipeline) → publicar resultados → `resolveOffset` → `commitOffsetsIfNecessary` → `heartbeat`.
- Mensajes malformados se descartan con log (poison pill); los que llevan más de 10 min en cola se marcan `EXPIRED_IN_QUEUE`.

✅ *Verificación:* tras encolar, `GET …/purchase-requests/:id` devuelve `{state: DONE, result: {status: HELD, …}}`.

## Fase 5 — Tiempo real (`modules/realtime`, `@Sse` en purchase)

- Workers: `PUBLISH fs:updates:{E}` con `result` y `progress` (uno por chunk).
- API: un `PSUBSCRIBE` → `Map<requestId, Subject>` + progreso por evento limitado a 250 ms.
- Stream: `progress…` → `result` → cierre. Re-check cada 5 s contra Redis por si se pierde algún mensaje.

✅ *Verificación:* `curl -N localhost:3000/events/1/purchase-requests/<id>/stream`.

## Fase 6 — Checkout y persistencia (`modules/checkout`, `workers/order-persister`)

1. Valida el hold (propietario, no expirado) → cobra con `idempotencyKey = holdId`.
2. `confirm.lua`; si devuelve `HOLD_EXPIRED` → reembolso + `410`.
3. Publica `orders-confirmed` (si falla, lo cubre el outbox).
4. Persister: transacción `INSERT orders ON CONFLICT DO NOTHING` + `INSERT tickets ON CONFLICT ON CONSTRAINT tickets_one_per_seat` → si hay conflicto, `status = CONFLICT` + alerta. Después, `HDEL outbox`.

✅ *Verificación:* `GET /admin/events/1/audit` → `invariants: {conservation, noOversell, persisted}` todos a `true`.

## Fase 7 — TTL y mantenimiento (`workers/maintenance.service.ts`)

Reaper cada 1 s y reconciler del outbox cada 15 s, protegidos contra ejecuciones solapadas. Seguros en N réplicas.

✅ *Verificación:* `HOLD_TTL_MS=15000`, reserva sin pagar y espera 16 s: el log muestra "expired hold(s) returned to inventory".

## Fase 8 — Frontend React (Netlify) — *siguiente*

```bash
npm create vite@latest frontend -- --template react-ts
```

Pantallas: **Evento** (mapa de asientos con `GET /events/:id` + `GET /events/:id/availability` cada 2 s) → **Sala de espera** (posición + barra de progreso) → **Checkout** (cuenta atrás del hold) → **Confirmación**.

Hook clave, la fila virtual por SSE:

```ts
// src/hooks/usePurchaseQueue.ts
import { useEffect, useState } from 'react';

const API = import.meta.env.VITE_API_URL;

type Result = { status: 'HELD' | 'USER_HAS_HOLD' | 'SOLD_OUT' | 'SEAT_UNAVAILABLE' | 'EXPIRED_IN_QUEUE';
                holdId?: string; seatId?: string; expiresAt?: number };
type State =
  | { phase: 'idle' }
  | { phase: 'queued'; position: number; ahead: number; total: number }
  | { phase: 'done'; result: Result }
  | { phase: 'error'; code: string; held?: number };

export function usePurchaseQueue(eventId: number, userId: string) {
  const [state, setState] = useState<State>({ phase: 'idle' });
  const [requestId, setRequestId] = useState<string | null>(null);

  async function join(seatId?: string) {
    const id = crypto.randomUUID();               // idempotencia: reintentos = misma solicitud
    const res = await fetch(`${API}/events/${eventId}/purchase-requests`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'x-user-id': userId },
      body: JSON.stringify({ requestId: id, seatId }),
    });
    const body = await res.json();
    if (res.status !== 202) return setState({ phase: 'error', code: body.code, held: body.held });
    setState({ phase: 'queued', position: body.position, ahead: body.ahead, total: body.ahead + 1 });
    setRequestId(id);
  }

  useEffect(() => {
    if (!requestId) return;
    const es = new EventSource(`${API}/events/${eventId}/purchase-requests/${requestId}/stream`);
    es.addEventListener('progress', (e) => {
      const { position, ahead } = JSON.parse((e as MessageEvent).data);
      setState((s) => ({ phase: 'queued', position, ahead,
        total: s.phase === 'queued' ? Math.max(s.total, ahead + 1) : ahead + 1 }));
    });
    es.addEventListener('result', (e) => {
      setState({ phase: 'done', result: JSON.parse((e as MessageEvent).data) });
      es.close();                                   // ¡imprescindible! si no, EventSource reconecta
    });
    es.addEventListener('error', () => { /* EventSource reintenta solo; el servidor re-chequea */ });
    return () => es.close();
  }, [eventId, requestId]);

  return { state, join };
}
// Barra de progreso: (total - ahead) / total
```

UX recomendada:
- Botón "Comprar" deshabilitado tras el clic. El `requestId` se genera **una vez** por intento.
- `409 SOLD_OUT` con `held > 0` → "Agotado por ahora: pueden liberarse entradas en unos minutos".
- `HELD` → temporizador hasta `expiresAt` y botón de pago. `410` → "Tu reserva expiró".

`netlify.toml`:
```toml
[build]
  command = "npm run build"
  publish = "dist"
[[redirects]]
  from = "/*"
  to = "/index.html"
  status = 200
```
Variable de entorno en Netlify: `VITE_API_URL=https://api.tu-dominio.com`. En el backend: `CORS_ORIGINS=https://tu-sitio.netlify.app`.

## Fase 9 — Despliegue en Dokploy — *siguiente*

1. Sube el repo a GitHub.
2. Dokploy → **Create Service → Compose** → repo → ruta `docker-compose.yml` (solo el base, **sin** el overlay de dev).
3. **Environment**: copia [.env.example](../.env.example) con secretos reales (`ADMIN_TOKEN` largo y aleatorio, `ALLOW_RESET=false`).
4. **Domains**: asigna `api.tu-dominio.com` → servicio `api`, puerto `3000`, HTTPS con Let's Encrypt. Traefik balancea entre las réplicas de `api`.
5. Deploy y abre la venta:
   ```bash
   curl -X POST https://api.tu-dominio.com/admin/events/1/open-sale -H "x-admin-token: $ADMIN_TOKEN"
   ```
6. VPS mínimo recomendado: 4 vCPU / 8 GB (Redpanda reserva 1 GB, Redis 512 MB).
7. Prueba de carga **desde otra máquina** contra el dominio público: `-e BASE_URL=https://api.tu-dominio.com -e ENQUEUE_P95_MS=500`, sin `reset` (o con un evento de pruebas y `ALLOW_RESET` temporal).

## Fase 10 — Endurecimiento — *backlog*

| Tarea | Detalle |
|---|---|
| Auth real | Guard JWT (`sub` → userId). Para SSE: cookie `HttpOnly` o token de corta vida en la query. |
| Rate limiting | Middleware de Traefik `rateLimit` por IP + límite por usuario en Redis. |
| Sala de espera por lotes | Token de admisión firmado, emitido a N usuarios/min, que el gate exige. |
| Extender el hold en el checkout | `+60 s` al iniciar el pago para no cobrar y luego reembolsar. |
| Observabilidad | `/metrics` (prom-client): lag del consumer, holds activos, tamaño del outbox, p95 del gate. Dashboard en Grafana. |
| Tests | Unitarios de `InventoryService` con Redis real (Testcontainers) + e2e del flujo completo. |
| CI | GitHub Actions: typecheck, build, `test:race` con un servicio Redis y k6 smoke con 200 VUs. |
| HA | Redis Sentinel/gestionado; Redpanda con 3 nodos y RF=3. |
