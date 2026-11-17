# Estrategia de pruebas de estrés

## Qué queremos demostrar

| Propiedad | Cómo se mide | Criterio (threshold k6) |
|---|---|---|
| **Cero sobreventa** | Teardown llama a `/admin/events/:id/audit`: Redis `sold ≤ capacity`, tickets en PG únicos, 0 órdenes `CONFLICT` | `fs_oversell_detected count==0` |
| **Conservación** | `available + held + sold == capacity`, y PG == Redis cuando el outbox está vacío | `fs_invariant_violations count==0` |
| **Absorción del pico** | Latencia del gate mientras 5.000 usuarios llegan en el mismo instante | `http_req_duration{endpoint:enqueue} p(95) < ENQUEUE_P95_MS` |
| **Sin errores** | Todas las respuestas son 2xx o 409/410 de negocio | `http_req_failed rate<1%` |
| **Cola rápida** | Tiempo desde el 202 hasta el resultado | `fs_queue_wait_ms p(95)<20s` |
| **TTL** | Los holds abandonados vuelven al stock y los reintentos los compran | `fs_tickets_sold` llega a la capacidad |

## Niveles de prueba

1. **Unidad de concurrencia, solo Redis** — [backend/scripts/race-test.mjs](../backend/scripts/race-test.mjs). 5.000 llamadas concurrentes a `reserve.lua` por 50 conexiones. Prueba la atomicidad sin ruido de HTTP ni de Kafka.
   ```bash
   cd backend && npm run test:race            # o: node scripts/race-test.mjs 20000 500
   ```
2. **Extremo a extremo con k6** — [load-tests/flash-sale.js](../load-tests/flash-sale.js). El sistema completo, detrás del balanceador.

## Diseño del script k6

- **Executor `per-vu-iterations`**: cada VU es una persona que intenta comprar una vez (con reintentos).
- **Barrera temporal**: `setup()` fija `startAt = now + 5 s` y todos los VUs duermen hasta ese instante, así el pico es real (5.000 peticiones en el mismo milisegundo) en lugar de una rampa.
- **Comportamiento humano**:
  - `ABANDON_RATIO` (por defecto 20 %) consigue un hold y nunca paga, para ejercitar el TTL.
  - Quien recibe `SOLD_OUT` / `SEAT_UNAVAILABLE` reintenta tras 5–15 s (`RETRIES`), pero solo si el gate dice que hay holds pendientes (`held > 0`).
  - Entre el hold y el pago hay 1–4 s ("rellenando la tarjeta").
- **Seguimiento por polling**: k6 no trae cliente SSE de serie, así que consulta `GET …/purchase-requests/:id` cada 0,5–1 s. El navegador usa SSE. Para probar SSE en k6 existe la extensión [xk6-sse](https://github.com/phymbert/xk6-sse).
- **`SEAT_MODE=pick`**: cada usuario elige un asiento al azar, lo que genera muchos conflictos `SEAT_UNAVAILABLE` (el caso del "mapa de asientos").
- `responseCallback` marca 409/410 como respuestas esperadas: son resultados de negocio, no fallos.
- `handleSummary` guarda `load-tests/results/summary.json` para el portafolio.

## Cómo ejecutarlo

```bash
# 1. Stack local (3 API + 2 workers + nginx LB)
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build

# 2. k6 dentro de la red de Docker (evita el NAT del host)
docker run --rm -i --network ticket-architecture_default \
  -e BASE_URL=http://lb -e VUS=5000 \
  -v "$PWD/load-tests:/scripts" grafana/k6:0.54.0 run /scripts/flash-sale.js
```

Variantes útiles:

```bash
-e SEAT_MODE=pick                      # todos pelean por asientos concretos
-e EVENT_ID=2                          # 2.000 asientos: la cola trabaja de verdad
-e ABANDON_RATIO=0.5 -e RETRIES=4      # mucho abandono + reintentos (combinar con HOLD_TTL_MS=15000)
-e ENQUEUE_P95_MS=500                  # SLO estricto cuando k6 corre en otra máquina
```

Para que el TTL se vea dentro de la prueba, levanta el stack con un TTL corto:
`HOLD_TTL_MS=15000 docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d`.

## Resultados medidos (portátil, todo en una VM Docker de 12 vCPU)

**Atomicidad (solo Redis)**: 5.000 peticiones / 100 asientos → **331 ms (~15.000 reservas/s)**, exactamente 100 holds, 0 duplicados. Los 13 checks pasan.

**Extremo a extremo, 5.000 VUs, evento de 100 asientos, TTL 15 s, 4 reintentos:**

| Métrica | Resultado |
|---|---|
| Holds concedidos | 129 (29 abandonados → devueltos por el reaper → revendidos) |
| Tickets vendidos | **100 / 100** |
| Sobreventa · violaciones de invariantes | **0 · 0** |
| Tickets en PG vs `sold` en Redis | 100 = 100 |
| `http_req_failed` | 0,00 % (≈24.800 peticiones) |
| Gate `enqueue` | mediana 2 ms · p95 0,9–1,9 s durante el pico |
| Espera en la cola (202 → resultado) | p95 970 ms |
| Checkout | p95 204 ms (incluye 50–200 ms de pasarela simulada) |
| Rechazados en el gate por `SOLD_OUT` | ~95 % de los intentos, sin tocar Kafka |

### Lectura honesta del p95 del gate

El p95 durante la ráfaga equivale al **tiempo de drenar** 5.000 peticiones simultáneas: N / (réplicas × throughput por réplica). Con nginx registrando `$upstream_response_time` se comprobó que el tiempo se gasta dentro de la API (la conexión al upstream tarda ~0 ms) y que el balanceo es uniforme (~1.695 peticiones por réplica). El factor dominante en local es que **k6 consume ~7 de los 12 cores en el pico**, compitiendo con las APIs. Por eso el p95 varía entre 0,9 y 2 s de una ejecución a otra.

Para tener números representativos, ejecuta k6 desde **otra máquina** (u otro VPS en la misma región) con `ENQUEUE_P95_MS=500`. Las palancas para bajar el p95 son horizontales: `API_REPLICAS`, más vCPU, o un script Lua de admisión que haga todo el gate en un único round-trip.

Ajustes que sí tuvieron efecto medible:
- `listen backlog` 8192 + `net.core.somaxconn` → el p95 de `http_req_connecting` bajó de **3,05 s a 0,33 s** (antes se desbordaba la cola de `accept` y el cliente pagaba un reintento de SYN).
- Pasar de 1 a 3 réplicas de API detrás de un balanceador.

## Pruebas de caos recomendadas (siguiente paso)

| Experimento | Esperado |
|---|---|
| `docker compose kill worker` a mitad de la prueba | El otro worker toma las particiones tras el rebalanceo; nada se pierde; sin sobreventa. |
| `docker compose restart redis` durante la venta | Errores transitorios en el gate (503); al volver, AOF restaura el estado y los invariantes se cumplen. |
| `docker compose stop postgres` durante la venta | El checkout sigue funcionando; el outbox crece; al arrancar PG, el persister drena y `persisted` vuelve a `true`. |
| `PAYMENT_FAILURE_RATE=0.3` | 402 para los pagos rechazados; los holds expiran y se revenden. |
| Soak: `EVENT_ID=2` (2.000 asientos), 20 min | Sin fugas de memoria en API/SSE; el lag del consumer se mantiene acotado. |
