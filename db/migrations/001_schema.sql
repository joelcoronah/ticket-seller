-- =============================================================================
-- Flash-sale engine — PostgreSQL schema (source of truth for paid orders)
--
-- Redis owns the *hot, ephemeral* state (available / held seats).
-- PostgreSQL owns the *durable* state: catalog + paid orders + issued tickets.
-- Holds are deliberately NOT stored here: they live 5 minutes and would turn
-- every click into a DB write — exactly what this architecture avoids.
-- =============================================================================

CREATE TABLE events (
  id             bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  slug           text        NOT NULL UNIQUE,
  name           text        NOT NULL,
  venue          text        NOT NULL,
  starts_at      timestamptz NOT NULL,
  sale_opens_at  timestamptz NOT NULL,
  status         text        NOT NULL DEFAULT 'DRAFT'
                 CHECK (status IN ('DRAFT', 'ON_SALE', 'SOLD_OUT', 'CLOSED')),
  created_at     timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE seats (
  id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  event_id     bigint  NOT NULL REFERENCES events (id) ON DELETE CASCADE,
  code         text    NOT NULL,              -- 'A-012': the identifier used in Redis
  section      text    NOT NULL,
  row_label    text    NOT NULL,
  seat_number  int     NOT NULL,
  price_cents  int     NOT NULL CHECK (price_cents >= 0),
  UNIQUE (event_id, code),
  UNIQUE (event_id, id)                       -- target of the composite FK below
);

CREATE TABLE orders (
  id            uuid        PRIMARY KEY,      -- = holdId -> natural idempotency key
  event_id      bigint      NOT NULL REFERENCES events (id),
  user_id       text        NOT NULL,         -- external identity (JWT `sub`)
  status        text        NOT NULL
                CHECK (status IN ('PAID', 'CONFLICT', 'REFUNDED')),
  total_cents   int         NOT NULL CHECK (total_cents >= 0),
  payment_ref   text        NOT NULL UNIQUE,  -- one charge -> one order
  confirmed_at  timestamptz NOT NULL,         -- when Redis confirmed the sale
  created_at    timestamptz NOT NULL DEFAULT now()  -- when it was persisted
);
CREATE INDEX orders_event_idx ON orders (event_id);
CREATE INDEX orders_user_idx  ON orders (user_id, created_at DESC);
-- Ops: orders that need a manual refund (should always be empty)
CREATE INDEX orders_conflict_idx ON orders (event_id) WHERE status = 'CONFLICT';

CREATE TABLE tickets (
  id         bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  order_id   uuid        NOT NULL REFERENCES orders (id),
  event_id   bigint      NOT NULL,
  seat_id    bigint      NOT NULL,
  issued_at  timestamptz NOT NULL DEFAULT now(),
  -- The seat must belong to the same event as the ticket.
  FOREIGN KEY (event_id, seat_id) REFERENCES seats (event_id, id),
  -- LAST LINE OF DEFENSE against overselling: even if Redis were wrong,
  -- PostgreSQL physically cannot store two tickets for the same seat.
  CONSTRAINT tickets_one_per_seat UNIQUE (event_id, seat_id)
);
CREATE INDEX tickets_order_idx ON tickets (order_id);
