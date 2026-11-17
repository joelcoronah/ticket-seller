-- Demo data: event 1 = 100 seats (the classic "5,000 users vs 100 tickets"),
--            event 2 = 2,000 seats (for longer-running load tests).

INSERT INTO events (slug, name, venue, starts_at, sale_opens_at)
VALUES
  ('demo-concert',  'Demo Concert — Flash Sale', 'Teatro Principal', now() + interval '30 days', now()),
  ('stadium-night', 'Stadium Night',             'Estadio Nacional', now() + interval '60 days', now());

-- Event 1: 4 sections (A-D) x 25 seats, codes A-001 .. D-025
INSERT INTO seats (event_id, code, section, row_label, seat_number, price_cents)
SELECT 1,
       chr(64 + s) || '-' || lpad(n::text, 3, '0'),
       chr(64 + s),
       ((n - 1) / 5 + 1)::text,
       n,
       (9000 - s * 1000)          -- A = 80.00, D = 50.00
FROM generate_series(1, 4) AS s, generate_series(1, 25) AS n;

-- Event 2: 10 sections x 200 seats
INSERT INTO seats (event_id, code, section, row_label, seat_number, price_cents)
SELECT 2,
       chr(64 + s) || '-' || lpad(n::text, 3, '0'),
       chr(64 + s),
       ((n - 1) / 20 + 1)::text,
       n,
       4000 + (10 - s) * 500
FROM generate_series(1, 10) AS s, generate_series(1, 200) AS n;
