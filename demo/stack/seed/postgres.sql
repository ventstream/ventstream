-- Demo source data: a small e-commerce "shop" schema.
--
-- Auto-loaded by the Postgres container on first boot (mounted into
-- /docker-entrypoint-initdb.d). Creates the schema, seeds a handful of
-- customers + orders + line items, and declares the publication the
-- VentStream agent consumes.
--
-- Shape mirrors a typical demo schema (primary + 1:1 + 1:many) in a
-- different domain so the same scenarios apply:
--   orders          → primary
--   customers       → 1:1   (orders.customer_id → customers.customer_id)
--   order_items     → 1:many (order_items.order_id → orders.order_id)

CREATE SCHEMA IF NOT EXISTS shop;

CREATE TABLE shop.customers (
  customer_id text PRIMARY KEY,
  name        text NOT NULL,
  email       text,
  tier        text DEFAULT 'standard'
);

CREATE TABLE shop.orders (
  order_id    text PRIMARY KEY,
  customer_id text NOT NULL REFERENCES shop.customers(customer_id),
  status      text NOT NULL DEFAULT 'pending',
  total       numeric(10,2) NOT NULL DEFAULT 0,
  placed_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE shop.order_items (
  item_id  text PRIMARY KEY,
  order_id text NOT NULL REFERENCES shop.orders(order_id),
  sku      text NOT NULL,
  qty      int  NOT NULL DEFAULT 1,
  price    numeric(10,2) NOT NULL DEFAULT 0
);

-- ── join-key indexes (REQUIRED for SQL-denormalize mode) ─────────────
-- Postgres does NOT auto-index FK columns; without these, the
-- SQL-denormalize compose query seq-scans the related table once per
-- primary row (O(N^2) bootstrap and per-event tail). With them, both
-- the 1:many items embed and the many:1 customer reverse-lookup are
-- index scans. Measured impact at 500k: bootstrap ~117 docs/s → ~24-58k
-- docs/s.
--
-- The 1:many items embed is the hot path. A COVERING index on the join
-- key (order_id) + sort key (item_id) that INCLUDEs the projected columns
-- makes the per-order items aggregation an Index-Only Scan (Heap Fetches:0),
-- which tightens per-event item-update tail latency (measured p99 40ms→4ms
-- at 3M items). Keep the projected column list in sync with the join spec's
-- `select:` for `items`.
CREATE INDEX idx_order_items_cover ON shop.order_items(order_id, item_id) INCLUDE (sku, qty, price);
CREATE INDEX idx_orders_customer_id ON shop.orders(customer_id);

-- ── seed: 5 customers ────────────────────────────────────────────────
INSERT INTO shop.customers (customer_id, name, email, tier) VALUES
  ('cust-001', 'Ada Lovelace',    'ada@example.com',    'gold'),
  ('cust-002', 'Alan Turing',     'alan@example.com',   'standard'),
  ('cust-003', 'Grace Hopper',    'grace@example.com',  'gold'),
  ('cust-004', 'Katherine Johnson','kj@example.com',    'standard'),
  ('cust-005', 'Edsger Dijkstra', 'edsger@example.com', 'platinum');

-- ── seed: 200 orders spread across the customers, each with 1-3 items ─
DO $$
DECLARE
  i int;
  cust text;
  n_items int;
  j int;
BEGIN
  FOR i IN 1..200 LOOP
    cust := 'cust-00' || (1 + (i % 5));
    INSERT INTO shop.orders (order_id, customer_id, status, total, placed_at)
    VALUES (
      'ord-' || lpad(i::text, 4, '0'),
      cust,
      (ARRAY['pending','paid','shipped','delivered'])[1 + (i % 4)],
      round((random() * 400 + 20)::numeric, 2),
      now() - (i || ' hours')::interval
    );
    n_items := 1 + (i % 3);
    FOR j IN 1..n_items LOOP
      INSERT INTO shop.order_items (item_id, order_id, sku, qty, price)
      VALUES (
        'item-' || lpad(i::text, 4, '0') || '-' || j,
        'ord-' || lpad(i::text, 4, '0'),
        'SKU-' || lpad(((i * 7 + j) % 500)::text, 4, '0'),
        1 + (j % 3),
        round((random() * 120 + 5)::numeric, 2)
      );
    END LOOP;
  END LOOP;
END $$;

-- Note: child-row deletes (removing one line item) propagate fine on the
-- DEFAULT replica identity — the engine recovers the parent order_id from
-- the row it cached when the item was inserted, so no REPLICA IDENTITY FULL
-- is needed here. See docs: connectors/sources/postgres "Child-row deletes".

-- ── publication the agent consumes ──────────────────────────────────
CREATE PUBLICATION ventstream_shop
  FOR TABLE shop.orders, shop.customers, shop.order_items;
