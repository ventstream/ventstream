-- Demo shop: customers place orders with line items.
CREATE TABLE public.customers (
  id int PRIMARY KEY,
  name text NOT NULL,
  email text NOT NULL,
  tier text NOT NULL DEFAULT 'standard'
);
CREATE TABLE public.orders (
  id int PRIMARY KEY,
  customer_id int NOT NULL REFERENCES public.customers(id),
  status text NOT NULL,
  total_cents int NOT NULL,
  placed_at timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE public.order_items (
  id int PRIMARY KEY,
  order_id int NOT NULL REFERENCES public.orders(id) ON DELETE CASCADE,
  sku text NOT NULL,
  qty int NOT NULL,
  unit_cents int NOT NULL
);
ALTER TABLE public.customers REPLICA IDENTITY FULL;
ALTER TABLE public.orders REPLICA IDENTITY FULL;
ALTER TABLE public.order_items REPLICA IDENTITY FULL;

INSERT INTO public.customers VALUES
  (1, 'Acme Corp',   'ops@acme.example',    'enterprise'),
  (2, 'Globex',      'it@globex.example',   'standard'),
  (3, 'Initech',     'buy@initech.example', 'standard'),
  (4, 'Hooli',       'proc@hooli.example',  'enterprise');

INSERT INTO public.orders VALUES
  (100, 1, 'paid',    249900, now() - interval '2 days'),
  (101, 2, 'pending',  99000, now() - interval '1 day'),
  (102, 3, 'paid',    120000, now() - interval '20 hours'),
  (103, 1, 'shipped',  75250, now() - interval '3 hours'),
  (104, 4, 'pending', 310500, now() - interval '1 hour');

INSERT INTO public.order_items VALUES
  (1, 100, 'SRV-RACK-42U', 2, 99900),
  (2, 100, 'PDU-BASIC',    5, 10020),
  (3, 101, 'CBL-CAT6-50M', 90, 1100),
  (4, 102, 'SRV-RACK-42U', 1, 99900),
  (5, 102, 'FAN-KIT',      4,  5025),
  (6, 103, 'PDU-BASIC',    5, 10050),
  (7, 103, 'CBL-CAT6-50M', 25, 1000),
  (8, 104, 'SRV-RACK-42U', 3, 99900),
  (9, 104, 'FAN-KIT',      2,  5400);

CREATE PUBLICATION vs_demo_pub FOR ALL TABLES;
