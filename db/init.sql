CREATE TABLE IF NOT EXISTS orders (
  id          UUID PRIMARY KEY,
  customer    TEXT NOT NULL,
  items       JSONB NOT NULL,
  kitchen_id  TEXT NOT NULL,
  status      TEXT NOT NULL,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS order_events (
  id         BIGSERIAL PRIMARY KEY,
  order_id   UUID NOT NULL REFERENCES orders(id),
  status     TEXT NOT NULL,
  at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_orders_kitchen ON orders(kitchen_id, status);
