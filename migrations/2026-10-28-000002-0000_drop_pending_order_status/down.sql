-- Brings PENDING back as an (unused) value, with the default and the index
-- predicate of the first migration.

DROP INDEX orders_one_live_per_user_product_idx;

CREATE TYPE order_status_old AS ENUM (
    'PENDING', 'AWAITING_PAYMENT', 'CONFIRMED', 'EXPIRED', 'FAILED', 'REFUNDED'
);

ALTER TABLE orders ALTER COLUMN status TYPE order_status_old USING status::text::order_status_old;
ALTER TABLE orders ALTER COLUMN status SET DEFAULT 'PENDING';

DROP TYPE order_status;
ALTER TYPE order_status_old RENAME TO order_status;

CREATE UNIQUE INDEX orders_one_live_per_user_product_idx ON orders (user_id, product_id)
    WHERE status IN ('PENDING', 'AWAITING_PAYMENT', 'CONFIRMED');
