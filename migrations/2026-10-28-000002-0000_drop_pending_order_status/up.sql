-- PENDING is never stored: the processor inserts an order only once its
-- payment session exists, directly as AWAITING_PAYMENT. The client-visible
-- PENDING (waiting in Kafka) lives in the hold and the read cache.
--
-- PostgreSQL cannot drop an enum value, so the column moves to a new type.

-- An order that was never linked belongs to no payment: it failed
UPDATE orders SET status = 'FAILED' WHERE status = 'PENDING';

-- Its predicate names the old type's values
DROP INDEX orders_one_live_per_user_product_idx;

-- The processor always sets the status
ALTER TABLE orders ALTER COLUMN status DROP DEFAULT;

CREATE TYPE order_status_new AS ENUM (
    'AWAITING_PAYMENT', 'CONFIRMED', 'EXPIRED', 'FAILED', 'REFUNDED'
);

ALTER TABLE orders ALTER COLUMN status TYPE order_status_new USING status::text::order_status_new;

DROP TYPE order_status;
ALTER TYPE order_status_new RENAME TO order_status;

-- One live order per user and product, whatever the Redis markers say
CREATE UNIQUE INDEX orders_one_live_per_user_product_idx ON orders (user_id, product_id)
    WHERE status IN ('AWAITING_PAYMENT', 'CONFIRMED');
