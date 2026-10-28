-- The expiration worker scans only unpaid orders, oldest deadline first:
--   WHERE status = 'AWAITING_PAYMENT' AND expires_at <= now() ORDER BY expires_at
-- A partial index holds just those rows, so it stays small however many
-- orders have finished.
CREATE INDEX orders_awaiting_payment_expires_at_idx ON orders (expires_at)
    WHERE status = 'AWAITING_PAYMENT';
