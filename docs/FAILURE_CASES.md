# Accepted failure cases

Rare failures the design does not recover from automatically, on purpose: covering
them would mean keeping per-ticket state in Redis that duplicates Kafka (the queue)
or PostgreSQL (the decisions). Each one can only make the sale sell **fewer** units
than it has. None of them can oversell: PostgreSQL confirms a payment only with
`UPDATE products SET sold = sold + 1 WHERE sold < total_inventory`.

Redis gates admission only (`stock:{sale}`, `user_live`, the idempotency key);
Kafka holds the queue; PostgreSQL decides every outcome.

| # | Failure | What is lost | Visible as |
|---|---|---|---|
| 1 | The Order Service crashes after `checkout.lua` and before the Kafka publish (or before compensating a failed publish) | One unit, for the rest of the sale. The user stays blocked by `user_live`: their checkout answers `409`. A retry with the same `Idempotency-Key` gets a ticket that never becomes an order. | `stock:{sale}` lower than `total_inventory - live orders` once the queue is drained; status `404` for that ticket |
| 2 | The expiration worker crashes, or Redis errors, after the PostgreSQL `UPDATE ... SET status = 'EXPIRED'` commits and before `INCR stock:{sale}` | One unit, for the rest of the sale. The user stays blocked by `user_live` although their order is `EXPIRED`. | The worker logs an error with the ticket when the Redis step fails; a crash leaves no log |
| 3 | The Order Service's write of `PENDING` to the status cache fails after a successful publish | Nothing is lost; the user cannot see their ticket's status until the processor writes the payment link | Status `404` until the processor's link write, then the normal answers (the status endpoint reads only the read cache) |
| 4 | Redis loses data (failover without the last writes, AOF loss) | Stock and one-per-user markers since the last persisted write | Not handled yet. The fix: rebuild `stock:{sale}` from PostgreSQL (`total_inventory` minus `sold` minus live orders), plus a reconciliation job |

## Why no automatic recovery

- **No retry of a stock return.** The worker cannot tell, after a crash, whether
  its `INCR` already ran. Retrying could return a unit twice: not an oversell
  (PostgreSQL refuses the extra payment), but a user who pays and is refunded. Losing
  the unit instead is the simpler and quieter failure.
- **No record of a checkout in Redis.** Detecting case 1 needs a per-ticket record
  written at checkout and compared against Kafka's progress, which is the queue's
  bookkeeping done twice.

## Manual repair

Once the sale's queue is drained (consumer lag 0) and no checkout is in flight, the
true stock is `total_inventory - sold - orders AWAITING_PAYMENT`; an operator can set
`stock:{sale}` to it and delete the `user_live` keys of users with no live order.
Not automated yet.
