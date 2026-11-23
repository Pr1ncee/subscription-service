---
name: outbox-checklist
description: Use when adding or changing an event type, outbox write, queue, or consumer
---

# Outbox checklist

The service guarantees that a committed state change never loses its event
(transactional outbox → relay → idempotent consumers, at-least-once). Every change to
the event pipeline must keep that guarantee. Work through each item and confirm it in
code and in a test.

1. **Event type.** Add a member to `EventType` in `domain/enums.py`. Its value is the
   routing key (e.g. `payment.succeeded`). Payload values are JSON-safe (strings, ints).
2. **Outbox row in the same UoW.** The service writes the event with
   `uow.outbox.add(EventEnvelope.new(...), aggregate_type, aggregate_id)` inside the
   **same** `UnitOfWork` as the state change. Never call a publisher from `api`/`services`.
3. **Queue and binding.** Add or extend a `QueueSpec` in `adapters/rabbit_topology.py`
   (`QUEUE_SPECS`). Both the worker and the consumers declare topology from these
   functions on startup, so queue arguments always match and events published before a
   consumer starts are kept.
4. **Idempotent consumer.** The handler class has `CONSUMER_NAME: ClassVar[str]` and
   calls `uow.inbox.try_mark_processed(CONSUMER_NAME, envelope.event_id, now)` first, in
   the same transaction as its side effects; returns early if it was already processed.
   Register it in `consumers/handlers.py::register_handlers` and wire it in
   `consumers/app.py` (the only composition root for consumers).
5. **Failure classification.** Decide what is permanent (record it, e.g. status `failed`
   with `last_error`, and ack) and what is transient (raise → transaction rolls back,
   inbox row included → `reject(requeue=True)` → retried until `x-delivery-limit`, then
   dead-lettered). Make sure `DeadLetterHandler` records the dead letter if the user can
   observe the outcome.
6. **Tests.**
   - Atomic rollback: force the outbox write to fail → no state change is committed.
   - Duplicate delivery: handle the same envelope twice → side effect applied once.
   - DLQ path: a message that keeps failing ends in a recorded, observable state.
