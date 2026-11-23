# Plan 2 of 3 — Worker (Scheduler + Outbox Relay) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A `worker` process that turns due notifications into outbox events and reliably publishes every outbox event to RabbitMQ at-least-once, surviving broker outages.

**Architecture:** Two polling loops in one asyncio process. Both use `FOR UPDATE SKIP LOCKED`, so multiple workers can run side by side. The relay depends on an `EventPublisher` Protocol (port). The RabbitMQ implementation and topology live in `adapters/`, and they are wired together only in `worker/main.py` (composition root). The worker declares the **full** topology (exchange, queues, bindings) itself, so events published before any consumer has started are still routed and kept.

**Tech Stack:** as Plan 1 + FastStream `RabbitBroker` (aio-pika underneath) for publishing.

**Spec:** `docs/superpowers/specs/2026-11-23-subscription-payments-design.md` (§5.1, §5.2, §7.4)

**Prerequisite:** Plan 1 complete (`2026-11-23-plan-1-foundation-and-write-side.md`); its Global Constraints and skill conventions apply here unchanged.

## Global Constraints

- Everything in Plan 1 → Global Constraints and "Conventions from the skills".
- Exchange `events` (topic, durable); DLX `events.dlx` (fanout, durable) → queue `events.dlq`.
- Queues (quorum, durable, `x-delivery-limit=CONSUMER_DELIVERY_LIMIT`, `x-dead-letter-exchange=events.dlx`, `x-dead-letter-strategy=at-least-once`, `x-overflow=reject-publish`): `payments.notify` ← `payment.*`; `payments.extend-subscription` ← `payment.succeeded`; `notifications.send` ← `notification.due`. RabbitMQ's default dead-letter transfer is at-most-once; the `at-least-once` strategy (which requires `reject-publish` overflow) keeps a message in the source queue until the DLQ has confirmed it.
- `events.dlq`: quorum, durable, `x-delivery-limit=-1` (unlimited). Quorum queues default to 20 in 4.x, and a DLQ message dropped past that limit would be lost for good.
- RabbitMQ image pinned to one exact `4.3.x` tag (`RABBITMQ_IMAGE` constant in `tests/conftest.py`, same tag in compose). Tests and compose must run the same broker version, because delivery-limit semantics changed in 4.3.
- Every process **connects before declaring** topology (declarations need an open channel).
- Message: body = `EventEnvelope.to_json()`, `message_id = str(event_id)`, `routing_key = event_type`, persistent, `content_type="application/json"`, publisher confirms on, `mandatory=True`.
- Relay: `attempts += 1` on every try. Failure sets `last_error` and `next_attempt_at = now + backoff_delay(attempts)`, where `backoff_delay = min(max_backoff_s, 0.5 * 2 ** (attempts - 1))` seconds. Rows are never deleted.

## Review Focus

1. **Relay publishes before consumers ever started** → still routed and retained (Task 10: `test_event_retained_without_consumers`).
2. **Broker paused while the worker runs** → publish fails within `PUBLISH_TIMEOUT_S`, the row stays pending, and it publishes after recovery without restarting the worker (Task 10: `test_publish_recovers_after_broker_pause`).
3. **One poison event keeps failing** → other events are still published (Task 9: `test_one_failing_event_does_not_block_others`).
4. **Two workers run at once** → no notification is queued twice (Task 8: `test_concurrent_schedulers_do_not_double_queue`).
5. **A loop step raises (e.g. the DB is briefly unavailable)** → the loop logs it and keeps running (Task 10: `test_run_periodic_survives_step_exception`).

---

### Task 8: Notification scheduler

**Files:**
- Create: `src/subscriptions_service/services/scheduler.py`
- Test: `tests/integration/test_scheduler.py`

**Interfaces:**
- Consumes: `NotificationRepository.lock_due`, `OutboxRepository.add`, `EventEnvelope.new`, `SubscriptionService` (tests only).
- Produces: `NotificationScheduler(uow_factory: UnitOfWorkFactory, clock: Clock, batch_size: int)` with `async run_once() -> int`. In one tx: lock due rows, set `status=queued`, and for each row add an outbox `notification.due` with payload `{"notification_id": str(id)}`, `aggregate_type="notification"`, `aggregate_id=str(id)`.

- [ ] **Step 1: Write failing tests**

```python
async def test_queues_due_reminder_and_writes_outbox(sched, subs, uow_factory, clock):
    await subs.upsert("s1", "u1", 30, clock.now() + 10 * D)
    clock.advance(7 * D)                       # 3-day reminder is now due
    assert await sched.run_once() == 1
    # notification status == "queued"; exactly one outbox row event_type "notification.due",
    # payload == {"notification_id": <that id>}

async def test_nothing_due_returns_zero(sched, subs, clock): ...
async def test_respects_batch_size(uow_factory, clock): ...   # 5 due, batch 2 -> 2, 2, 1, 0

async def test_concurrent_schedulers_do_not_double_queue(uow_factory, clock):
    # 50 due notifications; two NotificationScheduler(batch_size=30) run via asyncio.gather twice
    # sum of returns == 50; outbox has 50 notification.due rows with 50 distinct notification_ids
```

- [ ] **Step 2: Run — expect FAIL** — `uv run pytest tests/integration/test_scheduler.py -v`
- [ ] **Step 3: Implement `NotificationScheduler`**
- [ ] **Step 4: Run — expect PASS**
- [ ] **Step 5: Commit** — `feat(services): schedule due notifications into the outbox`

---

### Task 9: Outbox relay (port + service)

**Files:**
- Modify: `services/ports.py` (add `EventPublisher` Protocol: `async publish(envelope: EventEnvelope) -> None`, raises on any non-confirmed publish)
- Create: `services/relay.py`
- Modify: `tests/fakes.py` (add `RecordingPublisher` with `.published: list[EventEnvelope]`; `FailingPublisher(fail_types: set[EventType] | None = None, error="boom")`, which fails on all types when `None`)
- Test: `tests/unit/test_backoff.py`, `tests/integration/test_relay.py`

**Interfaces:**
- Produces: `backoff_delay(attempts: int, max_backoff_s: float) -> timedelta`; `OutboxRelay(uow_factory: UnitOfWorkFactory, publisher: EventPublisher, clock: Clock, batch_size: int, max_backoff_s: float)` with `async run_once() -> int` (number published). It rebuilds the `EventEnvelope` from the row (`id`, `event_type`, `created_at` as `occurred_at`, `payload`), publishes rows one by one in `created_at` order, and marks each row individually.

- [ ] **Step 1: Write failing tests**

```python
@pytest.mark.parametrize(("attempts", "seconds"), [(1, 0.5), (2, 1.0), (3, 2.0), (10, 60.0)])
def test_backoff_delay(attempts, seconds):
    assert backoff_delay(attempts, 60) == timedelta(seconds=seconds)

async def test_publishes_pending_and_marks_published(relay_factory, psvc, clock):
    pay = await psvc.record(CMD)
    pub = RecordingPublisher(); relay = relay_factory(pub)
    assert await relay.run_once() == 1
    assert pub.published[0].event_type == "payment.succeeded"
    status = await psvc.get_status(pay.view.payment_id)
    assert status.event.published and status.event.attempts == 1
    assert pub.published[0].event_id == status.event.event_id
    assert await relay.run_once() == 0           # not republished

async def test_failure_records_attempt_and_backs_off(relay_factory, psvc, clock):
    pay = await psvc.record(CMD)
    assert await relay_factory(FailingPublisher()).run_once() == 0
    ev = (await psvc.get_status(pay.view.payment_id)).event
    assert (ev.published, ev.attempts) == (False, 1) and "boom" in ev.last_error
    pub = RecordingPublisher()
    assert await relay_factory(pub).run_once() == 0            # not yet due (next_attempt_at = now + 0.5s)
    clock.advance(timedelta(seconds=1))
    assert await relay_factory(pub).run_once() == 1

async def test_one_failing_event_does_not_block_others(...):
    # one payment.failed + one payment.succeeded; FailingPublisher(fail_types={PAYMENT_FAILED}) -> returns 1
async def test_publishes_in_created_at_order(...): ...

async def test_commit_failure_after_publish_republishes_same_event_id(session_factory, psvc, clock):
    # crash window: broker confirmed, DB commit of published_at lost
    pay = await psvc.record(CMD)
    pub = RecordingPublisher()

    class CommitFailsOnce(UnitOfWork):
        async def commit(self) -> None:
            raise OperationalError("commit", {}, Exception("connection lost"))

    crashing = OutboxRelay(lambda: CommitFailsOnce(session_factory), pub, clock, batch_size=10, max_backoff_s=60)
    with pytest.raises(OperationalError):
        await crashing.run_once()
    assert len(pub.published) == 1                                  # it did reach the broker
    ev = (await psvc.get_status(pay.view.payment_id)).event
    assert (ev.published, ev.attempts) == (False, 0)                # marking was rolled back; row still pending

    healthy = OutboxRelay(lambda: UnitOfWork(session_factory), pub, clock, batch_size=10, max_backoff_s=60)
    assert await healthy.run_once() == 1
    assert len(pub.published) == 2
    assert pub.published[0].event_id == pub.published[1].event_id == ev.event_id   # same id -> consumers can dedup
    assert pub.published[0] == pub.published[1]                     # byte-identical envelope (same occurred_at, payload)
    assert (await psvc.get_status(pay.view.payment_id)).event.published is True
```

This test pins the at-least-once contract on the producer side. A duplicate is allowed, but it must carry the **same** `event_id`, which is what makes the consumers' inbox dedup work. If an implementation minted a fresh id on retry, or let the commit error escape per-row handling and silently drop the row, this test fails. Note that the commit error propagates out of `run_once()`: it is a whole-batch failure, not a per-row publish failure. `run_periodic` (Task 10) logs it and retries on the next tick.

- [ ] **Step 2: Run — expect FAIL**
- [ ] **Step 3: Implement `backoff_delay` and `OutboxRelay`**. Catch `Exception` per row (not bare `except`), log it with `event_id`, and keep going.
- [ ] **Step 4: Run — expect PASS**; `uv run lint-imports` (relay imports no broker code)
- [ ] **Step 5: Commit** — `feat(services): add outbox relay with exponential backoff`

---

### Task 10: RabbitMQ adapters & worker process

**Files:**
- Create: `adapters/rabbit_topology.py`, `adapters/rabbit_publisher.py`, `worker/loop.py`, `worker/main.py`, `worker/__main__.py`
- Modify: `tests/conftest.py`. Add `rabbit_url` (session-scoped `RabbitMqContainer(RABBITMQ_IMAGE)` with `RABBITMQ_IMAGE = "rabbitmq:4.3.<patch>-management-alpine"`. Pin the exact patch tag current when you write it, and use the same tag in compose. Started lazily.) and `rabbit_broker` (function-scoped: `RabbitBroker(rabbit_url)` connected, all project queues deleted on teardown so different `x-delivery-limit` values don't collide).
- Test: `tests/unit/test_loop.py`, `tests/integration/test_rabbit_publisher.py`

**Interfaces:**
- Produces:
  - `rabbit_topology.py`: `EVENTS_EXCHANGE: RabbitExchange`, `DEAD_LETTER_EXCHANGE: RabbitExchange`, `QueueSpec(name: str, routing_keys: tuple[str, ...])`, `QUEUE_SPECS: tuple[QueueSpec, ...]` (the three from Global Constraints), `DLQ_NAME = "events.dlq"`, `rabbit_queue(spec: QueueSpec, delivery_limit: int) -> RabbitQueue` (quorum, durable, the five arguments from Global Constraints), `dead_letter_queue() -> RabbitQueue` (quorum, durable, `x-delivery-limit=-1`). The same functions are used by the consumers' subscribers, so the arguments always match and a redeclare never fails with `PRECONDITION_FAILED`. `async declare_topology(broker: RabbitBroker, delivery_limit: int) -> None`. It declares both exchanges and all queues, then binds each queue with `await (await broker.declare_queue(q)).bind(exchange, routing_key)`. Idempotent.
  - `RabbitPublisher(broker: RabbitBroker, timeout_s: float)` implements `EventPublisher`. It wraps `broker.publish(..., exchange=EVENTS_EXCHANGE, mandatory=True, persist=True, message_id=..., timeout=...)` in `asyncio.timeout(timeout_s)`, and an unroutable return raises.
  - `run_periodic(step: Callable[[], Awaitable[int]], *, interval_s: float, name: str, stop: asyncio.Event) -> None`. If `step()` returns > 0 it calls again immediately; otherwise it waits `interval_s` or until `stop` is set. Exceptions are logged with `logger.exception` and the loop continues.
  - `run_worker(settings: Settings) -> None` (composition root). It builds the engine, session factory, `SystemClock` and `RabbitBroker(settings.rabbitmq_url)`, connects, calls `declare_topology`, then runs scheduler and relay `run_periodic` in an `asyncio.TaskGroup`. SIGTERM/SIGINT set `stop`. `__main__.py`: `asyncio.run(run_worker(Settings()))`.
  - Startup when the broker is unreachable: the process exits non-zero and compose `restart: unless-stopped` brings it back (Task 13). Document this in the README.

- [ ] **Step 1: Write failing tests**

```python
# tests/unit/test_loop.py
async def test_run_periodic_survives_step_exception():
    calls = 0; stop = asyncio.Event()
    async def step():
        nonlocal calls; calls += 1
        if calls == 1: raise RuntimeError("db blip")
        if calls == 3: stop.set()
        return 0
    await asyncio.wait_for(run_periodic(step, interval_s=0.01, name="t", stop=stop), 1)
    assert calls == 3

async def test_run_periodic_repeats_immediately_while_work_remains(): ...  # returns 5,5,0 -> 3 calls before first sleep

# tests/integration/test_rabbit_publisher.py
async def test_event_retained_without_consumers(rabbit_broker):          # Review Focus 1
    await declare_topology(rabbit_broker, delivery_limit=5)
    env = EventEnvelope.new(EventType.PAYMENT_SUCCEEDED, {"payment_id": "p1"}, NOW)
    await RabbitPublisher(rabbit_broker, timeout_s=5).publish(env)
    for name in ("payments.notify", "payments.extend-subscription"):
        msg = await (await rabbit_broker.declare_queue(RabbitQueue(name, passive=True))).get(timeout=5)
        assert msg.message_id == str(env.event_id) and EventEnvelope.from_json(msg.body) == env

async def test_queue_arguments_declared(rabbit_broker):
    await declare_topology(rabbit_broker, delivery_limit=5)
    # via the management HTTP API (container port 15672) GET /api/queues/%2F/notifications.send:
    # arguments include x-queue-type=quorum, x-delivery-limit=5, x-dead-letter-strategy=at-least-once,
    # x-overflow=reject-publish; events.dlq has x-delivery-limit=-1

async def test_unroutable_publish_raises(rabbit_broker):
    # declare only EVENTS_EXCHANGE (no queues) -> publish raises

@pytest.mark.slow
async def test_publish_recovers_after_broker_pause(rabbit_broker, rabbit_container):  # Review Focus 2
    await declare_topology(rabbit_broker, 5); pub = RabbitPublisher(rabbit_broker, timeout_s=2)
    rabbit_container.get_wrapped_container().pause()
    with pytest.raises(Exception): await pub.publish(env)
    rabbit_container.get_wrapped_container().unpause()
    await pub.publish(env)   # same broker object; robust connection recovered
```

- [ ] **Step 2: Run — expect FAIL** — `uv run pytest tests/unit/test_loop.py tests/integration/test_rabbit_publisher.py -v`
- [ ] **Step 3: Implement topology, publisher, loop, worker main.** Check against the installed FastStream version that unroutable `mandatory` returns raise. If the broker option is named differently (e.g. `on_return_raises`), set it in `run_worker` and in the `rabbit_broker` fixture.
- [ ] **Step 4: Run — expect PASS** (the `slow` test included: `uv run pytest -m "slow or not slow" tests/integration/test_rabbit_publisher.py -v`)
- [ ] **Step 5: Gates** — `make lint typecheck`
- [ ] **Step 6: Commit** — `feat(worker): add RabbitMQ publisher, topology and worker process`
