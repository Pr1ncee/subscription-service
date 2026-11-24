# Subscription Payments & Notifications Service — Design

Date: 2026-11-23
Status: Reviewed (builder check done), awaiting requester approval
Source task: [`docs/task.md`](../../task.md)

## 1. Intent

Backend service that manages user subscriptions, records payments for them, and
reliably schedules and sends notifications. It is a senior Python developer test
task: reviewers will judge **correctness of the delivery guarantee** (a recorded
payment must never lose its event), code clarity, testability, and the AI-assisted
process (this spec, the plan, agent rules are kept in the repo as artifacts).

Stack requested by the task: FastAPI, FastStream, asyncio, PostgreSQL, RabbitMQ.

## 2. Goals, non-goals, anti-goals

**Goals**
1. `PUT` a subscription (create/update) and schedule its reminder notifications.
2. `GET` a subscription with its full notification history.
3. `POST` a payment; the payment row and its event commit atomically (transactional outbox).
4. `GET` a payment's status plus whether its event was published and its notification sent.
5. Downstream consumers: notify the user of the payment result; extend the subscription on success.
6. Runs with one `docker compose up`; tested against real Postgres and RabbitMQ.

**Non-goals**
- Authentication / authorization (assumed internal service behind a gateway; documented).
- Real payment-provider integration, currency conversion, refunds.
- Real notification channels (email/SMS/push) — a pluggable `NotificationSender` with a logging implementation.
- Exactly-once delivery (we provide at-least-once + idempotent consumers).
- Metrics/tracing stack, CD (registry publishing, deployment environments), k8s manifests (mentioned as future work). CI **is** in scope: GitHub Actions running lint, types, tests, image build and a compose smoke test.

**Anti-goals** (failure even if "it works")
- A payment committed while its event is lost — or an event published for a payment that rolled back.
- The API depending on RabbitMQ being up to accept a payment.
- Consumers whose side effects double-apply on redelivery (e.g. extending a subscription twice).
- Ceremonial architecture that obscures the guarantee.

## 3. Approaches considered

| | Approach | Trade-off | Decision |
|---|---|---|---|
| A | 3 processes: `api` (DB only), `worker` (outbox relay + notification scheduler, polling with `SKIP LOCKED`), `consumers` (FastStream) | ≤ poll-interval latency (~0.5–1 s); simplest, independently scalable, guarantee is easy to reason about | **Chosen** |
| B | A + Postgres `LISTEN/NOTIFY` to wake the relay | Lower latency; extra connection/reconnect handling, polling still needed as fallback | Future improvement |
| C | Single process (FastAPI + FastStream router + background tasks) | Least ops; API, relay, consumers fail and scale together | Rejected |

Scheduling alternatives: **DB-polled scheduler (chosen)** vs RabbitMQ delayed-message
plugin (rejected: rescheduling/cancelling is awkward, state hidden in broker).

Layering: **pragmatic layers (chosen)** — `api → services → repositories → models`;
full hexagonal with DI container rejected as too ceremonial for the scope.

## 4. Domain decisions (made by the requester)

- **`day_count`** = billing period length in days. A successful payment extends the subscription by the period **captured when the payment was recorded** (`payments.period_days`, copied from the subscription's `day_count` at that moment and carried in the event). A later `PUT` that changes `day_count` cannot change what an already-recorded payment bought.
- **Reminders** are scheduled at configurable offsets before `expected_expires_on`.
  Default `REMINDER_OFFSETS_DAYS = [3, 1, 0]` → events `subscription.expiring` (3d, 1d) and `subscription.expired` (0d).
  Offsets already in the past at scheduling time are skipped.
- **Re-upsert / extension** cancels all still-`scheduled` reminder notifications (status → `cancelled`, kept in history) and schedules new ones.
- **Payment event effects**: (a) a notification about the result to the user; (b) on `succeeded`, extend the subscription.
- **Expiry never moves backwards**: on update, `PUT` sets `expected_expires_on = max(current, requested)`, so a paid extension can't be silently undone by a stale client. Shortening a subscription is out of scope (would be an explicit endpoint).
- **Payments require an existing subscription**: unknown `subscription_id` → `404`; the caller creates the subscription first and retries.
- **Idempotency**: `provider_payment` is unique. Re-POST with identical payload → `200` with the existing payment; same `provider_payment` but different payload → `409`.
- **No auth** (non-goal, documented in README).

## 5. Architecture

```
            ┌──────────┐  tx: payment + outbox row
 client ──► │   api    │ ───────────────────────────►┌────────────┐
            └──────────┘  tx: subscription + notifs  │ PostgreSQL │
                                                     └─────┬──────┘
            ┌─────────────────────────────┐   poll (SKIP LOCKED)│
            │ worker                      │◄──────────────────┘
            │  • scheduler: due notifs →  │
            │    status=queued + outbox   │
            │  • relay: outbox → RabbitMQ │──publish (confirms)──► RabbitMQ
            └─────────────────────────────┘                       (topic exchange "events")
                                                                       │
            ┌─────────────────────────────┐◄──────────────────────────┘
            │ consumers (FastStream)      │
            │  • payment-notifier         │  inbox dedup + side effects in one DB tx
            │  • subscription-extender    │
            │  • notification-sender      │──► NotificationSender (logging impl)
            └─────────────────────────────┘
```

All three processes are entrypoints of one Python package and one Docker image.

### 5.1 Delivery guarantee (the core)

1. **Write side**: the API writes business rows and an `outbox` row in the same transaction. No broker call from the API.
2. **Relay**: loop — `SELECT … FROM outbox WHERE published_at IS NULL AND next_attempt_at <= now() ORDER BY created_at LIMIT :batch FOR UPDATE SKIP LOCKED`; publish each with publisher confirms, persistent delivery mode, `message_id = outbox.id`; set `published_at`. On publish failure: `attempts += 1`, `last_error`, `next_attempt_at = now() + backoff(attempts)` (exponential, capped). Rows are never dropped.
3. **At-least-once**: a crash between publish-confirm and commit re-publishes → consumers must dedup.
4. **Consumers**: each handler opens a DB tx, `INSERT INTO processed_events(consumer, event_id) ON CONFLICT DO NOTHING`; if no row inserted → ack and skip; else apply side effects and commit; ack after commit. Unhandled error → **`basic.reject(requeue=True)`** — not `basic.nack`. Since RabbitMQ 4.3, requeued nacks no longer count toward the quorum-queue delivery limit; reject counts on every version. After `x-delivery-limit` (default 5) the message is dead-lettered to `events.dlq`. Dead-lettering uses the quorum `at-least-once` strategy, so a message can't be lost on its way to the DLQ.
5. **Ordering**: not guaranteed across relay instances; consumers do not rely on it (extension and notification creation are per-event idempotent and commutative enough — see 7.3).

### 5.2 RabbitMQ topology (declared idempotently by both worker and consumers on startup — so events published before any consumer ran are routed, not dropped; each process connects first, then declares)

- Exchange `events` (topic, durable). DLX `events.dlx` (fanout) → queue `events.dlq`.
- Queues (quorum, durable, `x-delivery-limit=5`, `x-dead-letter-exchange=events.dlx`, `x-dead-letter-strategy=at-least-once`, `x-overflow=reject-publish` — the overflow setting is required by the at-least-once strategy):
  - `payments.notify` ← `payment.*`
  - `payments.extend-subscription` ← `payment.succeeded`
  - `notifications.send` ← `notification.due`
- `events.dlq`: quorum, durable, `x-delivery-limit=-1`. The limit is switched off on purpose, because quorum queues in 4.x default to a limit of 20, and a dead letter with no DLX would be silently dropped after 20 failed handling attempts.
- **dlq-handler** consumer on `events.dlq`: logs every dead-lettered event; for `notification.due` it marks the notification `failed` with `last_error` (idempotent: only from `queued`). It acks **only after that DB write commits**. On any error (e.g. Postgres down) it waits briefly and `reject(requeue=True)`s, so a dead letter is retried until it is recorded and never discarded. This ensures no notification sits in `queued` forever and endpoint 4 reports the failure. Replay tooling is future work.
- RabbitMQ version is pinned to the same exact `4.3.x` image in compose and in testcontainers.
- Message: JSON envelope `{event_id, event_type, occurred_at, payload}`; `message_id = event_id`.

## 6. Data model (PostgreSQL)

All timestamps `timestamptz`, UTC.

**subscriptions**
| column | type | notes |
|---|---|---|
| subscription_id | text PK | client-supplied |
| user_id | text not null | immutable after create (mismatch → 409) |
| day_count | int not null, > 0 | billing period, days |
| expected_expires_on | timestamptz not null | |
| created_at, updated_at | timestamptz | |

**notifications**
| column | type | notes |
|---|---|---|
| id | uuid PK | |
| subscription_id | text FK | |
| payment_id | uuid FK null | set for payment-result notifications |
| event_name | text | `subscription.expiring`, `subscription.expired`, `payment.succeeded`, `payment.failed` |
| scheduled_for | timestamptz | |
| target_expires_on | timestamptz null | for reminders: the expiry they were computed from (stale-check in 7.5) |
| status | text | `scheduled → queued → sent`; `scheduled → cancelled`; `queued → cancelled` (stale reminder); `queued → failed` |
| last_error | text null | reason for `failed` |
| sent_at | timestamptz null | |
| created_at, updated_at | timestamptz | |

Indexes: partial `(scheduled_for) WHERE status='scheduled'`; `(subscription_id, scheduled_for)`; unique `(payment_id) WHERE payment_id IS NOT NULL` (one result notification per payment).

**payments**
| column | type | notes |
|---|---|---|
| id | uuid PK | |
| subscription_id | text FK | |
| provider_payment | text UNIQUE | idempotency key |
| amount | numeric(19,4), > 0 | |
| currency | char(3) | ISO-4217 uppercase |
| status | text | `succeeded` / `failed` |
| period_days | int not null, > 0 | snapshot of the subscription's `day_count` at record time — what this payment buys |
| created_at | timestamptz | |

**outbox**
| column | type | notes |
|---|---|---|
| id | uuid PK | = event_id = AMQP message_id |
| aggregate_type, aggregate_id | text | e.g. `payment`, `<uuid>` — used by endpoint 4 |
| event_type / routing_key | text | `payment.succeeded`, `payment.failed`, `notification.due` |
| payload | jsonb | |
| created_at | timestamptz | |
| published_at | timestamptz null | |
| attempts | int default 0 | |
| next_attempt_at | timestamptz default now() | |
| last_error | text null | |

Index: partial `(next_attempt_at, created_at) WHERE published_at IS NULL`.

**processed_events** (inbox): `(consumer text, event_id uuid)` PK, `processed_at`.

Migrations via Alembic; `api` container runs `alembic upgrade head` on start (or a one-shot `migrate` service in compose — builder's call).

## 7. Flows

### 7.1 `PUT /subscriptions/{subscription_id}`
Body: `{user_id, day_count, expected_expires_on}` (tz-aware datetime required; naive → 422).
One tx: `SELECT … FOR UPDATE` existing; if exists and `user_id` differs → 409. Upsert; on update `expected_expires_on = max(current, requested)` and `day_count` is overwritten. If the effective expiry is unchanged, reminders are left as they are; otherwise cancel `scheduled` reminder notifications (not payment ones). Insert reminders for each offset whose time is in the future.
Response: `201` created / `200` updated, body = same as GET.

### 7.2 `GET /subscriptions/{subscription_id}`
Response: `{subscription_id, user_id, day_count, expected_expires_on, notifications: [{id, event_name, scheduled_for, sent_at|null, status}]}` ordered by `scheduled_for`. 404 if unknown.

### 7.3 `POST /payments`
Body: `{subscription_id, provider_payment, amount, currency, status: "succeeded"|"failed"}`.
One tx: subscription must exist (404). `INSERT … ON CONFLICT (provider_payment) DO NOTHING RETURNING`; on conflict load existing → identical → `200` existing; differs → `409`. On insert: insert outbox `payment.<status>` with payload `{payment_id, subscription_id, provider_payment, amount, currency, status}`. Commit → `201`.

Consumers:
- **payment-notifier** (`payment.*`): inbox dedup; insert notification `{event_name=payment.<status>, payment_id, scheduled_for=now(), status=scheduled}` (unique on payment_id makes it idempotent too). The scheduler then picks it up like any other — one sending path.
- **subscription-extender** (`payment.succeeded`): inbox dedup; `SELECT subscription FOR UPDATE`; `expected_expires_on = max(expected_expires_on, now()) + payload.period_days days`. It does **not** use the subscription's current `day_count`. Then it cancels pending reminders and schedules new ones (same service function as 7.1).

The payment event payload is `{payment_id, subscription_id, provider_payment, amount, currency, status, period_days}`. The identical-duplicate check in `POST /payments` ignores `period_days`, because the client does not send it.

### 7.4 Scheduler (worker)
Loop: `SELECT notifications WHERE status='scheduled' AND scheduled_for <= now() ORDER BY scheduled_for LIMIT :batch FOR UPDATE SKIP LOCKED`; in the same tx set `status='queued'` and insert outbox `notification.due {notification_id}`.

### 7.5 notification-sender (consumer)
**Lock order is subscription → notification everywhere.** Here that means: inbox dedup; `SELECT subscription FOR SHARE`, then `SELECT notification FOR UPDATE`. The extender holds the subscription `FOR UPDATE`, so it cannot commit an extension between this handler's expiry check and its send. It waits, and the reminder is judged against a consistent expiry. The scheduler never locks subscriptions, so no lock cycle exists. If status ≠ `queued` → skip (already sent/cancelled). For reminders, if `target_expires_on` ≠ subscription's current `expected_expires_on` → mark `cancelled` (stale: subscription was extended after queuing) and do not send. Call `NotificationSender.send(notification, subscription)`; set `status='sent'`, `sent_at=now()`; commit. If sender raises a permanent error → `failed`; transient → raise (redelivery). Crash after send but before commit → possible duplicate send (documented at-least-once).

### 7.6 `GET /payments/{payment_id}` (and `GET /payments?provider_payment=…`)
Response:
```json
{
  "payment_id": "...", "subscription_id": "...", "provider_payment": "...",
  "amount": "10.00", "currency": "USD", "status": "succeeded", "created_at": "...",
  "event": {"event_id": "...", "event_type": "payment.succeeded",
            "published": true, "published_at": "...", "attempts": 1, "last_error": null},
  "notification": {"status": "sent", "sent_at": "..."}   // null if not yet created
}
```

### 7.7 Ops endpoints
`GET /health/live` (process up), `GET /health/ready` (DB reachable). API readiness does **not** depend on RabbitMQ (by design).

## 8. Code layout

```
src/subscriptions_service/
  config.py                # pydantic-settings
  db/ (engine, session, models.py, migrations/)
  domain/                  # enums, small pure functions (reminder schedule calc, extension calc)
  repositories/            # subscriptions, notifications, payments, outbox, inbox
  services/                # use cases; own the transaction (UoW); ports.py = Protocols (Clock, EventPublisher, NotificationSender)
  adapters/                # SystemClock, LoggingSender, RabbitPublisher, rabbit_topology
  api/ (app.py, deps.py, routers/, schemas.py, errors.py)   # deps.py = composition root
  worker/ (main.py, loop.py)                                # main.py = composition root
  consumers/ (app.py, handlers.py)                          # app.py = composition root
tests/ unit/ integration/ e2e/
docker-compose.yml, Dockerfile, pyproject.toml (uv), alembic.ini, README.md, CLAUDE.md
```

Dependency rule: `api/worker/consumers → adapters → services → repositories → db → domain`; `domain` depends on nothing. Enforced by import-linter contracts in `pyproject.toml`.

## 9. Tooling (builder's calls already made)
Python 3.12, `uv`, FastAPI, FastStream[rabbit], SQLAlchemy 2.0 async + asyncpg, Alembic,
Pydantic v2 + pydantic-settings, pytest + pytest-asyncio + testcontainers (Postgres, RabbitMQ),
httpx `AsyncClient`, black (formatter), ruff (linter only), mypy (strict on `src/`), import-linter, pre-commit, `make` targets shared by local dev and CI, structured logging via stdlib `logging` JSON formatter.
Delivery tooling: multi-stage non-root Docker image (pinned bases), docker compose with resource limits, GitHub Actions CI (`quality` / `test` / `docker` jobs), hadolint, actionlint, pip-audit, Dependabot.

## 10. Testing strategy
- **Unit**: reminder-schedule calculation, extension calculation, envelope serialization.
- **Integration (real Postgres)**: each service; payment idempotency (200/409); atomicity — force failure on outbox insert → no payment row; scheduler `SKIP LOCKED` with two concurrent workers doesn't double-queue; re-upsert cancels pending.
- **Integration (Postgres + RabbitMQ)**: relay publishes and marks; **broker down → POST /payments still 201, outbox pending; broker up → published**; consumer receives duplicate message → side effect applied once; stale queued reminder after extension → `cancelled`, not sent; notification message exceeding delivery limit → notification `failed`.
- **Rules**: PUT with older expiry after an extension keeps the later expiry; payment for unknown subscription → 404; a PUT changing `day_count` after a payment was recorded doesn't change that payment's extension.
- **Concurrency**: interleaved test where the sender is blocked mid-send while an extension is attempted — the extension must wait for the delivery tx.
- **Broker semantics**: delivery-limit exhaustion on the pinned RabbitMQ 4.3.x; DLQ handler with Postgres unavailable keeps the dead letter and records it after recovery.
- **Priority if time runs short**: guarantee-critical tests first; the compose E2E test and the `outbox-checklist` skill are the first to trim.
- **E2E (compose-level, one happy path)**: PUT subscription → POST payment succeeded → poll GET payment until `event.published` and `notification.status=sent`, and subscription extended.

## 11. AI-process artifacts (deliverable)
- `docs/superpowers/specs/` — this design; `docs/superpowers/plans/` — implementation plan.
- `CLAUDE.md` — project rules for agents (layering rule, outbox rule "never publish from API/services directly", how to run tests, commit conventions).
- `.claude/skills/` — existing `clean-architecture` review skill; plus a small project skill `outbox-checklist` (checklist for adding a new event type: outbox row in same tx, routing key, queue binding, idempotent consumer, test).
- README section "How this was built" pointing to these.

## 12. Configuration (env)
`DATABASE_URL`, `RABBITMQ_URL`, `REMINDER_OFFSETS_DAYS` (default `3,1,0`), `RELAY_POLL_INTERVAL_S` (0.5), `RELAY_BATCH_SIZE` (100), `SCHEDULER_POLL_INTERVAL_S` (1.0), `SCHEDULER_BATCH_SIZE` (100), `RELAY_MAX_BACKOFF_S` (60), `CONSUMER_DELIVERY_LIMIT` (5), `LOG_LEVEL`.

## 13. Left to the builder
- Exact backoff formula, batch sizes, logging fields.
- Whether migrations run in the api entrypoint or a one-shot compose service.
- Pagination of notification history (not required; histories are small).
- Precise module/file names within the layout above.

## 14. Future work (README)
LISTEN/NOTIFY relay wakeup; outbox cleanup/partitioning; Prometheus metrics (outbox lag, DLQ depth); OpenTelemetry tracing across outbox→consumer; DLQ replay tooling; auth.
