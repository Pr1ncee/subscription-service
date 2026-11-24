# subscriptions-service

[![CI](../../actions/workflows/ci.yml/badge.svg)](../../actions/workflows/ci.yml)

Backend for user subscriptions: create/update a subscription and schedule its reminder
notifications, record payments for it, and reliably deliver payment-result notifications
and subscription extensions. **A recorded payment never loses its event.**

Stack: Python 3.12 · FastAPI · FastStream · asyncio · PostgreSQL 16 · RabbitMQ 4.3 · SQLAlchemy 2 (async) · Alembic · uv.

## Quick start

```bash
docker compose up -d --build --wait      # postgres, rabbitmq, migrate, api, worker, consumers
```

```bash
# 1. Create or update a subscription (201 created, 200 updated)
curl -s -X PUT localhost:8000/subscriptions/sub-1 -H 'content-type: application/json' \
  -d '{"user_id": "user-1", "day_count": 30, "expected_expires_on": "2030-01-01T00:00:00Z"}'

# 2. Subscription state + notification history (200)
curl -s localhost:8000/subscriptions/sub-1

# 3. Record a payment (201 created; identical retry 200; same provider id, different payload 409)
curl -s -X POST localhost:8000/payments -H 'content-type: application/json' \
  -d '{"subscription_id": "sub-1", "provider_payment": "pp-1", "amount": "9.99", "currency": "USD", "status": "succeeded"}'

# 4. Payment status + whether its event was published and its notification sent (200)
curl -s 'localhost:8000/payments?provider_payment=pp-1'      # or /payments/{payment_id}
```

OpenAPI docs: <http://localhost:8000/docs>. RabbitMQ UI: <http://localhost:15672> (`app` / `app`).
Stop with `docker compose down -v`.

## API

| Method & path | Purpose | Responses |
|---|---|---|
| `PUT /subscriptions/{subscription_id}` | Create/update a subscription, (re)schedule reminders | `201` created, `200` updated, `409` other user, `422` invalid (e.g. naive datetime) |
| `GET /subscriptions/{subscription_id}` | State + notification history (`event_name`, `scheduled_for`, `sent_at`, `status`) | `200`, `404` |
| `POST /payments` | Record a payment and its event atomically | `201`, `200` identical retry, `404` unknown subscription, `409` conflicting duplicate, `422` |
| `GET /payments/{payment_id}` | Payment + event delivery (`published`, `attempts`, `last_error`) + notification | `200`, `404` |
| `GET /payments?provider_payment=…` | Same, by the provider's payment id | `200`, `404` |
| `GET /health/live` | Process is up | `200` |
| `GET /health/ready` | Database reachable (RabbitMQ deliberately not checked) | `200`, `503` |

Request bodies are limited to 64 KiB (`413`). Text fields reject NUL bytes; expiries must be
timezone-aware and between 1970 and 9000.

## Delivery guarantee

```
api ──tx: payment + outbox row──► PostgreSQL ◄──poll (SKIP LOCKED)── worker ──publish (confirms)──► RabbitMQ ──► consumers
```

1. **Transactional outbox.** `POST /payments` writes the payment and an `outbox` row in one
   transaction. The API never talks to RabbitMQ, so it accepts payments while the broker is down.
2. **Relay (worker).** Polls pending outbox rows with `FOR UPDATE SKIP LOCKED`, publishes each as a
   persistent, mandatory message with publisher confirms (`message_id = event_id`), and marks it
   published. A failure increments `attempts`, stores `last_error` and backs off exponentially
   (0.5 s … 60 s). Rows are never dropped. If the broker is unreachable the batch stops at the first
   failure, so an outage costs one publish timeout per poll, not one per event.
3. **At-least-once.** A crash between the broker's confirm and the DB commit republishes the same
   event (same `event_id`).
4. **Idempotent consumers (inbox).** Each consumer records `(consumer, event_id)` in
   `processed_events` in the same transaction as its side effects; a redelivery is acked and skipped.
   A failed handler rolls back everything (inbox row included). Failures are classified:
   - **database unreachable** — the consumer waits (exponential backoff, ≤ 5 s) and retries the
     same delivery without spending the delivery limit, so a brief DB outage never drops an event;
   - **malformed envelope or payload** — poison: dead-lettered immediately;
   - **anything else** (e.g. the sender failed) — backoff by delivery count, then
     `reject(requeue=True)`; after `CONSUMER_DELIVERY_LIMIT` attempts the message is dead-lettered to
     `events.dlq`, where the DLQ handler marks the notification `failed`.

**What can duplicate:** only the external notification send — a crash after the sender returned but
before the commit sends it again. Subscription extension and notification creation never double-apply.

## Design decisions

Original assignment (in Russian): [`docs/task.md`](docs/task.md). The full design is in [`docs/superpowers/specs/2026-11-23-subscription-payments-design.md`](docs/superpowers/specs/2026-11-23-subscription-payments-design.md).
Domain calls made by the requester:

- `day_count` is the billing period. A successful payment extends the subscription by the period
  **snapshotted onto the payment when it was recorded** (`payments.period_days`): a later `PUT` cannot
  change what an already-recorded payment bought.
- Reminders at `REMINDER_OFFSETS_DAYS` (default `3,1,0`) before expiry: `subscription.expiring`, and
  `subscription.expired` at 0. Slots already in the past are skipped.
- **Expiry never moves backwards:** `PUT` sets `expected_expires_on = max(current, requested)`, so a
  stale client cannot undo a paid extension.
- A reminder queued before the user paid is **cancelled, not sent**, if the expiry changed
  (lock order subscription → notification makes this race-free).
- Payments require an existing subscription (`404` otherwise); `provider_payment` is the idempotency key.

## Operations

- **Processes** — one image, four roles (compose services): `api` (FastAPI), `worker`
  (`python -m subscriptions_service.worker`: scheduler + outbox relay), `consumers`
  (`python -m subscriptions_service.consumers`: FastStream subscribers + DLQ handler) and the
  one-shot `migrate` (`alembic upgrade head`).
- **Health** — `/health/live`, `/health/ready` (DB only).
- **Dead letters** — `events.dlq` (quorum, no delivery limit). Every dead letter is logged; for
  `notification.due` the notification is marked `failed` with `last_error = "dead-lettered: <reason>"`.
  If Postgres is down the DLQ handler keeps retrying, so a dead letter is never discarded.
- **Worker restart policy** — the worker and consumers declare the full RabbitMQ topology on
  startup. If RabbitMQ is unreachable **at startup** they exit non-zero and compose
  (`restart: unless-stopped`) restarts them. Once running, broker outages are absorbed: pending events
  stay in the outbox and are published after recovery, without a restart.
- **Outage drill** (verified):
  ```bash
  docker compose stop rabbitmq
  # POST /payments → 201; GET /payments/{id} shows "published": false, "attempts" growing,
  # "last_error": "PublisherUnavailable: …"
  docker compose start rabbitmq
  # within ~60 s: "published": true and the payment notification "sent" (measured: 22 s)
  ```
- **Changing `CONSUMER_DELIVERY_LIMIT`** after the queues exist requires deleting the queues first:
  RabbitMQ rejects a redeclare with different arguments.

## Development

```bash
uv sync                         # Python 3.12, dev dependencies included
uv run pre-commit install       # black, ruff, mypy and file hygiene on every commit
make check                      # lint + typecheck + test (Docker must be running)
```

| Gate | Tool |
|---|---|
| Formatting | black (line length 100) — ruff is lint-only, `ruff format` is not used |
| Lint | ruff (`E F I N UP B ASYNC S RUF`) |
| Types | mypy `strict` on `src/` |
| Architecture | import-linter: layer order + framework-free inner layers + pure domain |

Configuration is environment-only (12-factor), validated at startup by `Settings`;
see [`.env.example`](.env.example).

## Testing

- `make test` — every test except e2e (unit + integration against real PostgreSQL and RabbitMQ via
  testcontainers, including the `slow` broker-pause tests), coverage floor **85 %** (currently ~97 %).
- `E2E_BASE_URL=http://localhost:8000 uv run pytest -m e2e` — happy path against the compose stack
  (the e2e test skips itself when `E2E_BASE_URL` is not set).

Notable tests: payment + outbox atomicity, concurrent duplicate payments, relay crash window
(republish with the same `event_id`), broker pause/recovery, stale reminder after extension,
extension blocked behind an in-flight send, delivery-limit → DLQ → `failed`, lost ack → no double
extension, retry via `reject` (not `nack`).

## CI

GitHub Actions (`.github/workflows/ci.yml`), on pull requests and pushes to `main`:

| Job | What it does |
|---|---|
| `quality` | `make lint`, `make typecheck`, pip-audit of runtime dependencies |
| `test` | all tests incl. slow, coverage ≥ 85 %, uploads `coverage.xml` / `junit.xml` |
| `docker` | hadolint, image build, `docker compose up --wait`, e2e test |

The `quality` job runs the same `make lint` / `make typecheck` targets as local development, and the
`test` job runs the same tests as `make test` (plus coverage/JUnit reports), so a green `make check`
predicts green `quality` and `test` jobs. The dependency audit, hadolint, image build and compose e2e
run only in CI (the commands are in the workflow if you want them locally). Dependabot
updates actions, base images and Python dependencies weekly. Nothing is pushed to a registry or deployed.

**Rollback compatibility:** migrations must stay compatible with the previous image
(expand → migrate → contract), because api, worker and consumers may briefly run mixed versions
during a rolling restart.

## Container

Multi-stage `Dockerfile`: the builder resolves dependencies with uv from `uv.lock` (dependency layer
cached separately from the source); the runtime stage is `python:3.12.15-slim-bookworm` with only the
virtualenv and source — no build tools, no dev dependencies — running as non-root uid `1001`. Base
images are pinned to exact tags; no secrets in the image. Compose sets CPU/memory limits for every
app service and persists Postgres and RabbitMQ data in named volumes.

## How this was built (AI-assisted)

Built with Claude Code following a spec → plan → test-driven execution process; all artifacts are in
the repo:

1. **Spec** — [`docs/superpowers/specs/`](docs/superpowers/specs/): requirements, approaches
   considered, domain decisions made by the requester, delivery guarantee, data model, test strategy.
   A builder check reviewed it before implementation.
2. **Plans** — [`docs/superpowers/plans/`](docs/superpowers/plans/): three task-by-task plans with
   interfaces, failing tests first, exact commands and a Review Focus list per plan.
3. **Execution** — each task RED → GREEN; every deviation from a plan was recorded as a ruling; each
   plan ended with an independent whole-branch review whose Critical/Important findings were fixed
   test-first.
4. **Agent rules and skills** — [`CLAUDE.md`](CLAUDE.md) (layering, outbox/inbox rules, commands, CI)
   and every skill used during development in [`.claude/skills/`](.claude/skills/) — see its
   [index](.claude/skills/README.md) for what each was used for. Two are specific to this repo:
   `clean-architecture` and `outbox-checklist`.

## Non-goals and future work

Non-goals: authentication (internal service behind a gateway), real payment providers, real
notification channels (pluggable `NotificationSender`, logging implementation), exactly-once delivery.

Future work: `LISTEN/NOTIFY` relay wake-up; outbox cleanup/partitioning; Prometheus metrics (outbox
lag, DLQ depth); OpenTelemetry tracing across outbox → consumer; DLQ replay tooling; auth;
database statement timeouts for the worker loops.
