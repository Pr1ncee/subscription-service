# Plan 3 of 3 — Consumers, Packaging & Agent Artifacts Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Idempotent FastStream consumers that notify on payment results, extend subscriptions, deliver notifications and handle dead letters. Plus a production-grade container image, a one-command `docker compose` stack, an end-to-end test, a GitHub Actions CI pipeline, and the AI-process artifacts reviewers asked for.

**Architecture:** Consumer logic lives in plain service classes (`services/consumers.py`), each with an `async handle(envelope)`. Every handler dedups through the inbox inside the same transaction as its side effects. FastStream handlers (`consumers/handlers.py`) are thin: they decode the envelope, call the service, and leave ack/nack to the broker policy. `consumers/app.py` is the composition root and the only place a `NotificationSender` implementation is chosen.

**Tech Stack:** as Plans 1–2 + FastStream subscribers, Docker (multi-stage, uv), docker compose, GitHub Actions, hadolint, actionlint, pip-audit, Dependabot.

**Spec:** `docs/superpowers/specs/2026-11-23-subscription-payments-design.md` (§5.1.4, §5.2, §7.3 consumers, §7.5, §10, §11)

**Prerequisite:** Plans 1 and 2 complete; their Global Constraints and skill conventions apply.

## Global Constraints

- Everything in Plans 1–2 → Global Constraints.
- Consumer names (inbox keys): `payment-notifier`, `subscription-extender`, `notification-sender`.
- Handlers use `AckPolicy.NACK_ON_ERROR` (requeue → quorum `x-delivery-limit` counts → dead-letter). A malformed envelope raises `RejectMessage` and goes straight to the DLQ. The DLQ subscriber uses `AckPolicy.REJECT_ON_ERROR` so a bug there cannot loop forever.
- Notification-sender stale rule: for a reminder, `target_expires_on != subscription.expected_expires_on` → `cancelled`, not sent.
- `PermanentSendError` → `failed` with `last_error`, committed. Any other exception propagates, the whole tx rolls back (including the inbox row), and the message is redelivered.
- Container rules (from the deployment-patterns skill):
  - multi-stage build
  - base images pinned to exact tags, never `latest`
  - dependency layer copied before the source so builds cache
  - no dev dependencies in the runtime image
  - non-root user (uid `1001`)
  - no secrets in the image; all config comes from env (12-factor), validated at startup by `Settings`
  - `.dockerignore` excludes `.git`, `.venv`, `tests`, caches and `.idea`
  - resource limits set in compose
- CI scope is **CI only**: lint, types, tests, image build and a compose smoke test. Nothing is pushed to a registry and nothing is deployed.

### Rollback compatibility (deployment-patterns)

Migrations must stay backward-compatible with the previous image (expand → migrate → contract). The worker, consumers and API may briefly run mixed versions during a rolling restart. This is documented in the README, not automated.

## Review Focus

1. **A reminder was queued, then the user paid** → no "expiring" message goes out (Task 11: `test_delivery_skips_stale_reminder_after_extension`).
2. **The sender keeps failing transiently** → after the delivery limit the notification shows `failed`, not stuck `queued` (Task 12: `test_delivery_limit_dead_letters_and_marks_failed`).
3. **Garbage bytes on a queue** → dead-lettered, never crash-looping the consumer (Task 12: `test_malformed_message_goes_to_dlq`).
4. **The same payment event is delivered twice** → the subscription is extended once (Task 11: `test_extender_applies_once_per_payment`).
5. **Transient send failure followed by redelivery** → the second attempt still sends, because the inbox row was rolled back (Task 11: `test_delivery_transient_error_rolls_back_and_retries`).

---

### Task 11: Consumer services

**Files:**
- Modify: `services/ports.py` — add `OutgoingNotification(notification_id: UUID, user_id: str, subscription_id: str, event_name: str, expected_expires_on: datetime)` (frozen dataclass), `NotificationSender` Protocol (`async send(message: OutgoingNotification) -> None`), `PermanentSendError(Exception)`
- Create: `services/consumers.py`, `adapters/logging_sender.py` (`LoggingSender`, which logs the message at INFO)
- Modify: `tests/fakes.py` — add `RecordingSender(error: Exception | None = None)` with `.sent: list[OutgoingNotification]` and `.attempts: int`
- Test: `tests/integration/test_consumer_services.py`

**Interfaces:**
- Consumes: `reschedule_reminders` (Task 5), `extend_expiry` (Task 2), repositories (Task 4), scheduler (Task 8, for test setup).
- Produces (each with `CONSUMER_NAME: ClassVar[str]` and `async handle(envelope: EventEnvelope) -> None`):
  - `PaymentResultNotifier(uow_factory, clock)`: inbox check, then `insert_payment_notification(event_name=payment.<status>, payment_id, scheduled_for=now, status=scheduled)`
  - `SubscriptionExtender(uow_factory, clock, reminder_offsets_days)`: inbox check, then `get_for_update` → `extend_expiry` → `reschedule_reminders`
  - `NotificationDelivery(uow_factory, clock, sender: NotificationSender)`: as spec §7.5 + Global Constraints
  - `DeadLetterHandler(uow_factory, clock)`: `async handle(envelope: EventEnvelope, reason: str) -> None`. Logs at ERROR. For `notification.due`, if the row is `queued` it sets `failed` with `last_error=f"dead-lettered: {reason}"`.

- [ ] **Step 1: Write failing tests**

```python
async def test_payment_notifier_creates_one_notification(notifier, payment_env, subs_view):
    await notifier.handle(payment_env); await notifier.handle(payment_env)
    pay_notes = [n for n in (await subs.get("s1")).notifications if n.event_name == "payment.succeeded"]
    assert len(pay_notes) == 1 and pay_notes[0].scheduled_for == clock.now()

async def test_payment_notifier_failed_payment_event_name(...):   # "payment.failed"

async def test_extender_applies_once_per_payment(extender, payment_env, subs, clock):   # Review Focus 4
    # subscription exp = now + 5D, day_count 30
    await extender.handle(payment_env); await extender.handle(payment_env)
    view = await subs.get("s1")
    assert view.expected_expires_on == clock.now() + 35 * D
    assert Counter(n.status for n in view.notifications) == {"cancelled": 3, "scheduled": 3}

async def test_extender_on_lapsed_subscription_starts_from_now(...):   # exp = now - 10D -> now + 30D
async def test_two_payments_extend_twice(...):                           # -> now + 65D

async def test_delivery_sends_and_marks_sent(delivery, sender, queued_note_env, clock):
    await delivery.handle(queued_note_env)
    assert [m.user_id for m in sender.sent] == ["u1"]
    # notification status "sent", sent_at == clock.now()
async def test_delivery_duplicate_message_sends_once(...): ...
async def test_delivery_skips_stale_reminder_after_extension(...):      # Review Focus 1
    # queue 3-day reminder via scheduler, then extender.handle(payment) -> delivery.handle(due) -> status "cancelled", sender.sent == []
async def test_delivery_skips_cancelled_notification(...): ...
async def test_delivery_permanent_error_marks_failed(...):              # PermanentSendError("bad address") -> "failed", last_error == "bad address", no raise
async def test_delivery_transient_error_rolls_back_and_retries(...):     # Review Focus 5
    sender.error = RuntimeError("smtp timeout")
    with pytest.raises(RuntimeError): await delivery.handle(env)
    # status still "queued"
    sender.error = None; await delivery.handle(env)
    # status "sent", len(sender.sent) == 1

async def test_dead_letter_marks_queued_notification_failed(...):     # last_error startswith "dead-lettered"
async def test_dead_letter_leaves_sent_notification(...): ...
async def test_dead_letter_payment_event_only_logs(..., caplog): ...
```

- [ ] **Step 2: Run — expect FAIL** — `uv run pytest tests/integration/test_consumer_services.py -v`
- [ ] **Step 3: Implement** the four classes and `LoggingSender`.
- [ ] **Step 4: Run — expect PASS**; `uv run mypy && uv run lint-imports`
- [ ] **Step 5: Commit** — `feat(services): add idempotent consumer handlers`

---

### Task 12: FastStream consumers app

**Files:**
- Create: `consumers/handlers.py`, `consumers/app.py`, `consumers/__main__.py`
- Test: `tests/integration/test_consumers_app.py` (Postgres + RabbitMQ)

**Interfaces:**
- Consumes: Task 10 topology (`EVENTS_EXCHANGE`, `QUEUE_SPECS`, `rabbit_queue`, `dead_letter_queue`, `declare_topology`), Task 11 services.
- Produces:
  - `ConsumerServices(notifier, extender, delivery, dead_letters)` frozen dataclass
  - `register_handlers(broker: RabbitBroker, services: ConsumerServices, delivery_limit: int) -> None`. It registers one subscriber per queue on `EVENTS_EXCHANGE` plus one on `events.dlq`. A handler signature is `async (body: bytes, message: RabbitMessage) -> None`. Decoding uses `EventEnvelope.from_json`, and `InvalidEnvelopeError` → `raise RejectMessage() from e`. The DLQ handler reads its reason from the `x-first-death-reason` header (default `"unknown"`). For an undecodable DLQ body it only logs.
  - `create_app(settings: Settings, *, sender: NotificationSender | None = None, clock: Clock | None = None) -> FastStream` (composition root; defaults `LoggingSender()`, `SystemClock()`). An `on_startup` hook calls `declare_topology`. `__main__.py`: `asyncio.run(create_app(Settings()).run())`.

- [ ] **Step 1: Write failing tests** (fixture: start the app's broker in-process with `async with broker: await broker.start()`; `wait_until(predicate, timeout=10)` helper polls the DB)

```python
async def test_payment_flow_through_broker(stack, subs, psvc, relay, sched, sender, clock):
    await subs.upsert("s1", "u1", 30, clock.now() + 5 * D)
    pay = await psvc.record(CMD)
    await relay.run_once()
    await wait_until(lambda: subs_expiry("s1") == clock.now() + 35 * D)
    await wait_until(lambda: payment_notification_exists(pay))
    await sched.run_once(); await relay.run_once()
    await wait_until(lambda: psvc_status(pay).notification.status == "sent")
    assert [m.event_name for m in sender.sent] == ["payment.succeeded"]

async def test_malformed_message_goes_to_dlq(stack, rabbit_broker, dlq_spy):           # Review Focus 3
    await rabbit_broker.publish(b"garbage", exchange=EVENTS_EXCHANGE, routing_key="payment.succeeded")
    await wait_until(lambda: dlq_spy.count >= 1)

async def test_delivery_limit_dead_letters_and_marks_failed(stack_with(delivery_limit=2, sender=RecordingSender(RuntimeError("down"))), ...):  # Review Focus 2
    # queue a notification, relay publishes notification.due
    await wait_until(lambda: note_status == "failed", timeout=20)
    assert sender.attempts >= 2 and note.last_error.startswith("dead-lettered")
```

- [ ] **Step 2: Run — expect FAIL** — `uv run pytest tests/integration/test_consumers_app.py -v`
- [ ] **Step 3: Implement handlers and app.** Check the `AckPolicy` names against the installed FastStream version. In this setup, delivery-limit counting needs nack **with requeue**.
- [ ] **Step 4: Run — expect PASS**, then the whole suite: `uv run pytest -m "slow or not slow" -v`
- [ ] **Step 5: Commit** — `feat(consumers): wire FastStream subscribers and dead-letter handling`

---

### Task 13: Container image, compose stack, E2E test

**Files:**
- Create: `Dockerfile`, `.dockerignore`, `docker-compose.yml`, `.env.example`, `tests/e2e/test_compose_flow.py`

**Interfaces:**
- Produces: one image, `Dockerfile` with two stages:
  - **builder** — `FROM python:3.12-slim-bookworm`, then `COPY --from=ghcr.io/astral-sh/uv:<exact version from uv.lock's generator / local "uv --version"> /uv /bin/uv`. Env: `UV_COMPILE_BYTECODE=1`, `UV_LINK_MODE=copy`. First `COPY pyproject.toml uv.lock ./` and `RUN --mount=type=cache,target=/root/.cache/uv uv sync --frozen --no-dev --no-install-project`. Then `COPY src alembic.ini ./` and `uv sync --frozen --no-dev`.
  - **runtime** — `FROM python:3.12-slim-bookworm`; `useradd -r -u 1001 app`. Copies `/app/.venv`, `src` and `alembic.ini` with `--chown=app`. `ENV PATH=/app/.venv/bin:$PATH PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1`, `USER app`, `EXPOSE 8000`. Default `CMD ["fastapi", "run", "--host", "0.0.0.0", "--port", "8000"]`. No `HEALTHCHECK` in the Dockerfile: the same image runs the worker and consumers, which serve no HTTP. Health checks are per service in compose.
- Process commands (compose `command:`):
  - `api`: default CMD
  - `worker`: `python -m subscriptions_service.worker`
  - `consumers`: `python -m subscriptions_service.consumers`
  - `migrate` (one-shot): `alembic upgrade head`
- `docker-compose.yml`:
  - **image and env:** app services use `image: ${IMAGE:-subscriptions-service:local}` plus `build: .`, so CI can inject a prebuilt image. Env comes from `.env.example` via `env_file`.
  - **postgres:** `postgres:16.4-alpine`, `pg_isready` healthcheck, named volume.
  - **rabbitmq:** `rabbitmq:4.0-management-alpine` (exact tags), `rabbitmq-diagnostics -q ping` healthcheck, UI on 15672.
  - **migrate:** depends on postgres healthy.
  - **api:** port 8000, depends on `migrate: service_completed_successfully`, healthcheck `python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/health/ready')"`.
  - **worker and consumers:** depend on migrate done + rabbitmq healthy, `restart: unless-stopped`.
  - **resource limits:** every app service gets `deploy.resources.limits` (`cpus: "0.5"`, `memory: 256M`).

- [ ] **Step 1: Write the E2E test** (marked `e2e`, skipped unless `E2E_BASE_URL` is set)

```python
@pytest.mark.e2e
async def test_full_flow_against_running_stack():
    async with httpx.AsyncClient(base_url=os.environ["E2E_BASE_URL"]) as c:
        sid, exp = f"e2e-{uuid4()}", datetime.now(UTC) + timedelta(days=10)
        assert (await c.put(f"/subscriptions/{sid}", json={"user_id": "u1", "day_count": 30, "expected_expires_on": exp.isoformat()})).status_code == 201
        pid = (await c.post("/payments", json={"subscription_id": sid, "provider_payment": f"pp-{uuid4()}", "amount": "9.99", "currency": "USD", "status": "succeeded"})).json()["payment_id"]
        body = await poll(c, f"/payments/{pid}", lambda b: b["event"]["published"] and (b["notification"] or {}).get("status") == "sent", timeout=20)
        sub = (await c.get(f"/subscriptions/{sid}")).json()
        assert datetime.fromisoformat(sub["expected_expires_on"]) == exp + timedelta(days=30)
```

- [ ] **Step 2: Lint the Dockerfile and check image hygiene**

Run: `docker run --rm -i hadolint/hadolint:<pinned tag> < Dockerfile`, then `docker build -t subscriptions-service:local .` and `docker run --rm subscriptions-service:local id -u`
Expected: hadolint reports no warnings; the uid printed is `1001`; `docker run --rm subscriptions-service:local python -c "import pytest"` fails (no dev dependencies in the image).

- [ ] **Step 3: Build and run the stack**

Run: `docker compose up -d --build --wait`
Expected: all services `healthy` or `running`; `migrate` exited 0.

- [ ] **Step 4: Run E2E — expect PASS** — `E2E_BASE_URL=http://localhost:8000 uv run pytest -m e2e -v`
- [ ] **Step 5: Outage drill (manual, recorded in README).** Run `docker compose stop rabbitmq`, POST a payment (expect `201`), and check that `GET /payments/{id}` shows `published: false` with `attempts` growing. Then run `docker compose start rabbitmq` and expect `published: true` within ~60 s.
- [ ] **Step 6: Tear down** — `docker compose down -v`
- [ ] **Step 7: Commit** — `build: add Dockerfile, compose stack and e2e test`

---

### Task 14: CI pipeline (GitHub Actions)

**Files:**
- Create: `.github/workflows/ci.yml`, `.github/dependabot.yml`

**Interfaces:**
- Consumes: `make lint`, `make typecheck`, `make test` (Task 1); `Dockerfile`, compose `IMAGE` variable, e2e test (Task 13).
- Produces: workflow `CI` triggered on `pull_request` and on `push` to `main`. It sets `permissions: contents: read` and `concurrency: { group: ci-${{ github.ref }}, cancel-in-progress: true }`. Every third-party action is pinned to a major version tag. Jobs:
  1. **`quality`**:
     - `actions/checkout@v4`
     - `astral-sh/setup-uv@v5` (`enable-cache: true`, `python-version: "3.12"`)
     - `uv sync --frozen`, `make lint`, `make typecheck`
     - `uv export --frozen --no-dev --no-hashes | uvx pip-audit --strict -r /dev/stdin`, which scans dependencies for CVEs
  2. **`test`** (runs in parallel with `quality`):
     - same setup steps as `quality`
     - `uv run pytest -m "slow or not slow" --cov --cov-report=xml --junitxml=junit.xml`. Testcontainers uses the runner's Docker. Coverage must stay ≥ 85 % (`fail_under`).
     - `actions/upload-artifact@v4` (`if: always()`) for `coverage.xml` and `junit.xml`
  3. **`docker`** (`needs: [quality, test]`):
     - `hadolint/hadolint-action@v3.1.0`
     - `docker/setup-buildx-action@v3`
     - `docker/build-push-action@v6` with `push: false`, `load: true`, `tags: subscriptions-service:ci`, `cache-from: type=gha`, `cache-to: type=gha,mode=max`
     - `IMAGE=subscriptions-service:ci docker compose up -d --wait`
     - install uv and run `E2E_BASE_URL=http://localhost:8000 uv run pytest -m e2e -v`
     - on `failure()`, `docker compose logs`
     - `if: always()` `docker compose down -v`
- `dependabot.yml`: weekly updates for `github-actions`, `docker` and `uv` ecosystems.

- [ ] **Step 1: Write `ci.yml` and `dependabot.yml`**

- [ ] **Step 2: Lint the workflow locally**

Run: `docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:<pinned tag> -color`
Expected: no output, exit 0.

- [ ] **Step 3: Reproduce each CI job locally with the same commands**

Run: `uv sync --frozen && make lint typecheck && uv run pytest -m "slow or not slow" --cov && IMAGE=subscriptions-service:ci docker build -t subscriptions-service:ci . && IMAGE=subscriptions-service:ci docker compose up -d --wait && E2E_BASE_URL=http://localhost:8000 uv run pytest -m e2e && docker compose down -v`
Expected: every command exits 0.

- [ ] **Step 4: Commit** — `ci: add GitHub Actions pipeline with lint, tests and image smoke test`

- [ ] **Step 5: Hand off (do NOT push).** Tell the user the pipeline is committed locally and that it only runs after *they* push to GitHub. Pushing needs their explicit approval (user rule).

---

### Task 15: Agent artifacts & README

**Files:**
- Create: `CLAUDE.md`, `.claude/skills/outbox-checklist/SKILL.md`, `README.md` (replace the uv stub)

**Interfaces:**
- `CLAUDE.md` must state:
  - the layer rule and that `uv run lint-imports` enforces it
  - the outbox rule: never publish to the broker from `api`/`services`; new events go through `OutboxRepository.add` in the same UoW
  - every consumer dedups through `InboxRepository`
  - composition roots are `api/deps.py`, `worker/main.py` and `consumers/app.py`
  - commands: `make format|lint|typecheck|test|check`, compose, e2e; formatter is black, ruff is lint-only, never run `ruff format`
  - the CI job map (`quality`, `test`, `docker`) and the rule that CI runs exactly the `make` targets, so a green `make check` locally predicts green CI
  - the skills to apply: `fastapi`, `clean-architecture` (in repo), `python-patterns`, `outbox-checklist`
  - commit style: Conventional Commits, no AI trailers
- `outbox-checklist/SKILL.md`: frontmatter `name: outbox-checklist`, `description: Use when adding or changing an event type, outbox write, queue, or consumer`. The checklist:
  1. `EventType` member
  2. outbox row written in the same UoW as the state change
  3. `QueueSpec` + binding, declared by both worker and consumers
  4. consumer has a `CONSUMER_NAME` and dedups via the inbox
  5. failure classification (permanent vs transient)
  6. tests: atomic rollback, duplicate delivery, DLQ path
- `README.md` sections:
  - Overview
  - Quick start (`docker compose up -d --build --wait`, curl for all 4 operations)
  - API table
  - Delivery guarantee explained (outbox → relay → inbox, at-least-once, what can duplicate: notification send after crash)
  - Design decisions: link to the spec, the requester's domain calls, the expiry-never-backwards rule
  - Operations: health endpoints, DLQ, outage drill from Task 13, worker restart policy
  - Development: `uv sync`, `uv run pre-commit install`, `make check`; linters (black, ruff, mypy strict, import-linter)
  - Testing: `make test`, slow, e2e; coverage floor 85 %
  - CI: CI status badge, what each job does, and the rollback-compatibility rule for migrations
  - Container: image layout, non-root, how one image runs four roles
  - How this was built (AI-assisted): spec, plans, CLAUDE.md, skills, builder check
  - Non-goals and future work (spec §14)

- [ ] **Step 1: Write the three files**
- [ ] **Step 2: Verify the README quick start from a clean checkout** — `docker compose down -v && docker compose up -d --build --wait`, then run every curl in the README and get the documented status codes.
- [ ] **Step 3: Final gates — expect all green**

Run: `make lint typecheck && uv run pytest -m "slow or not slow" --cov -v && docker run --rm -v "$PWD:/repo" -w /repo rhysd/actionlint:<pinned tag>`

- [ ] **Step 4: Commit** — `docs: add README, CLAUDE.md and outbox-checklist skill`
