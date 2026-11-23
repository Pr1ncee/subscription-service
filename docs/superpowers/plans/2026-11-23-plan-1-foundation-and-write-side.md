# Plan 1 of 3 — Foundation & Write Side Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A FastAPI service backed by PostgreSQL that upserts subscriptions (with reminder scheduling), records payments atomically together with an outbox event, and reports subscription/payment state — no broker yet.

**Architecture:** Pragmatic layers `api → services → repositories → db → domain`, enforced mechanically by import-linter. Services own the transaction through an async `UnitOfWork` context manager; ORM rows never leave the services layer (services return frozen dataclass DTOs, the API maps them to Pydantic schemas). Time and other side effects enter through `Protocol` ports injected at a single composition root (`api/deps.py`).

**Tech Stack:** Python 3.12, uv, FastAPI (`fastapi[standard]`), SQLAlchemy 2.0 async + asyncpg, Alembic, Pydantic v2 + pydantic-settings, pytest + pytest-asyncio + pytest-cov + testcontainers, httpx, black, ruff, mypy, import-linter, pre-commit, GNU make.

**Spec:** `docs/superpowers/specs/2026-11-23-subscription-payments-design.md`

**Sequence:** Plan 1 (this) → `2026-11-23-plan-2-worker.md` → `2026-11-23-plan-3-consumers-and-delivery.md`. Task numbers are global across the three plans.

## Global Constraints

- Python `3.12`; package `subscriptions_service` under `src/`; dependency management with `uv` only.
- All timestamps `timestamptz` UTC; API accepts only timezone-aware datetimes (naive → `422`).
- `REMINDER_OFFSETS_DAYS` default `3,1,0`; offset `0` → `subscription.expired`, others → `subscription.expiring`; only slots strictly in the future are scheduled.
- Expiry never moves backwards: update sets `expected_expires_on = max(current, requested)`.
- Payment extension: `expected_expires_on = max(expected_expires_on, now) + period_days days`, where `period_days` is the subscription's `day_count` **snapshotted onto the payment when it is recorded** (column `payments.period_days`, carried in the event payload).
- Lock order is **subscription → notification** in every transaction that touches both.
- `amount` `numeric(19,4)`, `> 0`; `currency` matches `^[A-Z]{3}$`; payment `status` ∈ `succeeded|failed`.
- `PUT /subscriptions/{id}` → `201` created / `200` updated; `user_id` mismatch → `409`; unknown subscription on GET or payment → `404`.
- `POST /payments` duplicate `provider_payment`: identical payload → `200` existing, different payload → `409`.
- The API process never talks to RabbitMQ; `/health/ready` checks only the DB.
- Defaults: `RELAY_POLL_INTERVAL_S=0.5`, `RELAY_BATCH_SIZE=100`, `SCHEDULER_POLL_INTERVAL_S=1.0`, `SCHEDULER_BATCH_SIZE=100`, `RELAY_MAX_BACKOFF_S=60`, `CONSUMER_DELIVERY_LIMIT=5`, `PUBLISH_TIMEOUT_S=5.0`, `LOG_LEVEL=INFO`.
- Commits: Conventional Commits, **no** `Co-Authored-By`/AI trailers; never `git push` (user rule).
- Quality gates (identical locally, in pre-commit, and in CI): **black** formats (line-length `100`, target `py312`). **ruff** lints only, with `ruff format` deliberately unused so two formatters never fight and `E501` ignored because black owns line length. **mypy** runs `strict` on `src/`, and **import-linter** enforces the layer contracts. Everything is invoked through `make` targets: `make format`, `make lint`, `make typecheck`, `make test`, `make check` (= lint + typecheck + test).

### Conventions from the skills the requester named

- **fastapi:** `Annotated[...]` for every `Path`/`Query`/`Depends`; a type alias per dependency (`SubscriptionServiceDep`, …); return-type annotations instead of `response_model`; router-level `prefix`/`tags`; no `...` defaults; no `RootModel`; `[tool.fastapi] entrypoint`; run with `fastapi dev` / `fastapi run`; `max_body_size=64 * 1024` on the app. Path operations are `async def` (all I/O goes through asyncpg — no blocking calls).
- **clean-architecture:** ports (`Clock`, later `EventPublisher`, `NotificationSender`) are `Protocol`s in `services/ports.py`; concrete adapters live in `adapters/`; composition roots are exactly `api/deps.py`, `worker/main.py`, `consumers/app.py` — nothing else instantiates adapters. Repositories are deliberately **not** behind Protocols (single implementation, tested against real Postgres — avoids the "over-abstraction" pitfall). `domain` imports nothing from the project or from frameworks.
- **python-patterns:** full type hints (mypy `strict`), `StrEnum` for statuses, `@dataclass(frozen=True, slots=True)` for DTOs/values, exception hierarchy `DomainError → NotFoundError | ConflictError`, `raise … from e`, no bare `except`, async context managers for resources.
- **Deliberate deviations from the fastapi skill:** SQLAlchemy 2.0 Core/ORM instead of SQLModel (we need `ON CONFLICT`, `FOR UPDATE SKIP LOCKED`, partial indexes, and we keep persistence models separate from API schemas); mypy instead of ty (spec decision, mature strict mode).

## Review Focus

1. **Relay publishes before any consumer has ever started** → messages to an exchange with no bound queues are silently dropped. Expected: nothing is lost. *(Owned by Task 10.)*
2. **The same `provider_payment` POSTed twice concurrently** → expected exactly one payment and one outbox row, one `201` and one `200`, never a `500`. *(Task 6.)*
3. **Idempotent retry with the same amount in a different scale** (`10` vs `10.00`) → expected `200`, not `409`. *(Task 6.)*
4. **PUT with an expiry already in the past** → expected subscription saved, no reminders, no error. *(Task 5.)*
5. **Broker becomes unreachable while the worker runs** → expected publish fails within the timeout, the event stays pending, and it is published once the broker is back, without restarting the worker. *(Task 10.)*

---

## File Structure (this plan)

```
pyproject.toml  .python-version  alembic.ini
src/subscriptions_service/
  __init__.py
  config.py                 Settings (pydantic-settings)
  logs.py                   configure_logging(): one-line JSON logs
  domain/  enums.py errors.py schedule.py events.py
  db/      base.py tables.py engine.py migrations/{env.py,script.py.mako,versions/0001_initial.py}
  repositories/ subscriptions.py notifications.py payments.py outbox.py inbox.py uow.py
  services/ ports.py dto.py reminders.py subscriptions.py payments.py
  adapters/ clock.py
  api/     app.py deps.py schemas.py errors.py routers/{subscriptions,payments,health}.py
  worker/__init__.py  consumers/__init__.py   (empty until Plans 2–3)
tests/
  conftest.py fakes.py factories.py test_architecture.py
  unit/ test_config.py test_schedule.py test_events.py
  integration/ test_migrations.py test_repositories.py test_subscription_service.py
               test_payment_service.py test_api.py
```

---

### Task 1: Project scaffold & quality gates

**Files:**
- Create: `pyproject.toml`, `.python-version`, `Makefile`, `.pre-commit-config.yaml`, `src/subscriptions_service/{__init__,config,logs}.py`, empty `__init__.py` in `domain/ db/ repositories/ services/ adapters/ api/ api/routers/ worker/ consumers/`
- Modify: `.gitignore` (add `.pytest_cache/ .mypy_cache/ .ruff_cache/ dist/`)
- Test: `tests/unit/test_config.py`, `tests/test_architecture.py`

**Interfaces:**
- Produces: `Settings` (fields: `database_url: str`, `rabbitmq_url: str`, `reminder_offsets_days: list[int]`, `relay_poll_interval_s: float`, `relay_batch_size: int`, `scheduler_poll_interval_s: float`, `scheduler_batch_size: int`, `relay_max_backoff_s: float`, `consumer_delivery_limit: int`, `publish_timeout_s: float`, `log_level: str`); `configure_logging(level: str) -> None`.

- [ ] **Step 1: Scaffold with uv**

```bash
uv init --package --name subscriptions-service --python 3.12 --vcs none .
uv add "fastapi[standard]" "faststream[rabbit]" "sqlalchemy[asyncio]" asyncpg alembic pydantic-settings
uv add --dev pytest pytest-asyncio pytest-cov "testcontainers[postgres,rabbitmq]" httpx black ruff mypy import-linter pre-commit
```

Rename the generated package dir to `src/subscriptions_service`. In `pyproject.toml` add:
- `[tool.fastapi] entrypoint = "subscriptions_service.api.app:app"`
- `[tool.black] line-length = 100`, `target-version = ["py312"]`
- `[tool.ruff] line-length = 100`, `target-version = "py312"`, `lint.select = ["E","F","I","N","UP","B","ASYNC","S","RUF"]`, `lint.ignore = ["E501"]`, per-file-ignore `S101` for `tests/**` (the `I` rules are compatible with black's import formatting)
- `[tool.mypy] strict = true`, `files = ["src"]`, `plugins = ["pydantic.mypy"]`
- `[tool.coverage.run] source = ["subscriptions_service"]`, `branch = true`; `[tool.coverage.report] fail_under = 85`, `show_missing = true`

`Makefile` (each target is a thin wrapper over `uv run`, `.PHONY` on all):

```make
format:    uv run black . && uv run ruff check --fix .
lint:      uv run black --check . && uv run ruff check .
typecheck: uv run mypy && uv run lint-imports
test:      uv run pytest -m "not e2e" --cov --cov-report=term-missing --cov-report=xml
check:     lint typecheck test
```

`.pre-commit-config.yaml`: `pre-commit-hooks` (trailing-whitespace, end-of-file-fixer, check-yaml, check-added-large-files), `black`, `ruff-pre-commit` (`ruff` hook with `--fix`, **not** `ruff-format`), and a `local` hook `mypy` with `entry: uv run mypy`, `language: system`, `pass_filenames: false`. Pin every `rev` to a release tag that exists when you write it. Then `uv run pre-commit install`.
- `[tool.pytest.ini_options] asyncio_mode = "auto"`, `markers = ["e2e: needs docker compose stack", "slow: pauses containers"]`, `addopts = "-m 'not e2e'"`
- import-linter contracts:

```toml
[tool.importlinter]
root_package = "subscriptions_service"
include_external_packages = true   # required for the forbidden contracts below to check fastapi/sqlalchemy/etc.

[[tool.importlinter.contracts]]
name = "Layers"
type = "layers"
layers = [
  "subscriptions_service.api | subscriptions_service.worker | subscriptions_service.consumers",
  "subscriptions_service.adapters",
  "subscriptions_service.services",
  "subscriptions_service.repositories",
  "subscriptions_service.db",
  "subscriptions_service.domain",
]

[[tool.importlinter.contracts]]
name = "Inner layers are framework- and config-free"
type = "forbidden"
source_modules = ["subscriptions_service.domain", "subscriptions_service.services", "subscriptions_service.repositories"]
forbidden_modules = ["fastapi", "faststream", "aio_pika", "subscriptions_service.config"]

[[tool.importlinter.contracts]]
name = "Domain is pure"
type = "forbidden"
source_modules = ["subscriptions_service.domain"]
forbidden_modules = ["sqlalchemy", "pydantic"]
```

- [ ] **Step 2: Write failing tests**

```python
# tests/unit/test_config.py
def test_reminder_offsets_parsed_from_csv(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+asyncpg://u:p@h/db")
    monkeypatch.setenv("RABBITMQ_URL", "amqp://g:g@h/")
    monkeypatch.setenv("REMINDER_OFFSETS_DAYS", "7,1,0")
    s = Settings()
    assert s.reminder_offsets_days == [7, 1, 0]
    assert (s.relay_poll_interval_s, s.relay_batch_size) == (0.5, 100)
    assert (s.scheduler_poll_interval_s, s.scheduler_batch_size) == (1.0, 100)
    assert (s.relay_max_backoff_s, s.consumer_delivery_limit, s.publish_timeout_s) == (60, 5, 5.0)

def test_default_offsets(monkeypatch):  # same env minus REMINDER_OFFSETS_DAYS
    assert Settings().reminder_offsets_days == [3, 1, 0]

def test_negative_offset_rejected(monkeypatch):  # REMINDER_OFFSETS_DAYS="3,-1"
    with pytest.raises(ValidationError): Settings()

# tests/test_architecture.py
def test_import_contracts_hold():
    result = subprocess.run(["lint-imports"], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout

def test_forbidden_contract_actually_bites(tmp_path):
    # copy src/ to tmp_path, append "import fastapi" to domain/__init__.py,
    # run lint-imports with cwd=tmp_path and PYTHONPATH=tmp_path/src
    assert result.returncode != 0
    assert "Inner layers are framework- and config-free BROKEN" in result.stdout
```
(The second test proves the external-package rules are live, not silently ignored.)

- [ ] **Step 3: Run — expect FAIL** (`ImportError: Settings`)

Run: `uv run pytest tests/unit/test_config.py tests/test_architecture.py -v`

- [ ] **Step 4: Implement `Settings` in `config.py` and `configure_logging` in `logs.py`**

`reminder_offsets_days: Annotated[list[int], NoDecode]` with a `field_validator(mode="before")` splitting the comma string; reject negatives and duplicates. Logging: stdlib `logging.Formatter` subclass emitting one JSON object per line (`ts`, `level`, `logger`, `msg`, plus any `extra` keys).

- [ ] **Step 5: Run — expect PASS; gates clean**

Run: `uv run pytest tests/unit/test_config.py tests/test_architecture.py -v && make lint typecheck && uv run pre-commit run --all-files`
Expected: tests pass; black reports nothing to reformat; ruff, mypy, import-linter and every pre-commit hook pass.

- [ ] **Step 6: Commit** — `git add -A && git commit -m "chore: scaffold project with settings, logging and quality gates"`

---

### Task 2: Domain — enums, errors, scheduling rules, event envelope

**Files:**
- Create: `src/subscriptions_service/domain/{enums,errors,schedule,events}.py`
- Test: `tests/unit/test_schedule.py`, `tests/unit/test_events.py`

**Interfaces:**
- Produces:
  - `NotificationStatus(StrEnum)`: `SCHEDULED QUEUED SENT CANCELLED FAILED` (values lowercase)
  - `PaymentStatus(StrEnum)`: `SUCCEEDED FAILED`
  - `NotificationEvent(StrEnum)`: `SUBSCRIPTION_EXPIRING="subscription.expiring"`, `SUBSCRIPTION_EXPIRED="subscription.expired"`, `PAYMENT_SUCCEEDED="payment.succeeded"`, `PAYMENT_FAILED="payment.failed"`
  - `EventType(StrEnum)`: `PAYMENT_SUCCEEDED="payment.succeeded"`, `PAYMENT_FAILED="payment.failed"`, `NOTIFICATION_DUE="notification.due"`
  - `DomainError(Exception)`, `NotFoundError(DomainError)`, `ConflictError(DomainError)`
  - `ReminderSlot(event: NotificationEvent, scheduled_for: datetime)` — frozen slots dataclass
  - `reminder_slots(expires_on: datetime, offsets_days: Sequence[int], now: datetime) -> list[ReminderSlot]` (ascending by `scheduled_for`, only `> now`)
  - `merge_expiry(current: datetime, requested: datetime) -> datetime`
  - `extend_expiry(current: datetime, day_count: int, now: datetime) -> datetime`
  - `EventEnvelope(event_id: UUID, event_type: EventType, occurred_at: datetime, payload: Mapping[str, Any])` frozen; `EventEnvelope.new(event_type, payload, occurred_at) -> EventEnvelope` (uuid4); `to_json() -> bytes`; `from_json(data: bytes) -> EventEnvelope` raising `InvalidEnvelopeError(ValueError)`.

- [ ] **Step 1: Write failing tests**

```python
NOW = datetime(2026, 1, 1, 12, tzinfo=UTC)
D = timedelta(days=1)

def test_reminder_slots_default_offsets():
    exp = NOW + 10 * D
    assert reminder_slots(exp, [3, 1, 0], NOW) == [
        ReminderSlot(NotificationEvent.SUBSCRIPTION_EXPIRING, exp - 3 * D),
        ReminderSlot(NotificationEvent.SUBSCRIPTION_EXPIRING, exp - D),
        ReminderSlot(NotificationEvent.SUBSCRIPTION_EXPIRED, exp),
    ]

def test_reminder_slots_skip_past():
    assert [s.scheduled_for for s in reminder_slots(NOW + 2 * D, [3, 1, 0], NOW)] == [NOW + D, NOW + 2 * D]

def test_reminder_slot_exactly_now_is_skipped():
    assert len(reminder_slots(NOW + 3 * D, [3, 1, 0], NOW)) == 2

def test_reminder_slots_all_past_is_empty():
    assert reminder_slots(NOW - timedelta(hours=1), [3, 1, 0], NOW) == []

def test_merge_expiry_never_moves_backwards():
    assert merge_expiry(NOW + 5 * D, NOW + D) == NOW + 5 * D
    assert merge_expiry(NOW + D, NOW + 5 * D) == NOW + 5 * D

def test_extend_expiry_from_future_expiry():
    assert extend_expiry(NOW + 5 * D, 30, NOW) == NOW + 35 * D

def test_extend_expiry_when_lapsed_starts_from_now():
    assert extend_expiry(NOW - 10 * D, 30, NOW) == NOW + 30 * D

# test_events.py
def test_envelope_roundtrip():
    env = EventEnvelope.new(EventType.PAYMENT_SUCCEEDED, {"payment_id": "p1", "amount": "10.0000"}, NOW)
    assert EventEnvelope.from_json(env.to_json()) == env

@pytest.mark.parametrize("raw", [b"not json", b"{}", b'{"event_id": "x"}'])
def test_invalid_envelope_raises(raw):
    with pytest.raises(InvalidEnvelopeError): EventEnvelope.from_json(raw)
```

- [ ] **Step 2: Run — expect FAIL** — `uv run pytest tests/unit -v`
- [ ] **Step 3: Implement the four modules** (stdlib only; `from_json` wraps `json.JSONDecodeError`/`KeyError`/`ValueError` with `raise InvalidEnvelopeError(...) from e`)
- [ ] **Step 4: Run — expect PASS** — `uv run pytest tests/unit -v && uv run lint-imports`
- [ ] **Step 5: Commit** — `git commit -am "feat(domain): add scheduling rules, statuses and event envelope"` (add new files first)

---

### Task 3: Database schema, migration, test infrastructure

**Files:**
- Create: `db/base.py` (`Base` with naming convention), `db/tables.py`, `db/engine.py`, `alembic.ini` (`script_location = src/subscriptions_service/db/migrations`), `db/migrations/{env.py,script.py.mako,versions/0001_initial.py}`
- Create: `tests/conftest.py`, `tests/fakes.py`
- Test: `tests/integration/test_migrations.py`

**Interfaces:**
- Produces:
  - ORM classes `SubscriptionRow`, `NotificationRow`, `PaymentRow`, `OutboxRow`, `ProcessedEventRow` mirroring spec §6 exactly (column names, types, `target_expires_on`, `last_error`, indexes incl. partial unique `uq_notifications_payment_id WHERE payment_id IS NOT NULL`, partial `ix_outbox_pending (next_attempt_at, created_at) WHERE published_at IS NULL`, partial `ix_notifications_due (scheduled_for) WHERE status='scheduled'`). Status columns are `Text` + `CheckConstraint` (not PG enums — cheaper migrations). Checks: `day_count > 0`, `amount > 0`, `currency ~ '^[A-Z]{3}$'`.
  - `create_engine(database_url: str) -> AsyncEngine`, `create_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]` (`expire_on_commit=False`)
  - Fixtures: `pg_url` (session, sync: `PostgresContainer("postgres:16-alpine", driver="asyncpg")`, runs `alembic upgrade head`), `session_factory` (function: `NullPool` engine, `TRUNCATE subscriptions, notifications, payments, outbox, processed_events CASCADE` before yield), `clock` → `FakeClock(datetime(2026, 1, 1, 12, tzinfo=UTC))`
  - `tests/fakes.py`: `FakeClock(now: datetime)` with `now() -> datetime`, `advance(delta: timedelta) -> None`

- [ ] **Step 1: Write failing tests**

```python
def test_migrations_match_models(pg_url):
    command.check(alembic_config(pg_url))  # raises AutogenerateDiffsDetected on drift

async def test_one_result_notification_per_payment(session_factory):
    # insert subscription + payment + two NotificationRow with same payment_id
    with pytest.raises(IntegrityError): ...
```

- [ ] **Step 2: Run — expect FAIL** — `uv run pytest tests/integration/test_migrations.py -v` (Docker must be running)
- [ ] **Step 3: Implement tables, engine, async Alembic `env.py`** (reads `DATABASE_URL` from `os.environ`; `target_metadata = Base.metadata`; runs migrations via `connection.run_sync`). Write `0001_initial.py` by autogenerate, then review it by hand against spec §6.
- [ ] **Step 4: Run — expect PASS**
- [ ] **Step 5: Commit** — `feat(db): add schema, initial migration and test fixtures`

---

### Task 4: Repositories & Unit of Work

**Files:**
- Create: `repositories/{subscriptions,notifications,payments,outbox,inbox,uow}.py`, `tests/factories.py`
- Modify: `tests/conftest.py` (add `uow_factory` fixture: `lambda: UnitOfWork(session_factory)`)
- Test: `tests/integration/test_repositories.py`

**Interfaces:**
- Produces (all methods `async`, each repo constructed with an `AsyncSession`):
  - `SubscriptionRepository`: `get(subscription_id: str) -> SubscriptionRow | None`; `get_for_update(subscription_id: str) -> SubscriptionRow | None`; `get_for_share(subscription_id: str) -> SubscriptionRow | None` (`FOR SHARE`, used by notification delivery in Task 11); `insert_if_absent(row: SubscriptionRow) -> bool` (`ON CONFLICT DO NOTHING`, True if inserted)
  - `NotificationRepository`: `list_for_subscription(subscription_id: str) -> list[NotificationRow]` (order `scheduled_for, created_at`); `add_many(rows: Sequence[NotificationRow]) -> None`; `cancel_scheduled_reminders(subscription_id: str, now: datetime) -> int` (only `status='scheduled' AND payment_id IS NULL`); `insert_payment_notification(row: NotificationRow) -> bool` (`ON CONFLICT (payment_id) WHERE payment_id IS NOT NULL DO NOTHING`); `lock_due(now: datetime, limit: int) -> list[NotificationRow]` (`FOR UPDATE SKIP LOCKED`); `get_for_update(notification_id: UUID) -> NotificationRow | None`; `get_by_payment(payment_id: UUID) -> NotificationRow | None`
  - `PaymentRepository`: `insert_if_absent(row: PaymentRow) -> PaymentRow | None` (`ON CONFLICT (provider_payment) DO NOTHING RETURNING`); `get(payment_id: UUID) -> PaymentRow | None`; `get_by_provider(provider_payment: str) -> PaymentRow | None`
  - `OutboxRepository`: `add(envelope: EventEnvelope, aggregate_type: str, aggregate_id: str) -> None`; `lock_pending(now: datetime, limit: int) -> list[OutboxRow]` (`published_at IS NULL AND next_attempt_at <= now ORDER BY created_at FOR UPDATE SKIP LOCKED`); `get_for_aggregate(aggregate_type: str, aggregate_id: str) -> OutboxRow | None`
  - `InboxRepository`: `try_mark_processed(consumer: str, event_id: UUID, at: datetime) -> bool`
  - `UnitOfWork(session_factory)`: async context manager; attributes `subscriptions notifications payments outbox inbox`; commits on clean exit, rolls back on exception, always closes. `UnitOfWorkFactory = Callable[[], UnitOfWork]`.

- [ ] **Step 1: Write failing tests** (SQL-specific behaviour only — plain CRUD is covered by service tests)

```python
async def test_uow_rolls_back_on_exception(uow_factory):
    with pytest.raises(RuntimeError):
        async with uow_factory() as uow:
            await uow.subscriptions.insert_if_absent(make_subscription("s1"))
            raise RuntimeError
    async with uow_factory() as uow:
        assert await uow.subscriptions.get("s1") is None

async def test_payment_insert_if_absent_returns_none_on_duplicate(uow_factory): ...
async def test_lock_due_skips_rows_locked_by_another_tx(uow_factory, clock):
    # 2 due rows; open uow A, lock_due(limit=1) -> 1 row; inside A, open uow B: lock_due(limit=10) -> the other row only
async def test_outbox_lock_pending_ignores_future_next_attempt_and_published(uow_factory, clock): ...
async def test_inbox_try_mark_processed_true_then_false(uow_factory, clock): ...
async def test_cancel_scheduled_reminders_ignores_payment_and_queued(uow_factory, clock):
    # rows: reminder scheduled, reminder queued, payment notification scheduled -> returns 1
```

- [ ] **Step 2: Run — expect FAIL** — `uv run pytest tests/integration/test_repositories.py -v`
- [ ] **Step 3: Implement** using `sqlalchemy.dialects.postgresql.insert(...).on_conflict_do_nothing(...)` and `select(...).with_for_update(skip_locked=True)`.
- [ ] **Step 4: Run — expect PASS**; `uv run lint-imports`
- [ ] **Step 5: Commit** — `feat(repositories): add repositories and unit of work`

---

### Task 5: Subscription service

**Files:**
- Create: `services/ports.py` (`Clock` Protocol: `now() -> datetime`), `services/dto.py`, `services/reminders.py`, `services/subscriptions.py`, `adapters/clock.py` (`SystemClock`)
- Test: `tests/integration/test_subscription_service.py`

**Interfaces:**
- Consumes: Task 2 domain functions, Task 4 `UnitOfWorkFactory`.
- Produces:
  - DTOs (frozen, slots): `NotificationView(id: UUID, event_name: str, scheduled_for: datetime, sent_at: datetime | None, status: NotificationStatus)`; `SubscriptionView(subscription_id, user_id, day_count, expected_expires_on, notifications: tuple[NotificationView, ...])`; `UpsertResult(view: SubscriptionView, created: bool)`
  - `reschedule_reminders(uow: UnitOfWork, subscription: SubscriptionRow, offsets_days: Sequence[int], now: datetime) -> None` — cancels scheduled reminders, adds rows for `reminder_slots(...)` with `target_expires_on = subscription.expected_expires_on`. Reused by Task 11's extender.
  - `SubscriptionService(uow_factory: UnitOfWorkFactory, clock: Clock, reminder_offsets_days: Sequence[int])`: `upsert(subscription_id: str, user_id: str, day_count: int, expected_expires_on: datetime) -> UpsertResult`; `get(subscription_id: str) -> SubscriptionView` (raises `NotFoundError`).
- Upsert algorithm: `insert_if_absent` → `get_for_update` → if not created: `user_id` mismatch → `ConflictError`; set `day_count`; `new = merge_expiry(...)`; only if `new != current` → update and `reschedule_reminders`. If created → `reschedule_reminders`. This makes concurrent creates safe (the loser blocks on the unique key, then sees the row).

- [ ] **Step 1: Write failing tests** (`svc` fixture: `SubscriptionService(uow_factory, clock, [3, 1, 0])`)

```python
async def test_create_schedules_three_reminders(svc, clock):
    exp = clock.now() + 10 * D
    res = await svc.upsert("s1", "u1", 30, exp)
    assert res.created is True
    assert [(n.event_name, n.scheduled_for, n.status) for n in res.view.notifications] == [
        ("subscription.expiring", exp - 3 * D, "scheduled"),
        ("subscription.expiring", exp - D, "scheduled"),
        ("subscription.expired", exp, "scheduled"),
    ]

async def test_later_expiry_cancels_and_reschedules(svc, clock):
    # upsert exp, then exp + 5D -> created False; statuses: 3 cancelled + 3 scheduled; expected_expires_on == exp + 5D

async def test_earlier_expiry_is_ignored_but_day_count_updates(svc, clock):
    # upsert(exp, day_count=30) then upsert(exp - 5D, day_count=60) -> expected_expires_on == exp; day_count == 60; 3 scheduled, 0 cancelled

async def test_same_expiry_leaves_reminders_untouched(svc, clock): ...   # same 3 ids, all scheduled
async def test_user_mismatch_conflicts(svc, clock):  # upsert u1 then u2 -> ConflictError
async def test_past_expiry_saves_without_reminders(svc, clock):          # Review Focus 4
    res = await svc.upsert("s1", "u1", 30, clock.now() - D)
    assert res.created and res.view.notifications == ()
async def test_get_unknown_raises_not_found(svc): ...
async def test_concurrent_creates_produce_one_subscription(svc, clock):
    results = await asyncio.gather(*(svc.upsert("s1", "u1", 30, clock.now() + 10 * D) for _ in range(2)))
    assert sorted(r.created for r in results) == [False, True]
    assert len((await svc.get("s1")).notifications) == 3
```

- [ ] **Step 2: Run — expect FAIL**
- [ ] **Step 3: Implement**; map rows → DTOs inside the service (rows never returned).
- [ ] **Step 4: Run — expect PASS**; `uv run mypy && uv run lint-imports`
- [ ] **Step 5: Commit** — `feat(services): add subscription upsert with reminder scheduling`

---

### Task 6: Payment service (transactional outbox write side)

**Files:**
- Create: `services/payments.py`; Modify: `services/dto.py`
- Test: `tests/integration/test_payment_service.py`

**Interfaces:**
- Produces:
  - DTOs: `RecordPaymentCommand(subscription_id: str, provider_payment: str, amount: Decimal, currency: str, status: PaymentStatus)`; `PaymentView(payment_id: UUID, subscription_id, provider_payment, amount: Decimal, currency, status: PaymentStatus, period_days: int, created_at: datetime)`; `RecordPaymentResult(view: PaymentView, created: bool)`; `EventDeliveryView(event_id: UUID, event_type: str, published_at: datetime | None, attempts: int, last_error: str | None)` with property `published -> bool`; `NotificationDeliveryView(status: NotificationStatus, sent_at: datetime | None)`; `PaymentStatusView(payment: PaymentView, event: EventDeliveryView, notification: NotificationDeliveryView | None)`
  - `PaymentService(uow_factory: UnitOfWorkFactory, clock: Clock)`: `record(cmd: RecordPaymentCommand) -> RecordPaymentResult`; `get_status(payment_id: UUID) -> PaymentStatusView`; `get_status_by_provider(provider_payment: str) -> PaymentStatusView` (both raise `NotFoundError`).
  - Outbox event: `aggregate_type="payment"`, `aggregate_id=str(payment_id)`, `event_type=payment.<status>`, payload `{"payment_id", "subscription_id", "provider_payment", "amount", "currency", "status", "period_days"}` — all strings except `period_days: int`; `amount` = `str(amount.quantize(Decimal("0.0001")))`.
  - `period_days` = the subscription's `day_count` read in the same transaction, stored on `payments.period_days` (Task 3 table gets this column, `CHECK (period_days > 0)`).
  - Identical-duplicate rule: same `subscription_id`, `currency`, `status`, and `Decimal` equality of `amount` (so `10 == 10.00`). `period_days` is **not** compared, because the client never sends it.

- [ ] **Step 1: Write failing tests** (fixture creates subscription `s1` via `SubscriptionService`)

```python
CMD = RecordPaymentCommand("s1", "pp-1", Decimal("10.00"), "USD", PaymentStatus.SUCCEEDED)

async def test_record_writes_payment_and_outbox_event(psvc):
    res = await psvc.record(CMD)
    status = await psvc.get_status(res.view.payment_id)
    assert res.created and status.event.event_type == "payment.succeeded"
    assert status.event.published is False and status.event.attempts == 0 and status.notification is None
    # and the outbox payload["amount"] == "10.0000", payload["period_days"] == 30 == res.view.period_days

async def test_period_is_snapshotted_at_record_time(psvc, subs, clock):
    res = await psvc.record(CMD)                                   # s1 has day_count 30
    await subs.upsert("s1", "u1", 60, clock.now() + 10 * D)        # later PUT changes the period
    assert (await psvc.get_status(res.view.payment_id)).payment.period_days == 30

async def test_outbox_failure_rolls_back_payment(psvc, monkeypatch):
    monkeypatch.setattr(OutboxRepository, "add", AsyncMock(side_effect=RuntimeError("db down")))
    with pytest.raises(RuntimeError): await psvc.record(CMD)
    monkeypatch.undo()
    with pytest.raises(NotFoundError): await psvc.get_status_by_provider("pp-1")

async def test_identical_duplicate_returns_existing(psvc):          # 1 payment, 1 outbox row, created False
async def test_duplicate_with_equal_amount_other_scale_is_identical(psvc):   # Review Focus 3
    await psvc.record(CMD)
    assert (await psvc.record(replace(CMD, amount=Decimal("10")))).created is False
async def test_duplicate_with_different_amount_conflicts(psvc):     # Decimal("11") -> ConflictError
async def test_concurrent_duplicates_single_payment(psvc):           # Review Focus 2
    results = await asyncio.gather(psvc.record(CMD), psvc.record(CMD))
    assert sorted(r.created for r in results) == [False, True]   # and exactly 1 outbox row
async def test_unknown_subscription_not_found(psvc): ...
```

- [ ] **Step 2: Run — expect FAIL**
- [ ] **Step 3: Implement** — one `UnitOfWork`: subscription `get` (404) → `insert_if_absent` → if `None`, load by provider and compare → else `outbox.add(...)`.
- [ ] **Step 4: Run — expect PASS**
- [ ] **Step 5: Commit** — `feat(services): record payments with transactional outbox`

---

### Task 7: HTTP API

**Files:**
- Create: `api/app.py`, `api/deps.py`, `api/schemas.py`, `api/errors.py`, `api/routers/{subscriptions,payments,health}.py`
- Modify: `tests/conftest.py` (add `api_client`)
- Test: `tests/integration/test_api.py`

**Interfaces:**
- Consumes: `SubscriptionService`, `PaymentService`, DTOs, `SystemClock`, `create_engine`, `create_session_factory`.
- Produces:
  - `create_app(settings: Settings | None = None) -> FastAPI`; module-level `app = create_app()`. Lifespan builds engine + session factory into `app.state`, disposes on shutdown, calls `configure_logging`. `FastAPI(max_body_size=64 * 1024, ...)`.
  - `deps.py` (composition root): `get_settings`, `get_session_factory(request) -> async_sessionmaker`, `get_clock() -> Clock` (returns `SystemClock()`), `get_subscription_service`, `get_payment_service`; aliases `SessionFactoryDep`, `ClockDep`, `SubscriptionServiceDep`, `PaymentServiceDep`.
  - Schemas: `SubscriptionUpsertIn(user_id: str = Field(min_length=1, max_length=255), day_count: int = Field(gt=0, le=3650), expected_expires_on: AwareDatetime)`; `NotificationOut`; `SubscriptionOut`; `PaymentIn(subscription_id: str (1..255), provider_payment: str (1..255), amount: Decimal = Field(gt=0, max_digits=19, decimal_places=4), currency: str = Field(pattern=r"^[A-Z]{3}$"), status: PaymentStatus)`; `PaymentOut`; `EventDeliveryOut(event_id, event_type, published, published_at, attempts, last_error)`; `NotificationDeliveryOut(status, sent_at)`; `PaymentStatusOut(<PaymentOut fields>, event: EventDeliveryOut, notification: NotificationDeliveryOut | None)`. Built from DTOs with `model_validate(..., from_attributes=True)`.
  - Routes (routers declare `prefix`/`tags`): `PUT /subscriptions/{subscription_id} -> SubscriptionOut` (201/200 via injected `Response`), `GET /subscriptions/{subscription_id} -> SubscriptionOut`, `POST /payments -> PaymentOut` (201/200), `GET /payments/{payment_id} -> PaymentStatusOut`, `GET /payments?provider_payment=… -> PaymentStatusOut`, `GET /health/live`, `GET /health/ready` (`SELECT 1`; 503 on failure).
  - `errors.py`: `register_error_handlers(app)` — `NotFoundError → 404`, `ConflictError → 409`, body `{"detail": str(exc)}`.
  - `api_client` fixture: `create_app(Settings(database_url=pg_url, rabbitmq_url="amqp://guest:guest@127.0.0.1:1/"))` with `dependency_overrides` for `get_session_factory` (test factory) and `get_clock` (`FakeClock`); `httpx.AsyncClient(transport=ASGITransport(app), base_url="http://test")`.

- [ ] **Step 1: Write failing tests**

```python
SUB = {"user_id": "u1", "day_count": 30, "expected_expires_on": "2026-02-01T00:00:00Z"}
PAY = {"subscription_id": "s1", "provider_payment": "pp-1", "amount": "10.00", "currency": "USD", "status": "succeeded"}

async def test_put_subscription_201_then_200(api_client):
    r1 = await api_client.put("/subscriptions/s1", json=SUB)
    assert r1.status_code == 201
    assert set(r1.json()["notifications"][0]) == {"id", "event_name", "scheduled_for", "sent_at", "status"}
    assert (await api_client.put("/subscriptions/s1", json=SUB)).status_code == 200

@pytest.mark.parametrize("patch", [{"expected_expires_on": "2026-02-01T00:00:00"}, {"day_count": 0}, {"user_id": ""}])
async def test_put_subscription_validation_422(api_client, patch): ...
async def test_get_unknown_subscription_404(api_client): ...
async def test_put_other_user_409(api_client): ...

async def test_post_payment_201_200_409(api_client):     # after PUT s1; same body -> 200; amount "11" -> 409
async def test_post_payment_succeeds_with_broker_unreachable(api_client):  # rabbitmq_url points at :1 -> still 201
@pytest.mark.parametrize("patch", [{"currency": "usd"}, {"amount": "0"}, {"amount": "1.00001"}, {"status": "pending"}])
async def test_post_payment_validation_422(api_client, patch): ...
async def test_post_payment_unknown_subscription_404(api_client): ...

async def test_get_payment_status(api_client):
    pid = (await api_client.post("/payments", json=PAY)).json()["payment_id"]
    body = (await api_client.get(f"/payments/{pid}")).json()
    assert Decimal(body["amount"]) == Decimal("10.00")
    assert body["event"]["published"] is False and body["event"]["event_type"] == "payment.succeeded"
    assert body["notification"] is None
    assert (await api_client.get("/payments", params={"provider_payment": "pp-1"})).json()["payment_id"] == pid

async def test_health_ready(api_client): ...       # 200
async def test_oversized_body_413(api_client):     # 100 KiB user_id -> 413
```

- [ ] **Step 2: Run — expect FAIL**
- [ ] **Step 3: Implement** following the fastapi conventions in Global Constraints.
- [ ] **Step 4: Run full suite & gates — expect PASS**

Run: `make check`

- [ ] **Step 5: Smoke run** — with a local Postgres, `DATABASE_URL=… uv run alembic upgrade head && uv run fastapi dev` → open `/docs`, the five routes appear.
- [ ] **Step 6: Commit** — `feat(api): expose subscription and payment endpoints`
