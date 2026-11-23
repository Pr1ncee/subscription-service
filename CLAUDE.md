# Project rules for agents

Subscription payments & notifications service. Design: `docs/superpowers/specs/2026-11-23-subscription-payments-design.md`.
Implementation plans: `docs/superpowers/plans/`.

## Architecture rules (enforced, not advisory)

- **Layers:** `api | worker | consumers → adapters → services → repositories → db → domain`.
  A layer imports only layers below it. `domain` imports nothing from the project or from frameworks.
  `uv run lint-imports` enforces this (also part of `make typecheck` and CI).
- **Inner layers are framework-free:** `domain`, `services`, `repositories` never import
  `fastapi`, `faststream`, `aio_pika` or `subscriptions_service.config`.
- **Composition roots** are exactly `api/deps.py`, `worker/main.py`, `consumers/app.py`.
  Nothing else instantiates adapters (`SystemClock`, `RabbitPublisher`, `LoggingSender`, …).
  Side effects enter services through `Protocol` ports in `services/ports.py`.
- **ORM rows never leave `services`.** Services return frozen dataclass DTOs; the API maps them to Pydantic schemas.

## Delivery-guarantee rules

- **Outbox rule:** never publish to the broker from `api` or `services`. A new event is written with
  `OutboxRepository.add(...)` **in the same `UnitOfWork`** as the state change it describes.
  Only the worker's relay publishes.
- **Inbox rule:** every consumer has a `CONSUMER_NAME` and calls `uow.inbox.try_mark_processed(...)`
  first, in the same transaction as its side effects. Consumers must be safe under redelivery.
- **Acks:** consumers use `AckPolicy.MANUAL` and settle only through `consumers/acking.py::settle`.
  Retries are `reject(requeue=True)`, never `nack` (RabbitMQ 4.3 does not count requeued nacks
  toward the delivery limit).
- **Lock order** is subscription → notification in every transaction that touches both.
- Queue arguments come only from `adapters/rabbit_topology.py` (`rabbit_queue()`, `dead_letter_queue()`);
  never declare a project queue any other way (PRECONDITION_FAILED otherwise).
- Before adding or changing an event type, outbox write, queue or consumer: apply the
  `outbox-checklist` skill (`.claude/skills/outbox-checklist/SKILL.md`).

## Commands

| Task | Command |
|---|---|
| Format | `make format` (black, then `ruff check --fix`) |
| Lint | `make lint` (black --check + ruff) |
| Types + layer contracts | `make typecheck` (mypy strict + import-linter) |
| Tests | `make test` (Docker required: testcontainers start Postgres and RabbitMQ) |
| Everything | `make check` = lint + typecheck + test |
| Slow/broker-pause tests too | `uv run pytest -m "slow or not slow"` |
| Full stack | `docker compose up -d --build --wait` |
| E2E against the stack | `E2E_BASE_URL=http://localhost:8000 uv run pytest -m e2e` |

**black is the formatter; ruff is lint-only. Never run `ruff format`.**

## CI

`.github/workflows/ci.yml` runs three jobs: `quality` (make lint, make typecheck, pip-audit),
`test` (all tests incl. slow, coverage ≥ 85 %), `docker` (hadolint, image build, compose up, e2e).
CI runs exactly the `make` targets, so a green `make check` locally predicts a green CI.

## Skills to apply

- `fastapi` — `Annotated` dependencies, return-type annotations, router prefixes/tags.
- `clean-architecture` (in this repo: `.claude/skills/clean-architecture/`) — for design and review.
- `python-patterns` — full type hints, `StrEnum`, frozen slotted dataclasses, `raise … from e`.
- `outbox-checklist` (in this repo) — for any event/queue/consumer change.

## Tests

TDD: write the failing test first and watch it fail. Integration tests run against real Postgres
and RabbitMQ (pinned `rabbitmq:4.3.8-management-alpine`, same tag as compose). Time is injected
(`FakeClock` in tests); never call `datetime.now()` outside `adapters/clock.py`.

## Commits

Conventional Commits (`feat(scope): …`, `fix: …`, `test: …`, `build: …`, `ci: …`, `docs: …`).
No AI attribution trailers. Never push or open PRs without the user's explicit approval.
