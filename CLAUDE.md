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

## Code conventions

- **Limits and tunings live in `config.py`:** deployment settings in `Settings` (env), fixed limits and
  timings (body size, id length, patterns, expiry years, consumer prefetch and retry delays) as
  documented `Final` constants. Only `api`, `worker` and `consumers` read them; inner layers take values
  as arguments.
- **Helpers live in the package's `utils.py`** (`api/`, `services/`, `consumers/`, `db/`, `worker/`),
  not as private functions inside feature modules. Enums live in an `enums.py` (`domain/enums.py`,
  `consumers/enums.py`).
- **No `#` comments.** Explain the "why" in docstrings (module, class, function; attribute docstrings
  for constants). Bare tool directives (`# noqa: …`, `# type: ignore[…]`, `# pragma: no cover`) are
  the only exception. ruff `D101`–`D103` requires docstrings on public classes, methods and functions.

## Delivery-guarantee rules

- **Outbox rule:** never publish to the broker from `api` or `services`. A new event is written with
  `OutboxRepository.add(...)` **in the same `UnitOfWork`** as the state change it describes.
  Only the worker's relay publishes.
- **Inbox rule:** every consumer has a `CONSUMER_NAME` and calls `uow.inbox.try_mark_processed(...)`
  first, in the same transaction as its side effects. Consumers must be safe under redelivery.
- **Acks:** consumers use `AckPolicy.MANUAL` and settle only through `consumers/acking.py::settle`;
  every path must settle exactly once. Payload problems raise `InvalidEnvelopeError` (poison → DLQ);
  DB connection errors are waited out in-process (they must not spend the delivery limit).
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
| Tests | `make test` — all but e2e, incl. `slow` (Docker required: testcontainers start Postgres and RabbitMQ) |
| Everything | `make check` = lint + typecheck + test |
| Full stack | `docker compose up -d --build --wait` |
| E2E against the stack | `E2E_BASE_URL=http://localhost:8000 uv run pytest -m e2e` |

**black is the formatter; ruff is lint-only. Never run `ruff format`.**

## CI

`.github/workflows/ci.yml` runs three jobs: `quality` (make lint, make typecheck, pip-audit),
`test` (all tests incl. slow, coverage ≥ 85 %), `docker` (hadolint, image build, compose up, e2e).
`quality` runs `make lint` + `make typecheck`; `test` runs the same tests as `make test`; so a green
`make check` predicts those two jobs. pip-audit, hadolint, the image build and the compose e2e run only
in CI — run their workflow commands locally when you touch dependencies, the Dockerfile or compose.

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
