import uuid
from decimal import Decimal
from typing import Any

import httpx
import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from subscriptions_service.api.app import create_app
from subscriptions_service.api.deps import get_session_factory
from subscriptions_service.config import Settings
from subscriptions_service.db.engine import create_session_factory
from tests.conftest import UNREACHABLE_BROKER

SUB = {"user_id": "u1", "day_count": 30, "expected_expires_on": "2026-02-01T00:00:00Z"}
PAY = {
    "subscription_id": "s1",
    "provider_payment": "pp-1",
    "amount": "10.00",
    "currency": "USD",
    "status": "succeeded",
}


async def _put_s1(client: httpx.AsyncClient) -> None:
    assert (await client.put("/subscriptions/s1", json=SUB)).status_code == 201


async def test_put_subscription_201_then_200(api_client: httpx.AsyncClient) -> None:
    r1 = await api_client.put("/subscriptions/s1", json=SUB)
    assert r1.status_code == 201
    body = r1.json()
    assert (body["subscription_id"], body["user_id"], body["day_count"]) == ("s1", "u1", 30)
    assert len(body["notifications"]) == 3
    assert set(body["notifications"][0]) == {
        "id",
        "event_name",
        "scheduled_for",
        "sent_at",
        "status",
    }
    r2 = await api_client.put("/subscriptions/s1", json=SUB)
    assert r2.status_code == 200
    assert r2.json() == body
    assert (await api_client.get("/subscriptions/s1")).json() == body


@pytest.mark.parametrize(
    "patch",
    [
        {"expected_expires_on": "2026-02-01T00:00:00"},
        {"day_count": 0},
        {"day_count": 3651},
        {"user_id": ""},
    ],
)
async def test_put_subscription_validation_422(
    api_client: httpx.AsyncClient, patch: dict[str, Any]
) -> None:
    r = await api_client.put("/subscriptions/s1", json={**SUB, **patch})
    assert r.status_code == 422


async def test_get_unknown_subscription_404(api_client: httpx.AsyncClient) -> None:
    r = await api_client.get("/subscriptions/nope")
    assert r.status_code == 404
    assert "nope" in r.json()["detail"]


async def test_put_other_user_409(api_client: httpx.AsyncClient) -> None:
    await _put_s1(api_client)
    r = await api_client.put("/subscriptions/s1", json={**SUB, "user_id": "u2"})
    assert r.status_code == 409
    assert "detail" in r.json()


async def test_post_payment_201_200_409(api_client: httpx.AsyncClient) -> None:
    await _put_s1(api_client)
    r1 = await api_client.post("/payments", json=PAY)
    assert r1.status_code == 201
    assert set(r1.json()) == {
        "payment_id",
        "subscription_id",
        "provider_payment",
        "amount",
        "currency",
        "status",
        "created_at",
    }
    r2 = await api_client.post("/payments", json=PAY)
    assert r2.status_code == 200
    assert r2.json() == r1.json()
    assert (await api_client.post("/payments", json={**PAY, "amount": "11"})).status_code == 409


async def test_post_payment_succeeds_with_broker_unreachable(
    api_client: httpx.AsyncClient,
) -> None:
    # The client's settings point RabbitMQ at a closed port; the API never talks to it.
    await _put_s1(api_client)
    assert (await api_client.post("/payments", json=PAY)).status_code == 201


@pytest.mark.parametrize(
    "patch",
    [
        {"currency": "usd"},
        {"amount": "0"},
        {"amount": "-1"},
        {"amount": "1.00001"},
        {"status": "pending"},
        {"provider_payment": ""},
    ],
)
async def test_post_payment_validation_422(
    api_client: httpx.AsyncClient, patch: dict[str, Any]
) -> None:
    await _put_s1(api_client)
    assert (await api_client.post("/payments", json={**PAY, **patch})).status_code == 422


async def test_post_payment_unknown_subscription_404(api_client: httpx.AsyncClient) -> None:
    assert (await api_client.post("/payments", json=PAY)).status_code == 404


async def test_get_payment_status(api_client: httpx.AsyncClient) -> None:
    await _put_s1(api_client)
    pid = (await api_client.post("/payments", json=PAY)).json()["payment_id"]
    r = await api_client.get(f"/payments/{pid}")
    assert r.status_code == 200
    body = r.json()
    assert Decimal(body["amount"]) == Decimal("10.00")
    assert body["event"]["published"] is False
    assert body["event"]["event_type"] == "payment.succeeded"
    assert set(body["event"]) == {
        "event_id",
        "event_type",
        "published",
        "published_at",
        "attempts",
        "last_error",
    }
    assert body["notification"] is None
    by_provider = await api_client.get("/payments", params={"provider_payment": "pp-1"})
    assert by_provider.json() == body


async def test_get_payment_status_unknown_404(api_client: httpx.AsyncClient) -> None:
    assert (await api_client.get(f"/payments/{uuid.uuid4()}")).status_code == 404
    r = await api_client.get("/payments", params={"provider_payment": "nope"})
    assert r.status_code == 404
    assert (await api_client.get("/payments/not-a-uuid")).status_code == 422


async def test_health_live_and_ready(api_client: httpx.AsyncClient) -> None:
    assert (await api_client.get("/health/live")).status_code == 200
    r = await api_client.get("/health/ready")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


async def test_health_ready_503_when_db_unreachable(pg_url: str) -> None:
    app = create_app(Settings(database_url=pg_url, rabbitmq_url=UNREACHABLE_BROKER))
    dead = create_async_engine("postgresql+asyncpg://u:p@127.0.0.1:1/db")
    app.dependency_overrides[get_session_factory] = lambda: create_session_factory(dead)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        r = await client.get("/health/ready")
    await dead.dispose()
    assert r.status_code == 503


async def test_oversized_body_413(api_client: httpx.AsyncClient) -> None:
    r = await api_client.put("/subscriptions/s1", json={**SUB, "user_id": "x" * 100 * 1024})
    assert r.status_code == 413


async def test_lifespan_wires_real_dependencies(
    pg_url: str, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    app = create_app(Settings(database_url=pg_url, rabbitmq_url=UNREACHABLE_BROKER))
    transport = httpx.ASGITransport(app=app)
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            assert (await client.get("/health/ready")).status_code == 200
            assert (await client.put("/subscriptions/s1", json=SUB)).status_code == 201
            assert (await client.post("/payments", json=PAY)).status_code == 201


def test_openapi_lists_routes() -> None:
    app = create_app(Settings(database_url="postgresql+asyncpg://u:p@h/db", rabbitmq_url="amqp://"))
    paths = app.openapi()["paths"]
    assert set(paths) == {
        "/subscriptions/{subscription_id}",
        "/payments",
        "/payments/{payment_id}",
        "/health/live",
        "/health/ready",
    }


@pytest.mark.parametrize(
    ("method", "url", "body"),
    [
        ("PUT", "/subscriptions/a%00b", SUB),
        ("GET", "/subscriptions/a%00b", None),
        ("PUT", "/subscriptions/s1", {**SUB, "user_id": "a\u0000b"}),
        ("POST", "/payments", {**PAY, "provider_payment": "x\u0000y"}),
        ("POST", "/payments", {**PAY, "subscription_id": "x\u0000y"}),
        ("GET", "/payments?provider_payment=a%00b", None),
    ],
)
async def test_nul_byte_in_text_input_is_422(
    api_client: httpx.AsyncClient, method: str, url: str, body: dict[str, Any] | None
) -> None:
    r = await api_client.request(method, url, json=body)
    assert r.status_code == 422


@pytest.mark.parametrize(
    "expires",
    [
        "0001-01-01T00:00:00Z",
        "0001-01-02T00:00:00+05:00",
        "0001-01-01T00:00:00+00:30",
        "9999-12-31T23:59:59-05:00",
        "9001-01-01T00:00:00Z",
        "1969-12-31T23:59:59Z",
    ],
)
async def test_out_of_range_expiry_is_422(api_client: httpx.AsyncClient, expires: str) -> None:
    r = await api_client.put("/subscriptions/s1", json={**SUB, "expected_expires_on": expires})
    assert r.status_code == 422


async def test_put_response_is_utc_and_matches_get(api_client: httpx.AsyncClient) -> None:
    await _put_s1(api_client)
    r = await api_client.put(
        "/subscriptions/s1", json={**SUB, "expected_expires_on": "2026-03-01T00:00:00+03:00"}
    )
    assert r.status_code == 200
    assert r.json()["expected_expires_on"] == "2026-02-28T21:00:00Z"
    assert r.json() == (await api_client.get("/subscriptions/s1")).json()
