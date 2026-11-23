"""Happy path against a running `docker compose` stack (api + worker + consumers)."""

import asyncio
import os
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import httpx
import pytest

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif("E2E_BASE_URL" not in os.environ, reason="needs a running compose stack"),
]


async def poll(
    client: httpx.AsyncClient, url: str, done: Callable[[Any], bool], limit_s: float
) -> Any:
    async with asyncio.timeout(limit_s):
        while True:
            body = (await client.get(url)).json()
            if done(body):
                return body
            await asyncio.sleep(0.5)


async def test_full_flow_against_running_stack() -> None:
    async with httpx.AsyncClient(base_url=os.environ["E2E_BASE_URL"], timeout=10) as c:
        sid, exp = f"e2e-{uuid4()}", datetime.now(UTC) + timedelta(days=10)
        put = await c.put(
            f"/subscriptions/{sid}",
            json={"user_id": "u1", "day_count": 30, "expected_expires_on": exp.isoformat()},
        )
        assert put.status_code == 201
        post = await c.post(
            "/payments",
            json={
                "subscription_id": sid,
                "provider_payment": f"pp-{uuid4()}",
                "amount": "9.99",
                "currency": "USD",
                "status": "succeeded",
            },
        )
        assert post.status_code == 201
        pid = post.json()["payment_id"]

        await poll(
            c,
            f"/payments/{pid}",
            lambda b: b["event"]["published"] and (b["notification"] or {}).get("status") == "sent",
            limit_s=30,
        )
        sub = await poll(
            c,
            f"/subscriptions/{sid}",
            lambda b: datetime.fromisoformat(b["expected_expires_on"]) == exp + timedelta(days=30),
            limit_s=10,
        )
        assert [n["status"] for n in sub["notifications"]].count("scheduled") == 3
