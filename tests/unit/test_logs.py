import json
import logging

import pytest

from subscriptions_service.logs import configure_logging


def test_configure_logging_emits_one_json_line_with_extras(
    capsys: pytest.CaptureFixture[str],
) -> None:
    configure_logging("debug")
    logging.getLogger("svc.test").info("hello %s", "world", extra={"payment_id": "p1"})

    lines = capsys.readouterr().err.strip().splitlines()
    assert len(lines) == 1
    entry = json.loads(lines[0])
    assert entry["level"] == "INFO"
    assert entry["logger"] == "svc.test"
    assert entry["msg"] == "hello world"
    assert entry["payment_id"] == "p1"
    assert "ts" in entry
    assert logging.getLogger().level == logging.DEBUG
