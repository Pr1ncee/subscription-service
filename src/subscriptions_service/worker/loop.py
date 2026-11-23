import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)


async def run_periodic(
    step: Callable[[], Awaitable[int]], *, interval_s: float, name: str, stop: asyncio.Event
) -> None:
    """Call `step` until `stop` is set.

    While `step` reports work done (> 0) it is called again immediately; otherwise the
    loop waits `interval_s` or until stopped. A failing step is logged and retried on
    the next tick, so a transient DB/broker error never kills the loop.
    """
    while not stop.is_set():
        try:
            done = await step()
        except Exception:
            logger.exception("loop step failed", extra={"loop": name})
            done = 0
        if done > 0:
            continue
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), timeout=interval_s)
