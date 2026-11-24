"""Process helpers for the worker."""

import asyncio
import signal


def stop_on_signals() -> asyncio.Event:
    """An event that is set on SIGTERM or SIGINT, for a graceful shutdown."""
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    return stop
