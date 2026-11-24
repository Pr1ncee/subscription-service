import asyncio

from subscriptions_service.worker.loop import run_periodic


async def test_run_periodic_survives_step_exception() -> None:
    calls = 0
    stop = asyncio.Event()

    async def step() -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("db blip")
        if calls == 3:
            stop.set()
        return 0

    await asyncio.wait_for(run_periodic(step, interval_s=0.01, name="t", stop=stop), 1)
    assert calls == 3


async def test_run_periodic_repeats_immediately_while_work_remains() -> None:
    """While a step reports work it runs again at once: the long interval (10 s)
    proves the first two calls did not sleep.
    """
    results = iter([5, 5, 0])
    calls = 0
    stop = asyncio.Event()

    async def step() -> int:
        nonlocal calls
        calls += 1
        result = next(results, 0)
        if result == 0:
            stop.set()
        return result

    await asyncio.wait_for(run_periodic(step, interval_s=10, name="t", stop=stop), 1)
    assert calls == 3


async def test_run_periodic_wakes_promptly_on_stop() -> None:
    stop = asyncio.Event()

    async def step() -> int:
        return 0

    task = asyncio.create_task(run_periodic(step, interval_s=60, name="t", stop=stop))
    await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, 1)
