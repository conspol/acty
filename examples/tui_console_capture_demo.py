"""Demo: in-process console capture with print + logging while running the TUI."""

from __future__ import annotations

import asyncio
import logging
import time

from acty import create_tui_app
from acty_core.events.types import Event


async def emit_events(queue: asyncio.Queue[Event]) -> None:
    seq = 0
    while True:
        seq += 1
        await queue.put(
            Event(
                schema_version=1,
                run_id="console-demo",
                seq=seq,
                ts=time.time(),
                type="job_queued",
                job_id=f"job-{seq}",
                group_id="g1",
                kind="primer",
            )
        )
        await asyncio.sleep(0.5)


async def emit_console_logs() -> None:
    logger = logging.getLogger("acty.console.demo")
    counter = 0
    while True:
        print(f"console print {counter}")
        logger.info("console log %s", counter)
        counter += 1
        await asyncio.sleep(1.0)


async def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    event_queue: asyncio.Queue[Event] = asyncio.Queue()
    console_log_queue: asyncio.Queue[str] = asyncio.Queue()
    app = create_tui_app(
        event_queue,
        console_log_queue=console_log_queue,
        poll_interval_s=0.05,
        stats_interval_s=0.25,
        plot_interval_s=1.0,
    )

    event_task = asyncio.create_task(emit_events(event_queue))
    console_task = asyncio.create_task(emit_console_logs())
    try:
        await app.run_async()
    finally:
        for task in (event_task, console_task):
            task.cancel()
        await asyncio.gather(event_task, console_task, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
