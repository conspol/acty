"""Manual TUI console bindings check (real terminal).

Run from repo root:
  python examples/tui_console_bindings_manual_check.py --no-console
  python examples/tui_console_bindings_manual_check.py --console

See tui_console_bindings_manual_check.md for step-by-step expectations.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import time

from acty import create_tui_app
from acty_core.events.types import Event


async def emit_events(queue: asyncio.Queue[Event], *, interval_s: float) -> None:
    seq = 0
    while True:
        seq += 1
        await queue.put(
            Event(
                schema_version=1,
                run_id="tui-bindings-check",
                seq=seq,
                ts=time.time(),
                type="job_queued",
                job_id=f"job-{seq}",
                group_id="g1",
                kind="primer",
            )
        )
        await asyncio.sleep(interval_s)


async def emit_console_logs(*, interval_s: float) -> None:
    logger = logging.getLogger("acty.tui.console.manual")
    counter = 0
    while True:
        print(f"console print {counter}")
        logger.info("console log %s", counter)
        counter += 1
        await asyncio.sleep(interval_s)


async def run_manual_check(*, enable_console: bool, event_interval_s: float, console_interval_s: float) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    event_queue: asyncio.Queue[Event] = asyncio.Queue()
    console_queue: asyncio.Queue[str] | None = asyncio.Queue() if enable_console else None

    app = create_tui_app(
        event_queue,
        console_log_queue=console_queue,
        poll_interval_s=0.05,
        stats_interval_s=0.25,
        plot_interval_s=1.0,
    )

    event_task = asyncio.create_task(emit_events(event_queue, interval_s=event_interval_s))
    console_task = None
    if enable_console:
        console_task = asyncio.create_task(emit_console_logs(interval_s=console_interval_s))

    try:
        await app.run_async()
    finally:
        tasks = [event_task]
        if console_task is not None:
            tasks.append(console_task)
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def main() -> None:
    parser = argparse.ArgumentParser(description="Manual TUI console bindings check.")
    parser.add_argument(
        "--console",
        dest="console",
        action="store_true",
        help="Enable console capture and console bindings.",
    )
    parser.add_argument(
        "--no-console",
        dest="console",
        action="store_false",
        help="Disable console capture and console bindings.",
    )
    parser.set_defaults(console=False)
    parser.add_argument(
        "--event-interval-s",
        type=float,
        default=0.5,
        help="Seconds between scheduler events.",
    )
    parser.add_argument(
        "--console-interval-s",
        type=float,
        default=1.0,
        help="Seconds between console log lines (when console capture is enabled).",
    )
    args = parser.parse_args()

    asyncio.run(
        run_manual_check(
            enable_console=args.console,
            event_interval_s=args.event_interval_s,
            console_interval_s=args.console_interval_s,
        )
    )


if __name__ == "__main__":
    main()
