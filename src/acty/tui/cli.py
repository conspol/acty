"""CLI entrypoint for acty TUI (replay/follow JSONL events)."""

from __future__ import annotations

import argparse
import asyncio

from acty.tui import TuiState, create_tui_app
from acty.tui.sources import FileEventSource, ReplayEventSource


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="acty-tui", description="Run acty TUI.")
    sub = parser.add_subparsers(dest="cmd", required=True)

    replay = sub.add_parser("replay", help="Replay events from a JSONL file.")
    replay.add_argument("path", type=str)
    replay.add_argument("--speed", type=float, default=1.0, help="Replay speed (1.0 = real time).")
    replay.add_argument("--last-n", type=int, default=None, help="Replay only the last N events.")
    replay.add_argument(
        "--start-from-ts",
        type=float,
        default=None,
        help="Replay events with timestamp >= this value.",
    )
    replay.add_argument("--queue-size", type=int, default=5000)

    follow = sub.add_parser("follow", help="Follow events appended to a JSONL file.")
    follow.add_argument("path", type=str)
    follow.add_argument("--from-start", action="store_true", help="Read existing events first.")
    follow.add_argument("--last-n", type=int, default=None, help="Start by reading the last N events.")
    follow.add_argument(
        "--start-from-ts",
        type=float,
        default=None,
        help="Start by reading events with timestamp >= this value.",
    )
    follow.add_argument("--poll-interval", type=float, default=0.2)
    follow.add_argument("--queue-size", type=int, default=5000)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.cmd == "replay":
        source = FileEventSource(
            args.path,
            follow=False,
            last_n=args.last_n,
            start_from_ts=args.start_from_ts,
        )
        source = ReplayEventSource(source, speed=args.speed)

        async def runner() -> None:
            state = TuiState()
            app = create_tui_app(source, state=state, queue_maxsize=args.queue_size)
            await app.run_async()

        asyncio.run(runner())
        return 0

    if args.cmd == "follow":
        last_n = args.last_n
        start_from_ts = args.start_from_ts
        if args.from_start and last_n is None and start_from_ts is None:
            last_n = None
        elif not args.from_start and last_n is None and start_from_ts is None:
            last_n = 0

        source = FileEventSource(
            args.path,
            follow=True,
            poll_interval=args.poll_interval,
            last_n=last_n,
            start_from_ts=start_from_ts,
        )

        async def runner() -> None:
            state = TuiState()
            app = create_tui_app(source, state=state, queue_maxsize=args.queue_size)
            await app.run_async()

        asyncio.run(runner())
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
