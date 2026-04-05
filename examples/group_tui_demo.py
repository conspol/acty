"""Group-only TUI demo harness for acty."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import random
import time
from dataclasses import dataclass, field
from pathlib import Path

import structlog

from acty import TuiState, create_tui_app
from acty.tui.sources import QueueEventSource
from acty_core.core.types import Job, JobResult
from acty_core.events import EventBus, JsonlEventSink
from acty_core.events.sinks.base import EventSink
from acty_core.events.types import Event
from acty_core.lifecycle import (
    FollowerDispatchPolicy,
    GroupLifecycleController,
    GroupSpec,
    GroupTaskKind,
)
from acty_core.results import AsyncQueueResultSink
from acty_core.scheduler import PoolConfig, WorkStealingScheduler

logger = structlog.get_logger(__name__)


class EventQueueSink(EventSink):
    def __init__(
        self,
        queue: asyncio.Queue[Event],
        *,
        drop_policy: str = "drop_oldest",
        drop_log_interval_s: float = 2.0,
    ) -> None:
        if drop_policy not in {"drop_oldest", "drop_newest", "block"}:
            raise ValueError("drop_policy must be block, drop_oldest, or drop_newest")
        self._queue = queue
        self._drop_policy = drop_policy
        self._dropped = 0
        self._drop_log_interval_s = drop_log_interval_s
        self._last_drop_log = 0.0

    @property
    def dropped(self) -> int:
        return self._dropped

    async def handle(self, event: Event) -> None:
        if self._drop_policy == "block":
            await self._queue.put(event)
            return
        try:
            self._queue.put_nowait(event)
        except asyncio.QueueFull:
            if self._drop_policy == "drop_newest":
                self._dropped += 1
                now = time.monotonic()
                if now - self._last_drop_log >= self._drop_log_interval_s:
                    self._last_drop_log = now
                    logger.debug(
                        "Event queue full in demo sink",
                        drop_policy=self._drop_policy,
                        dropped=self._dropped,
                        queue_maxsize=self._queue.maxsize,
                        queue_size=self._queue.qsize(),
                    )
                return
            try:
                _ = self._queue.get_nowait()
                self._queue.task_done()
                self._dropped += 1
            except asyncio.QueueEmpty:
                self._dropped += 1
                now = time.monotonic()
                if now - self._last_drop_log >= self._drop_log_interval_s:
                    self._last_drop_log = now
                    logger.debug(
                        "Event queue empty while dropping oldest in demo sink",
                        drop_policy=self._drop_policy,
                        dropped=self._dropped,
                        queue_maxsize=self._queue.maxsize,
                        queue_size=self._queue.qsize(),
                    )
                return
            with contextlib.suppress(asyncio.QueueFull):
                self._queue.put_nowait(event)


@dataclass(frozen=True)
class DemoConfig:
    groups: int = 3
    followers_per_group: int = 5
    open_groups: bool = False
    warm_delay_s: float = 0.0
    max_followers_inflight: int = 2
    follower_dispatch_policy: FollowerDispatchPolicy = field(default_factory=FollowerDispatchPolicy)
    primer_workers: int = 1
    follower_workers: int = 2
    min_delay_s: float = 0.01
    max_delay_s: float = 0.05
    event_jsonl: str | None = None
    seed: int = 0
    result_queue_size: int = 0
    result_drop_policy: str = "drop_oldest"
    event_queue_size: int = 5000
    event_drop_policy: str = "drop_oldest"


class DemoExecutor:
    def __init__(
        self,
        controller: GroupLifecycleController,
        *,
        rng: random.Random,
        min_delay_s: float,
        max_delay_s: float,
    ) -> None:
        self._controller = controller
        self._rng = rng
        self._min_delay_s = min_delay_s
        self._max_delay_s = max_delay_s

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        group_id = str(job.group_id) if job.group_id is not None else ""
        delay = self._rng.uniform(self._min_delay_s, self._max_delay_s)
        if job.kind == GroupTaskKind.PRIMER.value:
            await self._controller.mark_primer_started(group_id)
            await asyncio.sleep(delay)
            await self._controller.mark_primer_done(group_id)
        elif job.kind == GroupTaskKind.FOLLOWER.value:
            await asyncio.sleep(delay)
            await self._controller.mark_follower_done(group_id)
        else:
            await asyncio.sleep(delay)
        return JobResult(job_id=job.id, kind=job.kind, ok=True, output={"payload": job.payload})


async def _drain_results(
    queue: asyncio.Queue[JobResult],
    done_event: asyncio.Event,
) -> tuple[int, int]:
    ok = 0
    failed = 0
    last_timeout_log = 0.0
    log_interval_s = 2.0
    while True:
        if done_event.is_set() and queue.empty():
            break
        try:
            result = await asyncio.wait_for(queue.get(), timeout=0.1)
        except asyncio.TimeoutError:
            now = time.monotonic()
            if now - last_timeout_log >= log_interval_s:
                last_timeout_log = now
                logger.debug(
                    "Result queue wait timed out; continuing",
                    done_event_set=done_event.is_set(),
                    queue_size=queue.qsize(),
                )
            continue
        if result.ok:
            ok += 1
        else:
            failed += 1
        queue.task_done()
    return ok, failed


async def run_tui_demo(config: DemoConfig) -> None:
    event_queue: asyncio.Queue[Event] = asyncio.Queue(maxsize=config.event_queue_size)
    queue_sink = EventQueueSink(event_queue, drop_policy=config.event_drop_policy)

    sinks: list[EventSink] = [queue_sink]
    jsonl_sink = None
    if config.event_jsonl:
        jsonl_sink = JsonlEventSink(Path(config.event_jsonl))
        sinks.append(jsonl_sink)

    event_bus = EventBus(sinks)

    result_sink = AsyncQueueResultSink(
        maxsize=config.result_queue_size,
        drop_policy=config.result_drop_policy,
    )

    controller = GroupLifecycleController(
        event_bus=event_bus,
        follower_dispatch_policy=config.follower_dispatch_policy,
    )

    scheduler = WorkStealingScheduler(
        pools=[
            PoolConfig(name="primer", preferred_kinds=[GroupTaskKind.PRIMER.value], workers=config.primer_workers),
            PoolConfig(
                name="follower",
                preferred_kinds=[GroupTaskKind.FOLLOWER.value],
                workers=config.follower_workers,
            ),
        ],
        event_bus=event_bus,
        result_sink=result_sink,
    )

    rng = random.Random(config.seed)
    executor = DemoExecutor(
        controller,
        rng=rng,
        min_delay_s=config.min_delay_s,
        max_delay_s=config.max_delay_s,
    )

    group_ids: list[str] = []
    for idx in range(config.groups):
        group_id = f"group-{idx + 1}"
        spec = GroupSpec(
            group_id=group_id,
            followers_total=None if config.open_groups else config.followers_per_group,
            warm_delay_s=config.warm_delay_s,
            max_followers_inflight=config.max_followers_inflight,
            payload={"group_id": group_id, "primer": True},
        )
        await controller.add_group(spec)
        if config.open_groups:
            payloads = [
                {"group_id": group_id, "follower": i + 1}
                for i in range(config.followers_per_group)
            ]
            await controller.add_followers(group_id, payloads)
            await controller.close_group(group_id)
        group_ids.append(group_id)

    async def _wait_groups() -> None:
        await asyncio.gather(*(controller.wait_group_done(gid) for gid in group_ids))
        await controller.close()

    app = create_tui_app(
        QueueEventSource(event_queue),
        state=TuiState(),
        queue_maxsize=config.event_queue_size,
    )
    app_task = asyncio.create_task(app.run_async())
    scheduler_task = asyncio.create_task(scheduler.run(controller, executor))
    close_task = asyncio.create_task(_wait_groups())

    done_event = asyncio.Event()
    results_task = asyncio.create_task(_drain_results(result_sink.queue, done_event))

    done, _ = await asyncio.wait({app_task, scheduler_task}, return_when=asyncio.FIRST_COMPLETED)

    if scheduler_task in done:
        try:
            report = scheduler_task.result()
        except Exception:
            app.exit()
            await app_task
            close_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await close_task
            if jsonl_sink is not None:
                jsonl_sink.close()
            raise
        done_event.set()
        ok, failed = await results_task
        app.exit()
        await app_task
        close_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await close_task
        if jsonl_sink is not None:
            jsonl_sink.close()
        print("Run complete.")
        print(f"Queued: {report.queued}")
        print(f"Finished: {report.finished}")
        print(f"Failed: {report.failed}")
        print(f"Results OK: {ok} Failed: {failed} Dropped: {result_sink.dropped}")
        if queue_sink.dropped:
            print(f"Events dropped: {queue_sink.dropped}")
        return

    done_event.set()
    results_task.cancel()
    close_task.cancel()
    scheduler_task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await results_task
    with contextlib.suppress(asyncio.CancelledError):
        await close_task
    with contextlib.suppress(asyncio.CancelledError):
        await scheduler_task
    if jsonl_sink is not None:
        jsonl_sink.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run group-only runtime with the acty TUI.")
    parser.add_argument("--groups", type=int, default=3)
    parser.add_argument("--followers-per-group", type=int, default=5)
    parser.add_argument("--open-groups", action="store_true")
    parser.add_argument("--warm-delay-s", type=float, default=0.0)
    parser.add_argument("--max-followers-inflight", type=int, default=2)
    parser.add_argument("--dispatch-mode", type=str, default="target", choices=["serial", "target", "eager"])
    parser.add_argument("--dispatch-target-total", type=int, default=None)
    parser.add_argument("--dispatch-min-per-group", type=int, default=0)
    parser.add_argument("--dispatch-max-per-group", type=int, default=None)
    parser.add_argument("--primer-workers", type=int, default=1)
    parser.add_argument("--follower-workers", type=int, default=2)
    parser.add_argument("--min-delay-s", type=float, default=0.01)
    parser.add_argument("--max-delay-s", type=float, default=0.05)
    parser.add_argument("--event-jsonl", type=str, default="")
    parser.add_argument("--seed", type=int, default=0)
    parser.add_argument("--result-queue-size", type=int, default=0)
    parser.add_argument(
        "--result-drop-policy",
        type=str,
        default="drop_oldest",
        choices=["block", "drop_newest", "drop_oldest"],
    )
    parser.add_argument("--event-queue-size", type=int, default=5000)
    parser.add_argument(
        "--event-drop-policy",
        type=str,
        default="drop_oldest",
        choices=["block", "drop_newest", "drop_oldest"],
    )
    return parser


def _normalize_optional_int(value: int | None) -> int | None:
    if value is None or value == 0:
        return None
    return value


def build_demo_config(args: argparse.Namespace) -> DemoConfig:
    target_total = _normalize_optional_int(args.dispatch_target_total)
    max_per_group = _normalize_optional_int(args.dispatch_max_per_group)
    if args.dispatch_mode == "target" and target_total is None:
        target_total = max(1, args.follower_workers)
    return DemoConfig(
        groups=args.groups,
        followers_per_group=args.followers_per_group,
        open_groups=args.open_groups,
        warm_delay_s=args.warm_delay_s,
        max_followers_inflight=args.max_followers_inflight,
        follower_dispatch_policy=FollowerDispatchPolicy(
            mode=args.dispatch_mode,
            target_total=target_total,
            min_per_group=args.dispatch_min_per_group,
            max_per_group=max_per_group,
        ),
        primer_workers=args.primer_workers,
        follower_workers=args.follower_workers,
        min_delay_s=args.min_delay_s,
        max_delay_s=args.max_delay_s,
        event_jsonl=args.event_jsonl or None,
        seed=args.seed,
        result_queue_size=args.result_queue_size,
        result_drop_policy=args.result_drop_policy,
        event_queue_size=args.event_queue_size,
        event_drop_policy=args.event_drop_policy,
    )


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = build_demo_config(args)
    asyncio.run(run_tui_demo(config))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
