import argparse
import asyncio
from pathlib import Path

from tenacity import AsyncRetrying, retry_if_exception_type, stop_after_attempt, wait_fixed

from acty import ActyEngine, EngineConfig
from acty_core.core.types import Job, JobResult
from acty_core.events.sinks.jsonl import JsonlEventSink


class FlakyExecutor:
    def __init__(self, *, fail_times: int) -> None:
        self._fail_times = fail_times
        self._calls = 0

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        self._calls += 1
        if self._calls <= self._fail_times:
            raise RuntimeError("transient failure")
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=True,
            output={"payload": job.payload},
            group_id=job.group_id,
            follower_id=job.follower_id,
        )


async def _consume_results(engine: ActyEngine) -> None:
    async for result in engine.results():
        print(
            "result",
            {
                "job_id": str(result.job_id),
                "ok": result.ok,
                "meta": result.meta,
            },
        )


async def _run(event_jsonl: Path, fail_times: int) -> None:
    sink = JsonlEventSink(event_jsonl)
    executor = FlakyExecutor(fail_times=fail_times)
    retrying = AsyncRetrying(
        stop=stop_after_attempt(3),
        wait=wait_fixed(0.0),
        retry=retry_if_exception_type(RuntimeError),
    )
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(attempt_retry_policy=retrying),
        event_sinks=[sink],
    )

    results_task = asyncio.create_task(_consume_results(engine))
    try:
        submission = await engine.submit_group("g-tenacity-demo", {"p": 1}, [])
        assert submission.primer is not None
        await submission.primer
        await engine._controller.wait_group_done("g-tenacity-demo")
    finally:
        await engine.close()
        await results_task
        sink.close()

    print(f"event log written to {event_jsonl}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Tenacity retry demo for acty.")
    parser.add_argument(
        "--event-jsonl",
        type=Path,
        default=Path("/tmp/acty_tenacity_events.jsonl"),
        help="Path to write JSONL event log.",
    )
    parser.add_argument(
        "--fail-times",
        type=int,
        default=2,
        help="Number of transient failures before success.",
    )
    args = parser.parse_args()

    asyncio.run(_run(args.event_jsonl, args.fail_times))


if __name__ == "__main__":
    main()
