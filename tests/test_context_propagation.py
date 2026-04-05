from __future__ import annotations

import asyncio
import contextvars

import pytest

from acty import ActyEngine, ContextVarsPropagator, EngineConfig
from acty_core.core.types import Job, JobResult

TRACE_CONTEXT = contextvars.ContextVar("acty_test_context", default=None)


class ContextExecutor:
    async def execute(self, job: Job, *, pool: str) -> JobResult:
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=True,
            output={"ctx": TRACE_CONTEXT.get()},
            group_id=job.group_id,
            follower_id=job.follower_id,
        )


@pytest.mark.asyncio
async def test_context_propagation_submit_group() -> None:
    token = TRACE_CONTEXT.set("alpha")
    try:
        async with ActyEngine(
            executor=ContextExecutor(),
            config=EngineConfig(
                primer_workers=1,
                follower_workers=1,
                context_propagator=ContextVarsPropagator(),
            ),
        ) as engine:
            submission = await engine.submit_group(
                "g-alpha",
                {"prompt": "primer"},
                [{"prompt": "follower"}],
            )
            assert submission.primer is not None
            primer = await asyncio.wait_for(submission.primer, timeout=5.0)
            follower = await asyncio.wait_for(submission.followers[0], timeout=5.0)

            assert primer.output is not None
            assert follower.output is not None
            assert primer.output.get("ctx") == "alpha"
            assert follower.output.get("ctx") == "alpha"
    finally:
        TRACE_CONTEXT.reset(token)


@pytest.mark.asyncio
async def test_context_propagation_for_late_follower() -> None:
    async with ActyEngine(
        executor=ContextExecutor(),
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            context_propagator=ContextVarsPropagator(),
        ),
    ) as engine:
        token = TRACE_CONTEXT.set("primer")
        try:
            group = engine.open_group("g-stream", primer_payload={"prompt": "primer"})
            await group
            assert group.primer is not None
            primer = await asyncio.wait_for(group.primer, timeout=5.0)
        finally:
            TRACE_CONTEXT.reset(token)

        token = TRACE_CONTEXT.set("follower")
        try:
            follower_future = await group.add_follower({"prompt": "follower"})
            follower = await asyncio.wait_for(follower_future, timeout=5.0)
        finally:
            TRACE_CONTEXT.reset(token)

        assert primer.output is not None
        assert follower.output is not None
        assert primer.output.get("ctx") == "primer"
        assert follower.output.get("ctx") == "follower"


@pytest.mark.asyncio
async def test_context_propagation_deferred_primer() -> None:
    async with ActyEngine(
        executor=ContextExecutor(),
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            context_propagator=ContextVarsPropagator(),
        ),
    ) as engine:
        token = TRACE_CONTEXT.set("open")
        try:
            group = engine.open_group("g-deferred", primer_payload=None)
            await group
        finally:
            TRACE_CONTEXT.reset(token)

        token = TRACE_CONTEXT.set("primer")
        try:
            primer_future = await group.submit_primer({"prompt": "primer"})
            assert primer_future is not None
            primer = await asyncio.wait_for(primer_future, timeout=5.0)
        finally:
            TRACE_CONTEXT.reset(token)

        await group.close()
        assert primer.output is not None
        assert primer.output.get("ctx") == "primer"
