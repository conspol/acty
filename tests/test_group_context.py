import asyncio
from typing import Any, Mapping

import pytest

from acty import ActyEngine, EngineConfig
from acty_core.core.types import Job, JobResult
from acty_core.lifecycle import GroupTaskKind


class ContextRecordingExecutor:
    handles_lifecycle = True

    def __init__(self) -> None:
        self._controller = None
        self.contexts: list[Mapping[str, Any] | None] = []

    def bind(self, controller) -> None:
        self._controller = controller

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        controller = self._controller
        assert controller is not None
        group_id = str(job.group_id) if job.group_id is not None else ""
        self.contexts.append(job.context)
        if job.kind == GroupTaskKind.PRIMER.value:
            await controller.mark_primer_started(group_id)
            await controller.mark_primer_done(group_id)
        elif job.kind == GroupTaskKind.FOLLOWER.value:
            await controller.mark_follower_done(group_id)
        return JobResult(job_id=job.id, kind=job.kind, ok=True, group_id=job.group_id)


@pytest.mark.asyncio
async def test_group_context_propagates_and_is_read_only() -> None:
    executor = ContextRecordingExecutor()
    engine = ActyEngine(executor=executor, config=EngineConfig(primer_workers=1, follower_workers=1))
    executor.bind(engine._controller)
    group_context = {"tenant": "acme", "region": "us-east"}
    expected_context = dict(group_context)

    try:
        submission = await engine.submit_group(
            "g-context",
            {"p": 1},
            [{"f": 1}],
            group_context=group_context,
        )
        assert submission.primer is not None
        results = await asyncio.wait_for(
            asyncio.gather(submission.primer, *submission.followers),
            timeout=2.0,
        )
        kinds = [result.kind for result in results]
        assert kinds.count(GroupTaskKind.PRIMER.value) == 1
        assert kinds.count(GroupTaskKind.FOLLOWER.value) == 1
    finally:
        await engine.close()

    assert group_context == expected_context
    assert executor.contexts
    for context in executor.contexts:
        assert context is not None
        assert context["group_context"] == expected_context
        assert context["group_context"] is not group_context
        assert context["cache"] is None

    with pytest.raises(TypeError):
        executor.contexts[0]["group_context"] = {"tenant": "mutated"}
