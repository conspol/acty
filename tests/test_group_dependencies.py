import asyncio
import pytest

from acty import ActyEngine, EngineConfig
from acty_core.core.types import Job, JobResult
from acty_core.events.types import Event
from acty_core.lifecycle import GroupDependency, GroupDependencyPolicy, GroupTaskKind

DEPENDENCY_TIMEOUT_S = 0.2
EVENT_TIMEOUT_S = 2.0


class EventRecorder:
    def __init__(self) -> None:
        self.events: list[Event] = []
        self._cond = asyncio.Condition()

    async def handle(self, event: Event) -> None:
        async with self._cond:
            self.events.append(event)
            self._cond.notify_all()

    def count(self, event_type: str) -> int:
        return sum(1 for event in self.events if event.type == event_type)

    async def wait_for(self, event_type: str, count: int, timeout: float = 1.0) -> None:
        async with self._cond:
            await asyncio.wait_for(
                self._cond.wait_for(lambda: self.count(event_type) >= count),
                timeout=timeout,
            )


class _BlockingExecutor:
    def __init__(self) -> None:
        self._primer_events: dict[str, asyncio.Event] = {}
        self._blocked: set[str] = set()

    def block_primer(self, group_id: str) -> None:
        self._blocked.add(group_id)
        self._primer_events.setdefault(group_id, asyncio.Event())

    def allow_primer(self, group_id: str) -> None:
        self._blocked.discard(group_id)
        event = self._primer_events.setdefault(group_id, asyncio.Event())
        event.set()

    async def execute(self, job: Job, *, pool: str) -> JobResult:
        if job.kind == GroupTaskKind.PRIMER.value and job.group_id is not None:
            group_id = str(job.group_id)
            if group_id in self._blocked:
                event = self._primer_events.setdefault(group_id, asyncio.Event())
                await event.wait()
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=True,
            group_id=job.group_id,
            follower_id=job.follower_id,
        )


class _FailingPrimerExecutor:
    async def execute(self, job: Job, *, pool: str) -> JobResult:
        if job.kind == GroupTaskKind.PRIMER.value and job.group_id is not None:
            if str(job.group_id) == "g-dep":
                return JobResult(
                    job_id=job.id,
                    kind=job.kind,
                    ok=False,
                    error="dependency failed",
                    group_id=job.group_id,
                )
        return JobResult(job_id=job.id, kind=job.kind, ok=True, group_id=job.group_id)


@pytest.mark.asyncio
async def test_dependency_gate_blocks_until_done() -> None:
    recorder = EventRecorder()
    executor = _BlockingExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(primer_workers=1, follower_workers=1),
        event_sinks=[recorder],
    )
    try:
        dep = GroupDependency(group_id="g-dep", policy=GroupDependencyPolicy.ON_DONE)
        executor.block_primer("g-dep")
        submission_b = await engine.submit_group("g-blocked", {"p": 2}, [], depends_on=[dep])

        await recorder.wait_for("group_blocked", count=1, timeout=1.0)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(submission_b.primer), timeout=0.1)

        submission_a = await engine.submit_group("g-dep", {"p": 1}, [])
        executor.allow_primer("g-dep")
        await asyncio.wait_for(submission_a.primer, timeout=1.0)

        await recorder.wait_for("group_unblocked", count=1, timeout=1.0)
        result_b = await asyncio.wait_for(submission_b.primer, timeout=1.0)
        assert result_b.ok is True
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_dependency_gate_buffers_followers_while_blocked() -> None:
    executor = _BlockingExecutor()
    engine = ActyEngine(executor=executor, config=EngineConfig(primer_workers=1, follower_workers=1))
    try:
        dep = GroupDependency(group_id="g-dep", policy=GroupDependencyPolicy.ON_DONE)
        executor.block_primer("g-dep")
        group = await engine.open_group("g-stream", {"p": 2}, depends_on=[dep])
        follower_future = await group.add_follower({"f": 1})

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(follower_future), timeout=0.1)

        submission_a = await engine.submit_group("g-dep", {"p": 1}, [])
        executor.allow_primer("g-dep")
        await asyncio.wait_for(submission_a.primer, timeout=1.0)

        result = await asyncio.wait_for(follower_future, timeout=1.0)
        assert result.kind == GroupTaskKind.FOLLOWER.value
        if group.primer is not None:
            await asyncio.wait_for(group.primer, timeout=1.0)
        await group.close()
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_dependency_gate_require_success_fails_dependents() -> None:
    recorder = EventRecorder()
    engine = ActyEngine(
        executor=_FailingPrimerExecutor(),
        config=EngineConfig(primer_workers=1, follower_workers=1),
        event_sinks=[recorder],
    )
    try:
        dep = GroupDependency(group_id="g-dep", policy=GroupDependencyPolicy.REQUIRE_SUCCESS)
        submission_b = await engine.submit_group("g-dependent", {"p": 2}, [], depends_on=[dep])
        submission_a = await engine.submit_group("g-dep", {"p": 1}, [])

        result_a = await asyncio.wait_for(submission_a.primer, timeout=1.0)
        assert result_a.ok is False

        result_b = await asyncio.wait_for(submission_b.primer, timeout=1.0)
        assert result_b.ok is False
        assert result_b.meta is not None
        assert result_b.meta.get("finalized_without_execution") is True
        assert "dependency_failed" in (result_b.error or "")

        await recorder.wait_for("group_dependency_failed", count=1, timeout=1.0)
        failed_events = [event for event in recorder.events if event.type == "group_dependency_failed"]
        assert len(failed_events) == 1
        failed_event = failed_events[0]
        assert failed_event.group_id == "g-dependent"
        assert failed_event.payload is not None
        assert failed_event.payload.get("dependency_id") == "g-dep"
        assert failed_event.payload.get("policy") == GroupDependencyPolicy.REQUIRE_SUCCESS.value
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_dependency_gate_waits_for_all_dependencies() -> None:
    recorder = EventRecorder()
    executor = _BlockingExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(primer_workers=1, follower_workers=1),
        event_sinks=[recorder],
    )
    try:
        dep_a = GroupDependency(group_id="g-dep-a", policy=GroupDependencyPolicy.ON_DONE)
        dep_b = GroupDependency(group_id="g-dep-b", policy=GroupDependencyPolicy.ON_DONE)
        executor.block_primer("g-dep-a")
        executor.block_primer("g-dep-b")
        submission_b = await engine.submit_group("g-dependent", {"p": 2}, [], depends_on=[dep_a, dep_b])

        await recorder.wait_for("group_blocked", count=2, timeout=1.0)
        blocked_events = [event for event in recorder.events if event.type == "group_blocked"]
        assert len(blocked_events) == 2
        assert {event.payload.get("dependency_id") for event in blocked_events if event.payload} == {
            "g-dep-a",
            "g-dep-b",
        }
        assert all(
            event.payload is not None
            and event.payload.get("policy") == GroupDependencyPolicy.ON_DONE.value
            for event in blocked_events
        )

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(submission_b.primer), timeout=0.1)

        submission_a = await engine.submit_group("g-dep-a", {"p": 1}, [])
        submission_c = await engine.submit_group("g-dep-b", {"p": 1}, [])

        executor.allow_primer("g-dep-a")
        await asyncio.wait_for(submission_a.primer, timeout=1.0)

        with pytest.raises(asyncio.TimeoutError):
            await recorder.wait_for("group_unblocked", count=1, timeout=0.1)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(submission_b.primer), timeout=0.1)

        executor.allow_primer("g-dep-b")
        await asyncio.wait_for(submission_c.primer, timeout=1.0)

        await recorder.wait_for("group_unblocked", count=1, timeout=1.0)
        unblocked_events = [event for event in recorder.events if event.type == "group_unblocked"]
        assert len(unblocked_events) == 1
        unblocked_event = unblocked_events[0]
        assert unblocked_event.group_id == "g-dependent"
        assert unblocked_event.payload is not None
        assert unblocked_event.payload.get("dependency_id") == "g-dep-b"
        assert unblocked_event.payload.get("policy") == GroupDependencyPolicy.ON_DONE.value

        result_b = await asyncio.wait_for(submission_b.primer, timeout=1.0)
        assert result_b.ok is True
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_dependency_gate_with_admission_enabled() -> None:
    executor = _BlockingExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(primer_workers=1, follower_workers=1, max_active_groups=2),
    )
    try:
        dep = GroupDependency(group_id="g-dep", policy=GroupDependencyPolicy.ON_DONE)
        executor.block_primer("g-dep")
        submission_b = await engine.submit_group("g-blocked", {"p": 2}, [], depends_on=[dep])
        submission_a = await engine.submit_group("g-dep", {"p": 1}, [])

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(submission_b.primer), timeout=0.1)

        executor.allow_primer("g-dep")
        await asyncio.wait_for(submission_a.primer, timeout=1.0)
        await asyncio.wait_for(submission_b.primer, timeout=1.0)
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_dependency_gate_multiple_dependents_unblock() -> None:
    recorder = EventRecorder()
    executor = _BlockingExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(primer_workers=1, follower_workers=1),
        event_sinks=[recorder],
    )
    try:
        dep = GroupDependency(group_id="g-dep", policy=GroupDependencyPolicy.ON_DONE)
        executor.block_primer("g-dep")

        submissions = []
        for idx in range(4):
            submissions.append(
                await engine.submit_group(
                    f"g-dependent-{idx}",
                    {"p": idx},
                    [],
                    depends_on=[dep],
                )
            )

        await recorder.wait_for("group_blocked", count=4, timeout=1.0)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(submissions[0].primer), timeout=0.1)

        dep_submission = await engine.submit_group("g-dep", {"p": 99}, [])
        executor.allow_primer("g-dep")
        await asyncio.wait_for(dep_submission.primer, timeout=1.0)

        await recorder.wait_for("group_unblocked", count=4, timeout=1.0)
        unblocked_events = [event for event in recorder.events if event.type == "group_unblocked"]
        assert len(unblocked_events) == 4
        assert all(
            event.payload is not None
            and event.payload.get("dependency_id") == "g-dep"
            and event.payload.get("policy") == GroupDependencyPolicy.ON_DONE.value
            for event in unblocked_events
        )

        results = await asyncio.gather(
            *[asyncio.wait_for(submission.primer, timeout=1.0) for submission in submissions]
        )
        assert all(result.ok for result in results)
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_dependency_gate_multiple_dependents_fail_on_dependency_failure() -> None:
    recorder = EventRecorder()
    engine = ActyEngine(
        executor=_FailingPrimerExecutor(),
        config=EngineConfig(primer_workers=1, follower_workers=1),
        event_sinks=[recorder],
    )
    try:
        dep = GroupDependency(group_id="g-dep", policy=GroupDependencyPolicy.REQUIRE_SUCCESS)
        submissions = []
        for idx in range(4):
            submissions.append(
                await engine.submit_group(
                    f"g-dependent-{idx}",
                    {"p": idx},
                    [],
                    depends_on=[dep],
                )
            )

        dep_submission = await engine.submit_group("g-dep", {"p": 99}, [])
        dep_result = await asyncio.wait_for(dep_submission.primer, timeout=1.0)
        assert dep_result.ok is False

        results = await asyncio.gather(
            *[asyncio.wait_for(submission.primer, timeout=1.0) for submission in submissions]
        )
        assert all(result.ok is False for result in results)
        assert all(
            result.meta is not None and result.meta.get("finalized_without_execution") is True
            for result in results
        )
        assert all("dependency_failed" in (result.error or "") for result in results)

        await recorder.wait_for("group_dependency_failed", count=4, timeout=1.0)
        failed_events = [event for event in recorder.events if event.type == "group_dependency_failed"]
        assert len(failed_events) == 4
        assert all(event.payload is not None for event in failed_events)
        assert all(event.payload.get("dependency_id") == "g-dep" for event in failed_events)
        assert all(
            event.payload.get("policy") == GroupDependencyPolicy.REQUIRE_SUCCESS.value
            for event in failed_events
        )
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_dependency_gate_waits_for_five_dependencies() -> None:
    recorder = EventRecorder()
    executor = _BlockingExecutor()
    engine = ActyEngine(
        executor=executor,
        config=EngineConfig(primer_workers=1, follower_workers=1),
        event_sinks=[recorder],
    )
    try:
        deps = [
            GroupDependency(group_id=f"g-dep-{idx}", policy=GroupDependencyPolicy.ON_DONE)
            for idx in range(5)
        ]
        for dep in deps:
            executor.block_primer(dep.group_id)

        submission = await engine.submit_group("g-dependent", {"p": 1}, [], depends_on=deps)
        await recorder.wait_for("group_blocked", count=5, timeout=1.0)

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(submission.primer), timeout=0.1)

        for dep in deps[:-1]:
            dep_submission = await engine.submit_group(dep.group_id, {"p": dep.group_id}, [])
            executor.allow_primer(dep.group_id)
            await asyncio.wait_for(dep_submission.primer, timeout=1.0)

        with pytest.raises(asyncio.TimeoutError):
            await recorder.wait_for("group_unblocked", count=1, timeout=0.1)

        last_dep = deps[-1]
        last_submission = await engine.submit_group(last_dep.group_id, {"p": last_dep.group_id}, [])
        executor.allow_primer(last_dep.group_id)
        await asyncio.wait_for(last_submission.primer, timeout=1.0)

        await recorder.wait_for("group_unblocked", count=1, timeout=1.0)
        result = await asyncio.wait_for(submission.primer, timeout=1.0)
        assert result.ok is True
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_dependency_gate_rejects_cycles() -> None:
    engine = ActyEngine(executor=_BlockingExecutor(), config=EngineConfig(primer_workers=1, follower_workers=1))
    try:
        with pytest.raises(ValueError):
            await engine.submit_group(
                "g-self",
                {"p": 1},
                [],
                depends_on=[GroupDependency(group_id="g-self")],
            )

        await engine.submit_group(
            "g-a",
            {"p": 1},
            [],
            depends_on=[GroupDependency(group_id="g-b")],
        )
        with pytest.raises(ValueError):
            await engine.submit_group(
                "g-b",
                {"p": 1},
                [],
                depends_on=[GroupDependency(group_id="g-a")],
            )
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_dependency_gate_timeout_on_done_unblocks() -> None:
    recorder = EventRecorder()
    engine = ActyEngine(
        executor=_BlockingExecutor(),
        config=EngineConfig(primer_workers=1, follower_workers=1),
        event_sinks=[recorder],
    )
    try:
        dep = GroupDependency(
            group_id="g-missing",
            policy=GroupDependencyPolicy.ON_DONE,
            timeout_s=DEPENDENCY_TIMEOUT_S,
        )
        submission = await engine.submit_group("g-dependent", {"p": 1}, [], depends_on=[dep])

        await recorder.wait_for("group_blocked", count=1, timeout=EVENT_TIMEOUT_S)
        await recorder.wait_for("group_dependency_failed", count=1, timeout=EVENT_TIMEOUT_S)
        await recorder.wait_for("group_unblocked", count=1, timeout=EVENT_TIMEOUT_S)

        result = await asyncio.wait_for(submission.primer, timeout=EVENT_TIMEOUT_S)
        assert result.ok is True

        failed_event = next(event for event in recorder.events if event.type == "group_dependency_failed")
        assert failed_event.payload is not None
        assert failed_event.payload.get("dependency_id") == "g-missing"
        assert failed_event.payload.get("reason") == "timeout"
        assert failed_event.payload.get("timeout_s") == pytest.approx(DEPENDENCY_TIMEOUT_S)
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_dependency_gate_timeout_require_success_fails() -> None:
    recorder = EventRecorder()
    engine = ActyEngine(
        executor=_BlockingExecutor(),
        config=EngineConfig(primer_workers=1, follower_workers=1),
        event_sinks=[recorder],
    )
    try:
        dep = GroupDependency(
            group_id="g-missing",
            policy=GroupDependencyPolicy.REQUIRE_SUCCESS,
            timeout_s=DEPENDENCY_TIMEOUT_S,
        )
        submission = await engine.submit_group("g-dependent", {"p": 1}, [], depends_on=[dep])

        await recorder.wait_for("group_blocked", count=1, timeout=EVENT_TIMEOUT_S)
        await recorder.wait_for("group_dependency_failed", count=1, timeout=EVENT_TIMEOUT_S)

        result = await asyncio.wait_for(submission.primer, timeout=EVENT_TIMEOUT_S)
        assert result.ok is False
        assert result.meta is not None
        assert result.meta.get("finalized_without_execution") is True
        assert result.meta.get("dependency_failure_reason") == "timeout"
        assert "dependency_failed" in (result.error or "")

        failed_event = next(event for event in recorder.events if event.type == "group_dependency_failed")
        assert failed_event.payload is not None
        assert failed_event.payload.get("dependency_id") == "g-missing"
        assert failed_event.payload.get("policy") == GroupDependencyPolicy.REQUIRE_SUCCESS.value
        assert failed_event.payload.get("reason") == "timeout"
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_dependency_gate_timeout_with_admission_enabled() -> None:
    recorder = EventRecorder()
    engine = ActyEngine(
        executor=_BlockingExecutor(),
        config=EngineConfig(primer_workers=1, follower_workers=1, max_active_groups=1),
        event_sinks=[recorder],
    )
    try:
        dep = GroupDependency(
            group_id="g-missing",
            policy=GroupDependencyPolicy.ON_DONE,
            timeout_s=DEPENDENCY_TIMEOUT_S,
        )
        submission = await engine.submit_group("g-dependent", {"p": 1}, [], depends_on=[dep])

        await recorder.wait_for("group_blocked", count=1, timeout=EVENT_TIMEOUT_S)
        await recorder.wait_for("group_dependency_failed", count=1, timeout=EVENT_TIMEOUT_S)
        await recorder.wait_for("group_unblocked", count=1, timeout=EVENT_TIMEOUT_S)

        result = await asyncio.wait_for(submission.primer, timeout=EVENT_TIMEOUT_S)
        assert result.ok is True
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_dependency_gate_timeout_shutdown_no_unblock() -> None:
    recorder = EventRecorder()
    engine = ActyEngine(
        executor=_BlockingExecutor(),
        config=EngineConfig(primer_workers=1, follower_workers=1),
        event_sinks=[recorder],
    )
    closed = False
    try:
        dep = GroupDependency(
            group_id="g-missing",
            policy=GroupDependencyPolicy.ON_DONE,
            timeout_s=DEPENDENCY_TIMEOUT_S,
        )
        submission = await engine.submit_group("g-dependent", {"p": 1}, [], depends_on=[dep])
        await recorder.wait_for("group_blocked", count=1, timeout=EVENT_TIMEOUT_S)

        await engine.close()
        closed = True

        with pytest.raises(asyncio.TimeoutError):
            await recorder.wait_for("group_dependency_failed", count=1, timeout=DEPENDENCY_TIMEOUT_S * 1.5)
        with pytest.raises(asyncio.TimeoutError):
            await recorder.wait_for("group_unblocked", count=1, timeout=DEPENDENCY_TIMEOUT_S * 1.5)

        assert submission.primer is not None
        with pytest.raises(asyncio.CancelledError):
            await submission.primer
    finally:
        if not closed:  # pragma: no cover - safety cleanup if test fails early
            await engine.close()


@pytest.mark.asyncio
async def test_dependency_gate_disabled_raises() -> None:
    engine = ActyEngine(
        executor=_BlockingExecutor(),
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            enable_dependency_gates=False,
        ),
    )
    try:
        dep = GroupDependency(group_id="g-dep", policy=GroupDependencyPolicy.ON_DONE)
        with pytest.raises(ValueError, match="Dependency gates are disabled"):
            await engine.submit_group("g-dependent", {"p": 1}, [], depends_on=[dep])
    finally:
        await engine.close()


@pytest.mark.asyncio
async def test_dependency_gate_disabled_can_ignore() -> None:
    engine = ActyEngine(
        executor=_BlockingExecutor(),
        config=EngineConfig(
            primer_workers=1,
            follower_workers=1,
            enable_dependency_gates=False,
            allow_unsafe_ignore_dependencies=True,
        ),
    )
    try:
        dep = GroupDependency(group_id="g-dep", policy=GroupDependencyPolicy.ON_DONE)
        submission = await engine.submit_group("g-dependent", {"p": 1}, [], depends_on=[dep])
        result = await asyncio.wait_for(submission.primer, timeout=1.0)
        assert result.ok is True
    finally:
        await engine.close()
