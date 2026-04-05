# acty

Async grouped-job runtime for Python, AI agents, and LLM systems, with retries, fair scheduling, JSONL event logs, and a live terminal TUI.

Acty is a good fit when one setup step unlocks a family of related async jobs. That can be an AI agent that establishes session state, a coordinator that fans work out to specialist agents, an LLM request with shared context, a cache warmup step for prefix caching, or any workload where several follow-up jobs should stay tied to the same unit of work.

It builds on `acty-core` and adds the high-level engine API, the TUI app, and the `acty-tui` CLI for following or replaying JSONL event streams.

## Table of Contents

- [Why use acty?](#why-use-acty)
- [Core ideas](#core-ideas)
- [Why primer/follower instead of a flat task queue?](#why-primerfollower-instead-of-a-flat-task-queue)
- [Install](#install)
- [Quick Start](#quick-start)
- [LangChain With acty-langchain](#langchain-with-acty-langchain)
- [Open Groups + Cache Warmup](#open-groups--cache-warmup)
- [Lanes](#lanes)
- [Watch A Run Live](#watch-a-run-live)
- [Examples](#examples)
- [Package Map](#package-map)
- [Development](#development)

## Why use acty?

- Keep related jobs together instead of manually stitching queues, futures, and lifecycle state.
- Run one `primer` job and then fan out `follower` jobs behind it.
- Retry work without bolting retry logic onto every executor.
- Split worker capacity fairly across runs, tenants, or pipelines with lanes.
- Write JSONL events and inspect long runs live in a terminal TUI.

## Core ideas

- `group`: one logical unit of work
- `primer`: the setup step for a group, usually the job that establishes context, warms a cache, creates or reuses a provider/session handle, or prepares shared inputs
- `follower`: work that belongs to the same group and usually benefits from that setup step
- `lane`: a fairness label that gives a run or tenant its own share of worker capacity

If you have ten noisy runs and one important run, lanes let you give the important run a bigger share instead of letting everything fight in one flat queue. Idle lanes do not have to waste capacity; other lanes can still use spare workers.

## Why primer/follower instead of a flat task queue?

Because a lot of real systems are not just "N independent tasks". They usually have a shape more like:

1. Resolve or create shared context.
2. Fan out follow-up work that depends on that context.

That first step might be:

- opening or reusing a provider session
- warming a prompt or session cache for prefix caching before fan-out
- loading shared memory or tool context for an AI agent
- building a cache handle or provider reference
- doing one expensive planning step before parallel execution

In a flat queue, that structure is implicit. You end up rebuilding it yourself with ad-hoc dependency tracking, custom cancellation rules, and queueing hacks. Acty makes that boundary explicit so the runtime can do useful things with it:

- schedule setup work separately from fan-out work
- avoid a large follower burst starving new groups from getting their first step started
- keep retries and failure policies aligned with the actual shape of the workflow
- attach cache/session state to the group instead of smearing it across unrelated tasks
- model cache warmup explicitly when the first call prepares prefix-caching state for later calls
- make open groups natural when more followers arrive later

The point is not that every workload must have a dramatic "leader" task. The point is that many agent and LLM systems have a setup phase and an execution phase, and Acty gives those phases names the scheduler can understand.

## Install

```bash
pip install acty
```

Optional LangChain integration:

```bash
pip install "acty[langchain]"
```

Local development:

```bash
pip install -e .[dev]
```

## Quick Start

This example shows a more realistic grouped workflow: one coordinator step sets up the job, then several specialist steps continue the same unit of work.

```python
import asyncio
from pprint import pprint

from acty import ActyEngine, EngineConfig, EchoExecutor


async def main() -> None:
    # EchoExecutor is a zero-dependency demo executor: it simply returns the
    # payload it received. That makes it useful for learning the Acty API
    # without bringing in a model provider yet.
    engine = ActyEngine(executor=EchoExecutor(), config=EngineConfig())
    try:
        # The primer carries the shared setup and the full initial context.
        submission = await engine.submit_group(
            "release-readiness",
            {
                "agent": "coordinator",
                "task": "set up the shared review context",
                "messages": [
                    {
                        "role": "system",
                        "content": "You coordinate release-readiness reviews for an engineering team.",
                    },
                    {
                        "role": "user",
                        "content": "We want to ship the billing service update today.",
                    },
                    {
                        "role": "user",
                        "content": "Figure out which risks and checks should be reviewed before rollout.",
                    },
                ],
            },
            [
                {
                    "agent": "specialist:test-risk",
                    "task": "review regression risk",
                    "messages": [
                        {
                            "role": "user",
                            "content": "Look for missing tests or weak coverage before release.",
                        },
                    ],
                },
                {
                    "agent": "specialist:performance",
                    "task": "review latency and throughput risk",
                    "messages": [
                        {
                            "role": "user",
                            "content": "Check whether the release could hurt latency or throughput.",
                        },
                    ],
                },
                {
                    "agent": "specialist:ops",
                    "task": "review rollout and monitoring readiness",
                    "messages": [
                        {
                            "role": "user",
                            "content": "Check whether the deployment plan and monitoring are good enough.",
                        },
                    ],
                },
            ],
        )

        # Followers only carry the incremental asks that branch off that setup.
        if submission.primer is not None:
            pprint({"primer": (await submission.primer).output})
        for index, future in enumerate(submission.followers, start=1):
            pprint({"follower": index, "output": (await future).output})
    finally:
        await engine.close()


asyncio.run(main())
```

Notice that the followers only add the next request. They do not need to repeat the whole shared setup. With a real executor, they would typically reuse the group context, provider session, or cache state prepared by the primer. `EchoExecutor` just makes the grouped payloads easy to inspect.

## LangChain With acty-langchain

Install the integration and the core LangChain package:

```bash
pip install acty acty-langchain langchain-core
```

If you want to plug in a real chat model, also install the matching provider integration, for example:

```bash
pip install langchain-openai
```

This example stays provider-free by using `RunnableLambda`, but the chain shape is the same one you would use with a real LangChain chat model.

```python
import asyncio
from pprint import pprint

from acty import ActyEngine, EngineConfig
from acty_langchain import LangChainRunnableExecutor
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.runnables import RunnableLambda


def fake_chat_model(prompt_value):
    # RunnableLambda stands in for a real chat model so the example stays
    # runnable without API keys. In production, replace this with ChatOpenAI,
    # another chat model, or a longer LangChain pipeline.
    messages = prompt_value.to_messages()
    return {
        "message_count": len(messages),
        "last_message": messages[-1].content if messages else "",
    }


async def main() -> None:
    # This is a normal LangChain chat chain: prompt template first, then the
    # model step. Acty does not replace LangChain here; it runs the chain as the
    # job executor payload.
    review_chain = (
        ChatPromptTemplate.from_messages(
            [
                ("system", "You help coordinate release reviews for engineering teams."),
                ("human", "Shared context: {shared_context}"),
                ("human", "Current task: {task}"),
            ]
        )
        | RunnableLambda(fake_chat_model)
    )

    engine = ActyEngine(
        # LangChainRunnableExecutor reads `runnable` and `input` from each Acty
        # payload and invokes the LangChain chain for that job.
        executor=LangChainRunnableExecutor(),
        config=EngineConfig(),
    )
    try:
        # The primer handles the broad coordination step for the group.
        # Followers reuse the same chain but branch into narrower review tasks.
        submission = await engine.submit_group(
            "langchain-release-review",
            {
                "runnable": review_chain,
                "input": {
                    "shared_context": "Billing service release planned for today.",
                    "task": "Plan the review and identify the main risk areas.",
                },
            },
            [
                {
                    "runnable": review_chain,
                    "input": {
                        "shared_context": "Billing service release planned for today.",
                        "task": "Check automated-test risk for this release.",
                    },
                },
                {
                    "runnable": review_chain,
                    "input": {
                        "shared_context": "Billing service release planned for today.",
                        "task": "Check performance risk for this release.",
                    },
                },
                {
                    "runnable": review_chain,
                    "input": {
                        "shared_context": "Billing service release planned for today.",
                        "task": "Check rollout and alerting readiness.",
                    },
                },
            ],
        )

        if submission.primer is not None:
            pprint({"primer": (await submission.primer).output})
        for index, future in enumerate(submission.followers, start=1):
            pprint({"follower": index, "output": (await future).output})
    finally:
        await engine.close()


asyncio.run(main())
```

Use this pattern when you already have a LangChain chat chain and want Acty to manage grouped execution, retries, event logs, and live monitoring around it.

## Open Groups + Cache Warmup

Use `open_group(...)` when the work for a group arrives over time. This is a good fit for prefix caching: warm the shared cache/session once with the primer, then keep sending followers that reuse the same handle.

```python
import asyncio
from pprint import pprint

from acty import ActyEngine, EngineConfig
from acty_core.cache import CacheRegistry, CacheProvider, InMemoryStorage
from acty_core.core.types import Job, JobResult


class DemoPrefixCacheProvider:
    # This fake provider models what a real prefix-cache/session provider would
    # do for Acty: derive a stable cache identity from the primer payload and
    # create a provider_ref that later jobs can reuse.
    name = "demo-prefix"

    def fingerprint(self, primer, context=None) -> str:
        tenant = context["tenant"] if context is not None else "default"
        prefix_id = primer["prefix_id"]
        return f"{tenant}:{prefix_id}"

    async def create(self, primer, context=None) -> str | None:
        tenant = context["tenant"] if context is not None else "default"
        prefix_id = primer["prefix_id"]
        return f"prefix-cache:{tenant}:{prefix_id}"

    async def wait_ready(self, provider_ref, timeout_s=None) -> None:
        return None

    async def invalidate(self, provider_ref) -> None:
        return None


class PrefixAwareExecutor:
    # This fake executor does not call a model. It only exposes the shared
    # provider_ref in the result so you can see that the same cache/session
    # handle reaches every job in the group.
    async def execute(self, job: Job, *, pool: str) -> JobResult:
        cache = job.context.get("cache") if job.context is not None else None
        provider_ref = cache.get("provider_ref") if cache is not None else None
        return JobResult(
            job_id=job.id,
            kind=job.kind,
            ok=True,
            output={
                "agent": job.payload["agent"],
                "provider_ref": provider_ref,
                "last_message": job.payload["messages"][-1]["content"],
            },
            group_id=job.group_id,
            follower_id=job.follower_id,
        )


async def main() -> None:
    # CacheRegistry is the piece that remembers cache/session handles across the
    # group. InMemoryStorage keeps the example self-contained.
    registry = CacheRegistry(DemoPrefixCacheProvider(), storage=InMemoryStorage())
    engine = ActyEngine(
        executor=PrefixAwareExecutor(),
        config=EngineConfig(),
        cache_registry=registry,
    )
    try:
        # open_group(...) is useful here because the whole job does not arrive
        # up front: we warm the cache first, then send more work into the same
        # group as new follow-up questions appear.
        async with engine.open_group(
            "support-playbook-review",
            # cache_key says "all jobs in this group belong to the same warmed
            # prefix/session entry".
            cache_key="tenant:acme:support-playbook-v3",
            # cache_context carries any extra fields the provider needs to build
            # or look up that cache/session entry.
            cache_context={"tenant": "acme"},
        ) as group:
            # The primer warms the shared prefix/session state once for the group.
            primer = await group.submit_primer(
                {
                    "agent": "coordinator",
                    "prefix_id": "support-playbook-v3",
                    "messages": [
                        {
                            "role": "system",
                            "content": "Shared support playbook, escalation policy, and customer-history rules.",
                        },
                        {
                            "role": "user",
                            "content": "Warm the shared context for this investigation.",
                        },
                    ],
                }
            )

            # Followers can arrive later and still reuse the same warmed handle.
            followers = [
                await group.add_follower(
                    {
                        "agent": "specialist:billing",
                        "prefix_id": "support-playbook-v3",
                        "messages": [
                            {
                                "role": "user",
                                "content": "Check invoice retry failures for this customer.",
                            },
                        ],
                    }
                ),
                await group.add_follower(
                    {
                        "agent": "specialist:webhooks",
                        "prefix_id": "support-playbook-v3",
                        "messages": [
                            {
                                "role": "user",
                                "content": "Check whether webhook delivery is delayed or failing.",
                            },
                        ],
                    }
                ),
                await group.add_follower(
                    {
                        "agent": "specialist:history",
                        "prefix_id": "support-playbook-v3",
                        "messages": [
                            {
                                "role": "user",
                                "content": "Pull recent escalations that look similar.",
                            },
                        ],
                    }
                ),
            ]

        if primer is not None:
            pprint({"primer": (await primer).output})
        for index, future in enumerate(followers, start=1):
            pprint({"follower": index, "output": (await future).output})
    finally:
        await engine.close()


asyncio.run(main())
```

All results will show the same `provider_ref`. That is the point of the group here: the runtime has one place to attach the warmed cache/session handle and propagate it to every job in the same unit of work.

With a real cache-aware executor, that shared handle can mean one of two things:

- the primer warms a provider-side prefix cache and followers hit that same warmed prefix
- the primer creates or reuses a session/provider reference and followers continue from it without starting cold

That is much harder to model cleanly with a flat queue of unrelated tasks.

## Lanes

Lanes are how you give different workloads a fair share of the same worker pool.

Typical uses:

- separate traffic by tenant
- protect one evaluation run from another
- keep background batch work from swallowing all follower workers

You can attach lanes either by setting `group_context={"lane": ...}` when you submit a group or by using `engine.lane(...)` helpers.

The important part is the relative weight:

- `weight=2` means "this lane should get about twice the share"
- `weight=1` means "normal share"

That gives you fairness without hard-partitioning the workers into isolated pools.

## Watch A Run Live

Acty works well when you want both a programmatic API and a live view of what the runtime is doing. The example below creates a richer event stream: multiple groups, multiple followers, and two lanes competing in the same run.

Write JSONL events while several groups run across two lanes:

```python
import asyncio
from pathlib import Path

from acty import ActyEngine, EngineConfig, SimulatedExecutor
from acty_core.events.sinks.jsonl import JsonlEventSink
from acty_core.scheduler import LaneConfig


def build_followers(*names: str) -> list[dict]:
    # Keep follower payloads tiny here: the goal of this example is to generate
    # visible scheduling activity, not to show prompt design. Each returned
    # dict becomes one follower job inside a group.
    return [
        {
            "agent": name,
            "messages": [{"role": "user", "content": name}],
        }
        for name in names
    ]


async def main() -> None:
    # JsonlEventSink writes runtime events to disk so the TUI can follow them
    # live or replay them later.
    sink = JsonlEventSink(Path("/tmp/acty_events.jsonl"))
    engine = ActyEngine(
        # Use simulated delays so the TUI has enough activity to watch.
        executor=SimulatedExecutor(min_delay_s=0.2, max_delay_s=0.8, seed=7),
        config=EngineConfig(
            primer_workers=2,
            follower_workers=3,
            # Give the priority lane about twice the scheduling share of the
            # background lane when both have queued work.
            lane_configs={
                "priority": LaneConfig(weight=2),
                "background": LaneConfig(weight=1),
            },
        ),
        event_sinks=[sink],
    )
    try:
        # Submit several groups at once so the event log shows queueing, execution,
        # and lane competition instead of a single short burst.
        submissions = await asyncio.gather(
            engine.submit_group(
                "priority:billing",
                # Keep messages short so the example stays readable; the point is
                # to create several groups that compete for runtime capacity.
                {"agent": "coord", "messages": [{"role": "user", "content": "plan"}]},
                build_followers("tests", "perf", "ops"),
                group_context={"lane": "priority"},
            ),
            engine.submit_group(
                "priority:search",
                {"agent": "coord", "messages": [{"role": "user", "content": "plan"}]},
                build_followers("tests", "perf", "alerts"),
                group_context={"lane": "priority"},
            ),
            engine.submit_group(
                "background:analytics",
                {"agent": "coord", "messages": [{"role": "user", "content": "plan"}]},
                build_followers("schema", "perf", "dash"),
                group_context={"lane": "background"},
            ),
            engine.submit_group(
                "background:ops",
                {"agent": "coord", "messages": [{"role": "user", "content": "plan"}]},
                build_followers("logs", "alerts", "runbook"),
                group_context={"lane": "background"},
            ),
        )

        # Wait for every primer and follower so the JSONL file captures the whole
        # run instead of stopping after only a few early events.
        pending = []
        for submission in submissions:
            if submission.primer is not None:
                pending.append(submission.primer)
            pending.extend(submission.followers)
        await asyncio.gather(*pending)
    finally:
        await engine.close()
        sink.close()


asyncio.run(main())
```

This produces a much better event stream for the TUI: multiple groups, queued and completed followers, and lane-tagged work competing in the same run.

Then watch the event stream:

```bash
acty-tui follow /tmp/acty_events.jsonl
```

Or replay a finished run:

```bash
acty-tui replay /tmp/acty_events.jsonl --speed 2.0
```

## Examples

- `python examples/group_tui_demo.py --groups 3 --followers-per-group 2`
- `python examples/lane_demo.py`
- `python examples/tenacity_retry_demo.py --event-jsonl /tmp/acty_tenacity_events.jsonl`

## Package Map

- `acty-core`: low-level scheduler, lifecycle, event, and cache primitives
- `acty`: engine, client, TUI, and CLI
- `acty-langchain`: LangChain payload helpers and runnable executor support
- `acty-openai`: OpenAI executor integration
- `acty-gigachat`: GigaChat executor integration

If you only need the lower-level runtime pieces, use `acty-core`. If you want the higher-level API and the live terminal tooling, use `acty`.

## Development

- tests live under `tests/`
- example programs live under `examples/`
- adapter-specific integration coverage belongs in the adapter packages
