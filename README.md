# acty

`acty` is the high-level runtime package in the Acty stack. It builds on
`acty-core` and adds the engine API, TUI application, and the `acty-tui` CLI
for following or replaying JSONL event streams.

## Install

```bash
pip install acty
```

Install the optional LangChain integration:

```bash
pip install "acty[langchain]"
```

For local development:

```bash
pip install -e .[dev]
```

## Python Usage

```python
import asyncio

from acty import ActyEngine, EngineConfig, EchoExecutor


async def main() -> None:
    engine = ActyEngine(executor=EchoExecutor(), config=EngineConfig())
    try:
        submission = await engine.submit_group("demo", {"primer": 1}, [{"follower": 1}])
        if submission.primer is not None:
            print((await submission.primer).output)
        for fut in submission.followers:
            print((await fut).output)
    finally:
        await engine.close()


asyncio.run(main())
```

## CLI Usage

The package exposes `acty-tui`:

```bash
acty-tui follow /tmp/acty_events.jsonl
acty-tui replay /tmp/acty_events.jsonl --speed 2.0
```

## Example Programs

Run the TUI demo:

```bash
python examples/group_tui_demo.py --groups 3 --followers-per-group 2
```

Run the retry demo:

```bash
python examples/tenacity_retry_demo.py --event-jsonl /tmp/acty_tenacity_events.jsonl
```

## Package Relationships

- `acty-core` provides the low-level scheduler, lifecycle, event, and cache primitives
- `acty` provides the engine, client, TUI, and CLI built on top of `acty-core`
- `acty-langchain`, `acty-openai`, and `acty-gigachat` provide optional executor integrations

## Development

- tests live under `tests/`
- example programs live under `examples/`
- adapter-specific integration coverage belongs in the adapter repos, not in this repo
