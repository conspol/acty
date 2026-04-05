from __future__ import annotations

import pytest

from acty import EngineConfig

pytest_plugins = [
    "tests.support.tui_harness",
]


@pytest.fixture
def engine_config_small() -> EngineConfig:
    return EngineConfig(primer_workers=1, follower_workers=1)
