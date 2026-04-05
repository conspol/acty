from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

import pytest


@asynccontextmanager
async def run_tui_app(app: Any, *, size: tuple[int, int] = (80, 24)) -> AsyncIterator[Any]:
    async with app.run_test(size=size) as pilot:
        yield pilot


@pytest.fixture
def tui_runner():
    return run_tui_app
