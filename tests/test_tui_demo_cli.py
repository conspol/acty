from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_tui_module():
    root = Path(__file__).resolve().parents[1]
    module_path = root / "examples" / "group_tui_demo.py"
    spec = importlib.util.spec_from_file_location("acty_group_tui_demo", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Failed to load group_tui_demo module spec")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_tui_target_default_derived_from_workers() -> None:
    module = _load_tui_module()
    args = module.build_parser().parse_args(["--follower-workers", "3"])
    config = module.build_demo_config(args)
    policy = config.follower_dispatch_policy
    assert policy.mode == "target"
    assert policy.target_total == 3


def test_tui_target_zero_derived_from_workers() -> None:
    module = _load_tui_module()
    args = module.build_parser().parse_args(
        ["--follower-workers", "2", "--dispatch-target-total", "0"]
    )
    config = module.build_demo_config(args)
    policy = config.follower_dispatch_policy
    assert policy.mode == "target"
    assert policy.target_total == 2


def test_tui_serial_and_eager_do_not_require_target_total() -> None:
    module = _load_tui_module()
    serial_args = module.build_parser().parse_args(
        ["--dispatch-mode", "serial", "--dispatch-target-total", "0"]
    )
    serial_config = module.build_demo_config(serial_args)
    serial_policy = serial_config.follower_dispatch_policy
    assert serial_policy.mode == "serial"
    assert serial_policy.target_total is None

    eager_args = module.build_parser().parse_args(
        ["--dispatch-mode", "eager", "--dispatch-target-total", "0"]
    )
    eager_config = module.build_demo_config(eager_args)
    eager_policy = eager_config.follower_dispatch_policy
    assert eager_policy.mode == "eager"
    assert eager_policy.target_total is None
