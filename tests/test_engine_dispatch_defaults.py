from acty import EngineConfig
from acty_core.lifecycle import FollowerDispatchPolicy


def test_engine_config_defaults_to_target_dispatch() -> None:
    config = EngineConfig(follower_workers=3)
    policy = config.follower_dispatch_policy
    assert policy is not None
    assert policy.mode == "target"
    assert policy.target_total == 3


def test_engine_config_respects_explicit_policy() -> None:
    policy = FollowerDispatchPolicy.serial()
    config = EngineConfig(follower_dispatch_policy=policy)
    assert config.follower_dispatch_policy == policy
