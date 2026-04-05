"""Lane helper for tagging groups with lane metadata."""

from __future__ import annotations

from typing import Any, Mapping

from acty_core.scheduler import LaneConfig

from .groups import CacheKeyResolver, GroupCloseHook, GroupContextBuilder, GroupIdResolver, GroupRegistry

_LANE_UNSET = object()


class Lane:
    def __init__(self, engine: Any, lane_id: str, *, weight: float | None = None) -> None:
        if not isinstance(lane_id, str) or not lane_id:
            raise ValueError("lane_id must be a non-empty string")
        self._engine = engine
        self._lane_id = lane_id
        default_config = None
        if hasattr(engine, "update_lane_config"):
            scheduler = getattr(engine, "_scheduler", None)
            if scheduler is not None:
                default_config = getattr(scheduler, "_lane_default", None)
                if not isinstance(default_config, LaneConfig):
                    snapshot = getattr(scheduler, "_lane_snapshot", None)
                    if snapshot is not None:
                        default_config = getattr(snapshot, "default_config", None)
            if not isinstance(default_config, LaneConfig):
                engine_config = getattr(engine, "_config", None)
                if engine_config is not None:
                    default_config = getattr(engine_config, "lane_default", None)
        if not isinstance(default_config, LaneConfig):
            default_config = LaneConfig()
        resolved_weight = default_config.weight if weight is None else weight
        self._weight = float(resolved_weight)
        if hasattr(engine, "update_lane_config"):
            unset = getattr(engine, "UNSET", None)
            if unset is None:
                update_kwargs: dict[str, object] = {
                    "lane_id": lane_id,
                    "ensure": True,
                }
                if weight is not None:
                    update_kwargs["weight"] = weight
                try:
                    engine.update_lane_config(**update_kwargs)
                except TypeError as exc:
                    raise TypeError(
                        "engine.update_lane_config must accept omitted fields when UNSET is unavailable"
                    ) from exc
            else:
                weight_value: float | object = weight if weight is not None else unset
                if weight is not None and weight == default_config.weight:
                    weight_value = unset
                engine.update_lane_config(
                    lane_id=lane_id,
                    weight=weight_value,
                    ensure=True,
                )

    @property
    def lane_id(self) -> str:
        return self._lane_id

    @property
    def weight(self) -> float:
        return self._weight

    def update(
        self,
        *,
        weight: float | None = None,
        max_inflight: int | None | object = _LANE_UNSET,
    ) -> None:
        if weight is None and max_inflight is _LANE_UNSET:
            return
        engine = self._engine
        unset = getattr(engine, "UNSET", None)
        if unset is None:
            update_kwargs: dict[str, object] = {
                "lane_id": self._lane_id,
                "ensure": True,
            }
            if weight is not None:
                update_kwargs["weight"] = weight
            if max_inflight is not _LANE_UNSET:
                update_kwargs["max_inflight"] = max_inflight
            try:
                engine.update_lane_config(**update_kwargs)
            except TypeError as exc:
                raise TypeError(
                    "engine.update_lane_config must accept omitted fields when UNSET is unavailable"
                ) from exc
        else:
            weight_value: float | object = unset if weight is None else weight
            max_inflight_value: int | None | object = (
                unset if max_inflight is _LANE_UNSET else max_inflight
            )
            engine.update_lane_config(
                lane_id=self._lane_id,
                weight=weight_value,
                max_inflight=max_inflight_value,
                ensure=True,
            )
        if weight is not None:
            self._weight = float(weight)

    def registry(
        self,
        *,
        group_id_resolver: GroupIdResolver,
        cache_key_resolver: CacheKeyResolver | None = None,
        group_context_builder: GroupContextBuilder | None = None,
        on_group_close: GroupCloseHook | None = None,
    ) -> GroupRegistry:
        def _with_lane(key):
            base_context = None
            if group_context_builder is not None:
                base_context = group_context_builder(key)
            if base_context is None:
                return {"lane": self._lane_id}
            if not isinstance(base_context, Mapping):
                raise TypeError("group_context_builder must return a mapping or None")
            merged = dict(base_context)
            lane_value = merged.get("lane")
            if lane_value is None or (isinstance(lane_value, str) and not lane_value):
                merged["lane"] = self._lane_id
            return merged

        return GroupRegistry(
            self._engine,
            group_id_resolver=group_id_resolver,
            cache_key_resolver=cache_key_resolver,
            group_context_builder=_with_lane,
            on_group_close=on_group_close,
        )


__all__ = ["Lane"]
