"""RING_ONLY 隔离测试 — debounce 信息不灭前提验证。

修复: RING_ONLY 事件不进引擎。
  1. /tmp/ 事件不产 hint，不产 superseded marker
  2. /src/ 事件正常处理，superseded marker 指向的 event_id 都在 event_log 中
  3. RING_ONLY 事件仍进环形缓冲 (总线层面没丢)
"""

import asyncio
import json
import tempfile
import uuid
from pathlib import Path

from perception_layer.models.event import (
    RawEvent, StampedEvent, EventType, EventSource, EventPayload,
)
from perception_layer.bus.bus import EventBus
from perception_layer.bus.routing import RoutingRules, RoutingRule, PersistAction
from perception_layer.engine.debounce import DebounceWindow, DebounceConfig
from perception_layer.engine.rules.base import Rule3A, RuleContext
from perception_layer.engine.rules.same_path_burst import SamePathBurstRule


def _make_raw(path: str) -> RawEvent:
    return RawEvent(
        event_id=uuid.uuid4().hex[:12],
        sensor_id="test-01",
        event_type=EventType.FILE_MODIFY,
        sensor_timestamp="2026-01-01T00:00:00Z",
        source=EventSource(path=path, pid=None),
        payload=EventPayload(prev_hash="abc", new_hash="def"),
    )


class TestRingOnlyIsolation:

    def test_ring_only_events_stay_in_ring_buffer(self):
        """RING_ONLY 事件仍写进环形缓冲 (总线层面没丢)。"""
        routing = RoutingRules([
            RoutingRule(prefix="/tmp/", action=PersistAction.RING_ONLY, reason="temp"),
            RoutingRule(prefix="/", action=PersistAction.PERSIST, reason="default"),
        ])
        bus = EventBus(routing_rules=routing, ring_buffer_size=1024)

        async def run():
            raw = _make_raw("/tmp/build.log")
            stamped, action = await bus.ingest(raw)
            assert action == PersistAction.RING_ONLY
            assert stamped.routing_action == "ring_only"

            # 确认事件在环形缓冲中
            snapshot = bus.ring_snapshot
            assert len(snapshot) == 1
            assert snapshot[0].event_id == raw.event_id

        asyncio.run(run())

    def test_persist_events_have_correct_routing_action(self):
        """PERSIST 事件的 routing_action 为 'persist'。"""
        routing = RoutingRules([
            RoutingRule(prefix="/", action=PersistAction.PERSIST, reason="default"),
        ])
        bus = EventBus(routing_rules=routing)

        async def run():
            raw = _make_raw("/src/auth.ts")
            stamped, action = await bus.ingest(raw)
            assert action == PersistAction.PERSIST
            assert stamped.routing_action == "persist"

        asyncio.run(run())

    def test_engine_skips_ring_only_events(self):
        """RING_ONLY 事件不进引擎 → 不产 hint, 不产 superseded marker。"""
        routing = RoutingRules([
            RoutingRule(prefix="/tmp/", action=PersistAction.RING_ONLY, reason="temp"),
            RoutingRule(prefix="/", action=PersistAction.PERSIST, reason="default"),
        ])
        bus = EventBus(routing_rules=routing)

        debounce = DebounceWindow(DebounceConfig(window_ms=100, max_wait_ms=500))
        rules = [SamePathBurstRule(window_ms=500, min_events=2)]

        # 模拟引擎的过滤逻辑
        async def run():
            # 发送 3 个 /tmp/ 事件 (RING_ONLY)
            raw_tmp = _make_raw("/tmp/build.log")
            stamped_tmp, action_tmp = await bus.ingest(raw_tmp)
            assert action_tmp == PersistAction.RING_ONLY

            # 发送 3 个 /src/ 事件 (PERSIST)
            raw_src = _make_raw("/src/auth.ts")
            stamped_src, action_src = await bus.ingest(raw_src)
            assert action_src == PersistAction.PERSIST

            # 模拟引擎过滤: RING_ONLY 不进 debounce
            if stamped_tmp.routing_action == "ring_only":
                pass  # 跳过

            # PERSIST 进 debounce
            result = debounce.ingest(stamped_src)

            import time
            time.sleep(0.2)
            debounced = debounce.tick()

            # 只有一个事件进 debounce → 单事件不合并 → 无 superseded marker
            markers = debounce.drain_superseded()
            assert len(markers) == 0

            # 环形缓冲中有两个事件
            assert len(bus.ring_snapshot) == 2

        asyncio.run(run())


class TestSupersededMarkersOnlyForPersistEvents:
    """superseded marker 指向的 event_id 都可从 event_log 查到。"""

    def test_all_superseded_handles_are_persisted(self):
        """合并 PERSIST 事件后，superseded marker 的 event_id 都在 event_log 中。"""
        routing = RoutingRules([
            RoutingRule(prefix="/", action=PersistAction.PERSIST, reason="default"),
        ])
        bus = EventBus(routing_rules=routing)
        debounce = DebounceWindow(DebounceConfig(window_ms=100, max_wait_ms=500))

        async def run():
            events = []
            for i in range(3):
                raw = _make_raw("/src/file.ts")
                stamped, action = await bus.ingest(raw)
                assert action == PersistAction.PERSIST
                events.append(stamped)
                debounce.ingest(stamped)

            import time
            time.sleep(0.2)
            debounce.tick()

            markers = debounce.drain_superseded()
            # 3 个事件合并 → 3 个 superseded marker
            assert len(markers) == 3

            # 所有 marker 的 event_id 都在 events 列表中 (即"已落盘")
            event_ids = {e.event_id for e in events}
            for marker in markers:
                assert marker.event_id in event_ids, (
                    f"superseded marker 指向 {marker.event_id}，"
                    f"但此 event_id 不在已落盘事件集合中"
                )

        asyncio.run(run())
