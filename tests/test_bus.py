"""事件总线测试。

验证:
  1. 盖单调时钟戳 (地基 1)
  2. 路由规则机械执行
  3. 环形缓冲容量
  4. 总线不接受非传感器来源事件 (接口层面: 入口唯一的 ingest 只接受 RawEvent)
"""

import time
import uuid

from perception_layer.models.event import (
    RawEvent, StampedEvent, EventType, EventSource, EventPayload,
)
from perception_layer.bus.bus import EventBus
from perception_layer.bus.routing import RoutingRules, RoutingRule, PersistAction


def _make_raw(path: str | None = None) -> RawEvent:
    return RawEvent(
        event_id=uuid.uuid4().hex[:12],
        sensor_id="test-01",
        event_type=EventType.FILE_MODIFY,
        sensor_timestamp="2026-01-01T00:00:00Z",
        source=EventSource(path=path, pid=None),
        payload=EventPayload(),
    )


class TestBusIngest:
    """总线摄入: 盖戳 + 路由。"""

    def test_ingest_stamps_monotonic_timestamp(self):
        routing = RoutingRules([
            RoutingRule(prefix="/", action=PersistAction.PERSIST, reason="default"),
        ])
        bus = EventBus(routing_rules=routing)

        import asyncio
        async def run():
            raw = _make_raw("/src/test.ts")
            stamped, action = await bus.ingest(raw)

            # 验证盖戳
            assert stamped.event_id == raw.event_id
            assert stamped.bus_timestamp != ""
            assert int(stamped.bus_timestamp) >= 0  # ns since bus start
            assert stamped.sensor_timestamp == raw.sensor_timestamp
            assert action == PersistAction.PERSIST

        asyncio.run(run())

    def test_ingest_preserves_original_data(self):
        routing = RoutingRules([
            RoutingRule(prefix="/", action=PersistAction.PERSIST, reason="default"),
        ])
        bus = EventBus(routing_rules=routing)

        import asyncio
        async def run():
            raw = _make_raw("/src/auth.ts")
            stamped, _ = await bus.ingest(raw)

            assert stamped.sensor_id == raw.sensor_id
            assert stamped.event_type == raw.event_type
            assert stamped.source.path == "/src/auth.ts"

        asyncio.run(run())

    def test_monotonic_now_increases(self):
        routing = RoutingRules([
            RoutingRule(prefix="/", action=PersistAction.PERSIST, reason="default"),
        ])
        bus = EventBus(routing_rules=routing)

        t1 = int(bus.monotonic_now())
        time.sleep(0.01)
        t2 = int(bus.monotonic_now())
        assert t2 > t1


class TestBusRouting:
    """路由规则机械执行。"""

    def test_routing_match_persist(self):
        routing = RoutingRules([
            RoutingRule(prefix="/src/", action=PersistAction.PERSIST, reason="src code"),
            RoutingRule(prefix="/", action=PersistAction.PERSIST, reason="default"),
        ])
        bus = EventBus(routing_rules=routing)

        import asyncio
        async def run():
            _, action = await bus.ingest(_make_raw("/src/auth.ts"))
            assert action == PersistAction.PERSIST

        asyncio.run(run())

    def test_routing_match_ring_only(self):
        routing = RoutingRules([
            RoutingRule(prefix="/tmp/", action=PersistAction.RING_ONLY, reason="temp"),
            RoutingRule(prefix="/", action=PersistAction.PERSIST, reason="default"),
        ])
        bus = EventBus(routing_rules=routing)

        import asyncio
        async def run():
            _, action = await bus.ingest(_make_raw("/tmp/build.log"))
            assert action == PersistAction.RING_ONLY

        asyncio.run(run())

    def test_routing_longest_prefix_match(self):
        routing = RoutingRules([
            RoutingRule(prefix="/tmp/important/", action=PersistAction.PERSIST, reason="important temp"),
            RoutingRule(prefix="/tmp/", action=PersistAction.RING_ONLY, reason="temp"),
            RoutingRule(prefix="/", action=PersistAction.PERSIST, reason="default"),
        ])
        bus = EventBus(routing_rules=routing)

        import asyncio
        async def run():
            # /tmp/important/ 匹配最长前缀 → PERSIST
            _, action = await bus.ingest(_make_raw("/tmp/important/data.txt"))
            assert action == PersistAction.PERSIST

            # /tmp/other/ → RING_ONLY
            _, action = await bus.ingest(_make_raw("/tmp/other/log.txt"))
            assert action == PersistAction.RING_ONLY

        asyncio.run(run())

    def test_routing_null_path_defaults_persist(self):
        routing = RoutingRules([
            RoutingRule(prefix="/", action=PersistAction.PERSIST, reason="default"),
        ])
        bus = EventBus(routing_rules=routing)

        import asyncio
        async def run():
            _, action = await bus.ingest(_make_raw(path=None))
            assert action == PersistAction.PERSIST

        asyncio.run(run())


class TestBusRingBuffer:
    """环形缓冲容量。"""

    def test_ring_buffer_stores_events(self):
        routing = RoutingRules([
            RoutingRule(prefix="/", action=PersistAction.PERSIST, reason="default"),
        ])
        bus = EventBus(routing_rules=routing, ring_buffer_size=1024)

        import asyncio
        async def run():
            for i in range(10):
                await bus.ingest(_make_raw(f"/src/file_{i}.ts"))

            snapshot = bus.ring_snapshot
            assert len(snapshot) == 10

        asyncio.run(run())


class TestBusEntryPoint:
    """总线入口唯一: ingest(RawEvent)。无 inject_system_event (修正1)。"""

    def test_no_inject_system_event_method(self):
        """修正1: EventBus 没有 inject_system_event 方法。
        系统标记走 PerceptionLog.write_system_event，不经总线。
        """
        routing = RoutingRules([
            RoutingRule(prefix="/", action=PersistAction.PERSIST, reason="default"),
        ])
        bus = EventBus(routing_rules=routing)

        # 确认没有非传感器注入方法
        assert not hasattr(bus, "inject_system_event")
