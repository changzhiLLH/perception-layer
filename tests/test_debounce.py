"""去抖窗口测试 — Regime 3 关键实现。

验证:
  1. 单事件不合并 (不无谓违反原子性)
  2. 多事件合并 + merged_from 保留 (信息不灭)
  3. superseded marker 生成
  4. max_wait_ms 强制到期
  5. flush 清空所有窗口
"""

import time
import uuid

from perception_layer.models.event import (
    RawEvent, StampedEvent, MergedEvent, EventType,
    EventSource, EventPayload,
)
from perception_layer.engine.debounce import DebounceWindow, DebounceConfig


def _make_stamped(path: str, offset_ns: int = 0) -> StampedEvent:
    return StampedEvent(
        event_id=uuid.uuid4().hex[:12],
        sensor_id="test-01",
        event_type=EventType.FILE_MODIFY,
        bus_timestamp=str(offset_ns),
        sensor_timestamp="2026-01-01T00:00:00Z",
        source=EventSource(path=path, pid=None),
        payload=EventPayload(prev_hash="abc", new_hash="def"),
    )


class TestDebounceSingleEvent:
    """单事件: 不合并，原样通过。"""

    def test_single_event_passes_through(self):
        debounce = DebounceWindow(DebounceConfig(window_ms=200, max_wait_ms=2000))
        event = _make_stamped("/src/auth.ts", offset_ns=0)

        # 摄入后无直接产出 (等待窗口)
        result = debounce.ingest(event)
        assert result == []

        # tick 后窗口到期，但仅 1 个事件 → 不合并
        time.sleep(0.3)  # 300ms > 200ms window
        results = debounce.tick()
        assert len(results) == 1
        assert isinstance(results[0], StampedEvent)
        assert results[0].event_id == event.event_id

        # 无 superseded marker
        markers = debounce.drain_superseded()
        assert len(markers) == 0

    def test_single_event_no_atomicity_violation(self):
        """单事件通过时不标 atomicity_violation — 原子性未违反。"""
        debounce = DebounceWindow(DebounceConfig(window_ms=50, max_wait_ms=2000))
        event = _make_stamped("/src/auth.ts", offset_ns=0)

        debounce.ingest(event)
        time.sleep(0.15)
        results = debounce.tick()

        assert len(results) == 1
        # StampedEvent 无 atomicity_violation 属性
        assert not hasattr(results[0], "atomicity_violation")


class TestDebounceMerge:
    """多事件: 合并 + 信息不灭。"""

    def test_multiple_events_merged(self):
        debounce = DebounceWindow(DebounceConfig(window_ms=200, max_wait_ms=2000))

        e1 = _make_stamped("/src/auth.ts", offset_ns=0)
        e2 = _make_stamped("/src/auth.ts", offset_ns=50_000_000)  # 50ms
        e3 = _make_stamped("/src/auth.ts", offset_ns=100_000_000)  # 100ms

        debounce.ingest(e1)
        debounce.ingest(e2)
        debounce.ingest(e3)

        time.sleep(0.3)
        results = debounce.tick()

        assert len(results) == 1
        merged = results[0]
        assert isinstance(merged, MergedEvent)
        assert merged.merge_count == 3
        assert len(merged.merged_from) == 3
        assert e1.event_id in merged.merged_from
        assert e2.event_id in merged.merged_from
        assert e3.event_id in merged.merged_from

    def test_merged_from_preserves_original_event_ids(self):
        """merged_from 作为 handle — Agent 可回查原始事件。"""
        debounce = DebounceWindow(DebounceConfig(window_ms=200, max_wait_ms=2000))

        e1 = _make_stamped("/src/auth.ts", offset_ns=0)
        e2 = _make_stamped("/src/auth.ts", offset_ns=50_000_000)

        debounce.ingest(e1)
        debounce.ingest(e2)

        time.sleep(0.3)
        results = debounce.tick()

        merged = results[0]
        assert isinstance(merged, MergedEvent)
        # merged_from 包含所有被合并的原始 event_id
        assert set(merged.merged_from) == {e1.event_id, e2.event_id}

    def test_atomicity_violation_marked(self):
        """合并事件标 atomicity_violation: True (钉死的冻结点)。"""
        debounce = DebounceWindow(DebounceConfig(window_ms=200, max_wait_ms=2000))

        debounce.ingest(_make_stamped("/src/auth.ts", offset_ns=0))
        debounce.ingest(_make_stamped("/src/auth.ts", offset_ns=50_000_000))

        time.sleep(0.3)
        results = debounce.tick()

        merged = results[0]
        assert isinstance(merged, MergedEvent)
        assert merged.atomicity_violation is True
        assert "200ms" in merged.atomicity_violation_reason

    def test_superseded_markers_generated(self):
        """补强1: 被合并事件产生 superseded marker — 信息不灭。"""
        debounce = DebounceWindow(DebounceConfig(window_ms=200, max_wait_ms=2000))

        e1 = _make_stamped("/src/auth.ts", offset_ns=0)
        e2 = _make_stamped("/src/auth.ts", offset_ns=50_000_000)

        debounce.ingest(e1)
        debounce.ingest(e2)

        time.sleep(0.3)
        debounce.tick()

        markers = debounce.drain_superseded()
        assert len(markers) == 2  # e1, e2 都标 superseded

        marker_event_ids = {m.event_id for m in markers}
        assert marker_event_ids == {e1.event_id, e2.event_id}

        # 所有 marker 指向同一个 merged_into
        merged_ids = {m.merged_into for m in markers}
        assert len(merged_ids) == 1


class TestDebounceDifferentPaths:
    """不同路径不应合并。"""

    def test_different_paths_not_merged(self):
        debounce = DebounceWindow(DebounceConfig(window_ms=200, max_wait_ms=2000))

        debounce.ingest(_make_stamped("/src/auth.ts", offset_ns=0))
        debounce.ingest(_make_stamped("/src/login.ts", offset_ns=0))

        time.sleep(0.3)
        results = debounce.tick()

        # 两个独立路径 → 两个独立产出 (都是单事件，不合并)
        assert len(results) == 2
        for r in results:
            assert isinstance(r, StampedEvent)


class TestDebounceMaxWait:
    """max_wait_ms 强制到期。"""

    def test_max_wait_forces_flush(self):
        debounce = DebounceWindow(DebounceConfig(
            window_ms=200,
            max_wait_ms=500,  # 短最大等待
            tick_interval_ms=50,
        ))

        # 持续写入，每个事件重置 window 计时器
        # 但 500ms max_wait 会强制到期
        debounce.ingest(_make_stamped("/src/auth.ts", offset_ns=0))
        time.sleep(0.1)
        debounce.ingest(_make_stamped("/src/auth.ts", offset_ns=100_000_000))
        time.sleep(0.1)
        debounce.ingest(_make_stamped("/src/auth.ts", offset_ns=200_000_000))

        time.sleep(0.6)  # > 500ms max_wait
        results = debounce.tick()

        assert len(results) == 1
        merged = results[0]
        assert isinstance(merged, MergedEvent)
        assert merged.merge_count == 3


class TestDebounceFlush:
    """flush 强制清空所有窗口。"""

    def test_flush_clears_all(self):
        debounce = DebounceWindow(DebounceConfig(window_ms=200, max_wait_ms=2000))

        debounce.ingest(_make_stamped("/src/a.ts", offset_ns=0))
        debounce.ingest(_make_stamped("/src/b.ts", offset_ns=0))
        debounce.ingest(_make_stamped("/src/c.ts", offset_ns=0))

        # 不等待窗口到期，直接 flush
        results = debounce.flush()

        # 3 个单事件窗口 → 3 个独立产出
        assert len(results) == 3

        # flush 后窗口为空
        assert debounce.pending_count == 0
