"""事件原子性 + 数据模型测试。

验证:
  1. RawEvent 零判断 — 无 severity 字段
  2. MergedEvent atomicity_violation 标记
  3. SupersededMarker 信息不灭
  4. EventType 分类 Type A 可验
"""

import uuid

from perception_layer.models.event import (
    RawEvent, StampedEvent, MergedEvent, EventType,
    EventSource, EventPayload, PerceptionHint, SupersededMarker,
)


class TestRawEventZeroJudgment:
    """传感器零判断原则 — RawEvent 不含任何判断字段。"""

    def test_raw_event_has_no_severity(self):
        event = RawEvent(
            event_id="test-001",
            sensor_id="fs-watch-01",
            event_type=EventType.FILE_MODIFY,
            sensor_timestamp="2026-01-01T00:00:00Z",
            source=EventSource(path="/src/test.ts"),
            payload=EventPayload(),
        )

        # 确保没有 severity 字段
        assert not hasattr(event, "severity")
        assert not hasattr(event, "priority")
        assert not hasattr(event, "is_anomaly")

    def test_raw_event_has_no_is_anomaly(self):
        event = RawEvent(
            event_id="test-001",
            sensor_id="fs-watch-01",
            event_type=EventType.FILE_DELETE,
            sensor_timestamp="2026-01-01T00:00:00Z",
            source=EventSource(path="/src/test.ts"),
            payload=EventPayload(),
        )
        assert not hasattr(event, "is_anomaly")

    def test_event_type_is_type_a_verifiable(self):
        """EventType 分类 Type A 可验:
        file.modify → hash 变化 (可计算)
        file.create → inode 从无到有 (可计算)
        file.delete → inode 消失 (可计算)
        """
        # 断言这些分类都有明确的 Type A 探针
        assert EventType.FILE_MODIFY.value == "file.modify"
        assert EventType.FILE_CREATE.value == "file.create"
        assert EventType.FILE_DELETE.value == "file.delete"


class TestMergedEventAtomicityViolation:
    """钉死的冻结点: atomicity_violation 标记。"""

    def test_merged_event_has_atomicity_violation(self):
        merged = MergedEvent(
            event_id="merged-001",
            event_type=EventType.FILE_MODIFY,
            bus_timestamp="1000",
            sensor_timestamp="2026-01-01T00:00:00Z",
            source=EventSource(path="/src/test.ts"),
            payload=EventPayload(),
            merged_from=["evt-001", "evt-002"],
            merge_count=2,
            atomicity_violation=True,
            atomicity_violation_reason="Windows 文件保存非原子性 + 去抖防抖动 200ms",
        )

        assert merged.atomicity_violation is True
        assert "非原子性" in merged.atomicity_violation_reason
        assert len(merged.merged_from) == 2
        assert merged.merge_count == 2

    def test_merged_from_preserves_handles(self):
        """Agent 可通过 merged_from 的 handle 回查被合并的原始事件。"""
        handles = ["evt-001", "evt-002", "evt-003"]
        merged = MergedEvent(
            event_id="merged-001",
            event_type=EventType.FILE_MODIFY,
            bus_timestamp="1000",
            sensor_timestamp="2026-01-01T00:00:00Z",
            source=EventSource(path="/src/test.ts"),
            payload=EventPayload(),
            merged_from=handles,
            merge_count=len(handles),
            atomicity_violation=True,
            atomicity_violation_reason="test",
        )

        assert len(merged.merged_from) == 3
        for h in handles:
            assert h in merged.merged_from


class TestSupersededMarker:
    """SupersededMarker — 信息不灭的物化。"""

    def test_marker_points_to_merged_event(self):
        marker = SupersededMarker(
            event_id="evt-001",
            merged_into="merged-001",
            bus_timestamp="1000",
        )
        assert marker.marker_type == "superseded"
        assert marker.event_id == "evt-001"
        assert marker.merged_into == "merged-001"

    def test_agent_can_trace_from_original_to_merged(self):
        """Agent 回查原始事件 evt-001 → 发现 superseded marker → 追踪到 merged-001。
        这是 Regime 3 信息不灭的物理基础。
        """
        marker = SupersededMarker(
            event_id="evt-001",
            merged_into="merged-001",
        )
        # Agent 通过 query_by_handle("evt-001") 找到此 marker
        # → 根据 merged_into="merged-001" 回查合并事件
        assert marker.merged_into is not None


class TestPerceptionHint:
    """PerceptionHint — 引擎产出，只报结构特征。"""

    def test_hint_has_frozen_semantic_flag(self):
        hint = PerceptionHint(
            hint_id="hint-001",
            hint="5 文件同目录 src/auth/, 200ms 内",
            handle=["evt-001", "evt-002"],
            rule_id="same_dir_comodify",
            bus_timestamp="1000",
            frozen_semantic=True,
            type_a_probe="source.path 目录部分相等 + timestamp 差",
            regime="Regime 1",
        )

        assert hint.frozen_semantic is True
        assert hint.regime == "Regime 1"
        assert hint.type_a_probe != ""
        assert "重构" not in hint.hint  # 结构特征，不是语义结论

    def test_handle_provides_escape_valve(self):
        """handle 字段 — "可绕过"原则的物理基础。
        Agent 不信任 hint 时，可通过 handle 回查原始事件。
        """
        hint = PerceptionHint(
            hint_id="hint-001",
            hint="测试 hint",
            handle=["evt-001", "evt-002", "evt-003"],
            rule_id="test_rule",
            bus_timestamp="1000",
            frozen_semantic=True,
            type_a_probe="test",
            regime="Regime 1",
        )

        assert len(hint.handle) == 3
        for h in hint.handle:
            assert h.startswith("evt-")
