"""3a 规则测试。

验证:
  1. 规则只报结构特征 (不报语义结论)
  2. Type A 探针可验证
  3. 不同规则同时匹配
"""

import uuid

from perception_layer.models.event import (
    StampedEvent, MergedEvent, EventType, EventSource, EventPayload,
    PerceptionHint,
)
from perception_layer.engine.rules.base import RuleContext
from perception_layer.engine.rules.same_path_burst import SamePathBurstRule
from perception_layer.engine.rules.same_dir_comodify import SameDirCoModifyRule
from perception_layer.engine.rules.edit_cluster import EditClusterRule


def _make_stamped(path: str, offset_ns: int) -> StampedEvent:
    return StampedEvent(
        event_id=uuid.uuid4().hex[:12],
        sensor_id="test-01",
        event_type=EventType.FILE_MODIFY,
        bus_timestamp=str(offset_ns),
        sensor_timestamp="2026-01-01T00:00:00Z",
        source=EventSource(path=path, pid=None),
        payload=EventPayload(),
    )


def _make_merged(path: str, offset_ns: int, merged_count: int) -> MergedEvent:
    return MergedEvent(
        event_id=uuid.uuid4().hex[:12],
        event_type=EventType.FILE_MODIFY,
        bus_timestamp=str(offset_ns),
        sensor_timestamp="2026-01-01T00:00:00Z",
        source=EventSource(path=path, pid=None),
        payload=EventPayload(),
        merged_from=[uuid.uuid4().hex[:12] for _ in range(merged_count)],
        merge_count=merged_count,
        atomicity_violation=True,
        atomicity_violation_reason="test",
    )


class TestSamePathBurstRule:

    def test_triggers_on_same_path_burst(self):
        rule = SamePathBurstRule(window_ms=500, min_events=3)

        events = [
            _make_stamped("/src/auth.ts", offset_ns=0),
            _make_stamped("/src/auth.ts", offset_ns=50_000_000),    # +50ms
            _make_stamped("/src/auth.ts", offset_ns=100_000_000),   # +100ms
        ]
        context = RuleContext(recent_events=events)

        hint = rule.match(events[-1], context)
        assert hint is not None
        assert isinstance(hint, PerceptionHint)
        # hint 是结构特征，不是语义结论
        assert "/src/auth.ts" in hint.hint
        assert "500ms" in hint.hint or "3" in hint.hint  # 至少包含计数或窗口
        assert "重构" not in hint.hint
        assert "格式化" not in hint.hint
        assert hint.frozen_semantic is True
        assert hint.regime == "Regime 1"

    def test_no_trigger_on_different_paths(self):
        rule = SamePathBurstRule(window_ms=500, min_events=3)

        events = [
            _make_stamped("/src/auth.ts", offset_ns=0),
            _make_stamped("/src/login.ts", offset_ns=50_000_000),
            _make_stamped("/src/admin.ts", offset_ns=100_000_000),
        ]
        context = RuleContext(recent_events=events)

        hint = rule.match(events[-1], context)
        assert hint is None

    def test_no_trigger_under_min_events(self):
        rule = SamePathBurstRule(window_ms=500, min_events=3)

        events = [
            _make_stamped("/src/auth.ts", offset_ns=0),
            _make_stamped("/src/auth.ts", offset_ns=50_000_000),
        ]
        context = RuleContext(recent_events=events)

        hint = rule.match(events[-1], context)
        assert hint is None

    def test_type_a_probe_declared(self):
        rule = SamePathBurstRule(window_ms=200, min_events=3)
        assert "source.path" in rule.type_a_probe
        assert "bus_timestamp" in rule.type_a_probe
        assert "200ms" in rule.type_a_probe


class TestSameDirCoModifyRule:

    def test_triggers_on_same_dir_comodify(self):
        rule = SameDirCoModifyRule(window_ms=500, min_files=3)

        events = [
            _make_stamped("/src/auth/login.ts", offset_ns=0),
            _make_stamped("/src/auth/logout.ts", offset_ns=30_000_000),
            _make_stamped("/src/auth/session.ts", offset_ns=60_000_000),
        ]
        context = RuleContext(recent_events=events)

        hint = rule.match(events[-1], context)
        assert hint is not None
        assert isinstance(hint, PerceptionHint)
        assert "src/auth" in hint.hint
        # hint 是结构特征，不是语义结论
        assert "重构" not in hint.hint
        assert "git checkout" not in hint.hint
        assert hint.frozen_semantic is True
        assert hint.regime == "Regime 1"

    def test_no_trigger_under_min_files(self):
        rule = SameDirCoModifyRule(window_ms=500, min_files=3)

        events = [
            _make_stamped("/src/auth/login.ts", offset_ns=0),
            _make_stamped("/src/auth/logout.ts", offset_ns=30_000_000),
        ]
        context = RuleContext(recent_events=events)

        hint = rule.match(events[-1], context)
        assert hint is None


class TestEditClusterRule:

    def test_triggers_on_edit_cluster(self):
        rule = EditClusterRule(gap_threshold_ms=500, min_cluster_size=5)

        # 5 个事件在紧密时间窗内
        events = [
            _make_stamped("/src/a.ts", offset_ns=0),
            _make_stamped("/src/b.ts", offset_ns=100_000_000),     # +100ms
            _make_stamped("/src/c.ts", offset_ns=200_000_000),     # +200ms
            _make_stamped("/src/d.ts", offset_ns=300_000_000),     # +300ms
            _make_stamped("/src/e.ts", offset_ns=400_000_000),     # +400ms
        ]
        context = RuleContext(recent_events=events)

        hint = rule.match(events[-1], context)
        assert hint is not None
        assert isinstance(hint, PerceptionHint)
        assert "聚集" in hint.hint or "cluster" in hint.hint.lower()
        assert hint.frozen_semantic is True
        assert hint.regime == "Regime 1"

    def test_no_trigger_on_sparse_events(self):
        rule = EditClusterRule(gap_threshold_ms=100, min_cluster_size=3)

        # 事件间隔 > gap_threshold → 不聚类
        events = [
            _make_stamped("/src/a.ts", offset_ns=0),
            _make_stamped("/src/b.ts", offset_ns=500_000_000),     # +500ms > 100ms gap
            _make_stamped("/src/c.ts", offset_ns=1_000_000_000),   # +1000ms > 100ms gap
        ]
        context = RuleContext(recent_events=events)

        hint = rule.match(events[-1], context)
        assert hint is None

    def test_no_trigger_under_min_cluster_size(self):
        rule = EditClusterRule(gap_threshold_ms=500, min_cluster_size=5)

        events = [
            _make_stamped("/src/a.ts", offset_ns=0),
            _make_stamped("/src/b.ts", offset_ns=100_000_000),
            _make_stamped("/src/c.ts", offset_ns=200_000_000),
        ]
        context = RuleContext(recent_events=events)

        hint = rule.match(events[-1], context)
        assert hint is None


class TestHintSemanticNeutrality:
    """所有 hint 必须只报结构特征，不报语义结论。"""

    SEMANTIC_BANNED = [
        "重构", "refactor", "格式化", "format", "泄漏", "leak",
        "异常", "anomaly", "重要", "important", "严重", "severe",
        "批量操作", "batch", "编辑器", "editor", "自动保存",
    ]

    def _check_hint(self, hint: PerceptionHint):
        hint_lower = hint.hint.lower()
        for banned in self.SEMANTIC_BANNED:
            assert banned.lower() not in hint_lower, (
                f"Hint 包含禁止的语义词汇 '{banned}': {hint.hint}"
            )

    def test_burst_hint_no_semantic_terms(self):
        rule = SamePathBurstRule(window_ms=500, min_events=3)
        events = [
            _make_stamped("/src/auth.ts", offset_ns=i * 50_000_000)
            for i in range(3)
        ]
        context = RuleContext(recent_events=events)
        hint = rule.match(events[-1], context)
        if hint:
            self._check_hint(hint)

    def test_comodify_hint_no_semantic_terms(self):
        rule = SameDirCoModifyRule(window_ms=500, min_files=3)
        events = [
            _make_stamped(f"/src/auth/file_{i}.ts", offset_ns=i * 30_000_000)
            for i in range(3)
        ]
        context = RuleContext(recent_events=events)
        hint = rule.match(events[-1], context)
        if hint:
            self._check_hint(hint)

    def test_cluster_hint_no_semantic_terms(self):
        rule = EditClusterRule(gap_threshold_ms=500, min_cluster_size=5)
        events = [
            _make_stamped(f"/src/file_{i}.ts", offset_ns=i * 100_000_000)
            for i in range(5)
        ]
        context = RuleContext(recent_events=events)
        hint = rule.match(events[-1], context)
        if hint:
            self._check_hint(hint)
