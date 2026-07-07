"""SensorCooccurRule 单元测试。

测试:
  1. 跨传感器共现触发 (各 >= 1 + 总数 >= min_events)
  2. 单传感器不触发 (不同 sensor_id < 2)
  3. 总数不足不触发
  4. Type A 探针声明的正确性
  5. hint 不含语义词汇
  6. 非对称场景 (1 commit + 3 modify → 触发)
"""

import uuid

import pytest

from perception_layer.models.event import (
    StampedEvent,
    MergedEvent,
    EventType,
    EventSource,
    EventPayload,
    PerceptionHint,
)
from perception_layer.engine.rules.base import RuleContext
from perception_layer.engine.rules.sensor_cooccur import SensorCooccurRule


def _make_stamped(
    offset_ns: int,
    sensor_id: str = "fs-watch-01",
    path: str = "/src/test.ts",
) -> StampedEvent:
    return StampedEvent(
        event_id=uuid.uuid4().hex[:12],
        sensor_id=sensor_id,
        event_type=EventType.FILE_MODIFY,
        bus_timestamp=str(offset_ns),
        sensor_timestamp="2026-01-01T00:00:00Z",
        source=EventSource(path=path, pid=None),
        payload=EventPayload(),
    )


def _make_git_stamped(
    offset_ns: int,
    event_type: EventType = EventType.GIT_COMMIT,
    sensor_id: str = "git-01",
) -> StampedEvent:
    return StampedEvent(
        event_id=uuid.uuid4().hex[:12],
        sensor_id=sensor_id,
        event_type=event_type,
        bus_timestamp=str(offset_ns),
        sensor_timestamp="2026-01-01T00:00:00Z",
        source=EventSource(path=None, pid=None),
        payload=EventPayload(),
    )


class TestSensorCooccurRule:

    def test_triggers_on_cross_sensor_cooccur(self):
        """两个传感器各 >= 1 + 总数 >= min_events → 触发。"""
        rule = SensorCooccurRule(window_ms=500, min_events=3)

        events = [
            _make_stamped(offset_ns=0, sensor_id="fs-watch-01", path="/src/a.ts"),
            _make_stamped(offset_ns=20_000_000, sensor_id="fs-watch-01", path="/src/b.ts"),
            _make_git_stamped(offset_ns=30_000_000, event_type=EventType.GIT_COMMIT),
        ]
        context = RuleContext(recent_events=events)

        hint = rule.match(events[-1], context)
        assert hint is not None
        assert isinstance(hint, PerceptionHint)
        assert "fs-watch-01" in hint.hint
        assert "git-01" in hint.hint
        assert "500ms" in hint.hint
        assert hint.frozen_semantic is True
        assert hint.regime == "Regime 1"

    def test_no_trigger_single_sensor(self):
        """只有一个传感器 → 不触发。"""
        rule = SensorCooccurRule(window_ms=500, min_events=3)

        events = [
            _make_stamped(offset_ns=0, sensor_id="fs-watch-01", path="/src/a.ts"),
            _make_stamped(offset_ns=20_000_000, sensor_id="fs-watch-01", path="/src/b.ts"),
            _make_stamped(offset_ns=30_000_000, sensor_id="fs-watch-01", path="/src/c.ts"),
        ]
        context = RuleContext(recent_events=events)

        hint = rule.match(events[-1], context)
        assert hint is None

    def test_no_trigger_under_min_events(self):
        """总数 < min_events → 不触发。"""
        rule = SensorCooccurRule(window_ms=500, min_events=4)

        events = [
            _make_stamped(offset_ns=0, sensor_id="fs-watch-01", path="/src/a.ts"),
            _make_git_stamped(offset_ns=20_000_000, event_type=EventType.GIT_COMMIT),
        ]
        # Only 2 events, min_events=4
        context = RuleContext(recent_events=events)

        hint = rule.match(events[-1], context)
        assert hint is None

    def test_asymmetric_scenario(self):
        """1 commit + 3 modify → 触发 (非对称场景)。"""
        rule = SensorCooccurRule(window_ms=500, min_events=3)

        events = [
            _make_stamped(offset_ns=0, sensor_id="fs-watch-01", path="/src/a.ts"),
            _make_stamped(offset_ns=10_000_000, sensor_id="fs-watch-01", path="/src/b.ts"),
            _make_stamped(offset_ns=20_000_000, sensor_id="fs-watch-01", path="/src/c.ts"),
            _make_git_stamped(offset_ns=30_000_000, event_type=EventType.GIT_COMMIT),
        ]
        # 4 events: 3 from fs-watch + 1 from git → total 4 >= 3
        context = RuleContext(recent_events=events)

        hint = rule.match(events[-1], context)
        assert hint is not None
        assert "git-01 有 1 个" in hint.hint or "git-01 y" in hint.hint
        assert "fs-watch-01 有 3 个" in hint.hint or "fs-watch-01 y" in hint.hint

    def test_type_a_probe_declared(self):
        rule = SensorCooccurRule(window_ms=500, min_events=3)
        assert "min_events" in rule.type_a_probe or "sensor_id" in rule.type_a_probe
        assert "500ms" in rule.type_a_probe or "window" in rule.type_a_probe

    def test_window_boundary(self):
        """事件在窗口外的 → 不计入。"""
        rule = SensorCooccurRule(window_ms=100, min_events=3)

        # fs-watch events at 0ns, git event at 200_000_000ns (>100ms window)
        events = [
            _make_stamped(offset_ns=0, sensor_id="fs-watch-01", path="/src/a.ts"),
            _make_stamped(offset_ns=50_000_000, sensor_id="fs-watch-01", path="/src/b.ts"),
            _make_git_stamped(offset_ns=200_000_000, event_type=EventType.GIT_COMMIT),
        ]
        context = RuleContext(recent_events=events)

        hint = rule.match(events[-1], context)
        # git event is at 200ms, but window is 100ms. Only 0ns and 50ns events are in window (and only one sensor)
        assert hint is None

    def test_handle_collection(self):
        """handle 收集窗口内所有事件的 event_id。"""
        rule = SensorCooccurRule(window_ms=500, min_events=3)

        events = [
            _make_stamped(offset_ns=0, sensor_id="fs-watch-01"),
            _make_stamped(offset_ns=10_000_000, sensor_id="fs-watch-01"),
            _make_git_stamped(offset_ns=20_000_000),
        ]
        context = RuleContext(recent_events=events)

        hint = rule.match(events[-1], context)
        assert hint is not None
        assert len(hint.handle) == 3
        for e in events:
            assert e.event_id in hint.handle

    def test_dedup_same_signature(self):
        """同一窗口内相同传感器计数组合 → 不重复报。"""
        rule = SensorCooccurRule(window_ms=500, min_events=3)

        events = [
            _make_stamped(offset_ns=0, sensor_id="fs-watch-01"),
            _make_stamped(offset_ns=10_000_000, sensor_id="fs-watch-01"),
            _make_git_stamped(offset_ns=20_000_000),
        ]
        context = RuleContext(recent_events=events)

        # 第一次触发: 成功
        hint1 = rule.match(events[-1], context)
        assert hint1 is not None

        # 后续事件 (同窗口 + 同一传感器计数): 被去重
        hint2 = rule.match(events[-1], context)
        assert hint2 is None, "同一传感器计数组合不应重复报"


class TestSensorCooccurHintSemanticNeutrality:
    """hint 只报结构特征，不报语义结论。"""

    SEMANTIC_BANNED = [
        "重构", "refactor", "格式化", "format", "泄漏", "leak",
        "异常", "anomaly", "重要", "important", "严重", "severe",
        "引起了", "caused by", "因为", "because",
    ]

    def test_hint_no_semantic_terms(self):
        rule = SensorCooccurRule(window_ms=500, min_events=3)

        events = [
            _make_stamped(offset_ns=0, sensor_id="fs-watch-01"),
            _make_stamped(offset_ns=10_000_000, sensor_id="fs-watch-01"),
            _make_git_stamped(offset_ns=20_000_000),
        ]
        context = RuleContext(recent_events=events)

        hint = rule.match(events[-1], context)
        assert hint is not None
        hint_lower = hint.hint.lower()
        for banned in self.SEMANTIC_BANNED:
            assert banned.lower() not in hint_lower, (
                f"Hint 包含禁止的语义词汇 '{banned}': {hint.hint}"
            )
