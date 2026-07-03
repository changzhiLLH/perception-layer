"""同路径 burst 检测。

Type A 探针: source.path 字符串相等 + timestamp 差 < window_ms。
Regime 1。
"""

import uuid
from typing import Optional

from perception_layer.models.event import StampedEvent, MergedEvent, PerceptionHint
from perception_layer.engine.rules.base import Rule3A, RuleContext


class SamePathBurstRule(Rule3A):
    """检测同一文件在短时间内被多次修改。

    触发条件: N 个事件 (modify/create/delete) 在同一 source.path 上，
    window_ms 时间窗口内。

    Type A 探针:
      - source.path 字符串相等 (确定性)
      - max(bus_timestamp) - min(bus_timestamp) < window_ms (确定性)
      - event_count >= min_events (确定性)
    Regime 1。

    hint 示例: "3 次修改同一文件 src/auth.ts, 150ms 内"
    不对语义做判断: 不说"这是编辑器自动保存"或"这是用户反复调整"。
    """

    def __init__(self, window_ms: int = 200, min_events: int = 3) -> None:
        self._window_ms = window_ms
        self._min_events = min_events

    @property
    def rule_id(self) -> str:
        return "same_path_burst"

    @property
    def type_a_probe(self) -> str:
        return (
            f"source.path 字符串相等 + max(bus_timestamp) - min(bus_timestamp) "
            f"< {self._window_ms}ms + event_count >= {self._min_events}"
        )

    @property
    def description(self) -> str:
        return (
            f"同一文件在 {self._window_ms}ms 内被修改 >= {self._min_events} 次"
        )

    def match(
        self,
        event: MergedEvent | StampedEvent,
        context: RuleContext,
    ) -> PerceptionHint | None:
        """检查是否有同路径 burst 模式。

        策略: 在每个事件上检查 — 统计此路径在窗口内的总事件数。
        (原始事件 + 合并事件均计入，MergedEvent 的 merge_count 会被计入)
        """
        if event.source.path is None:
            return None

        path = event.source.path

        # 统计此路径在窗口内的事件数
        window_events = context.events_in_window(self._window_ms)
        same_path_events = [
            e for e in window_events
            if e.source.path and e.source.path.replace("\\", "/") == path.replace("\\", "/")
        ]

        # 计算有效事件数 (MergedEvent 的 merge_count 计入)
        effective_count = 0
        for e in same_path_events:
            if isinstance(e, MergedEvent):
                effective_count += e.merge_count
            else:
                effective_count += 1

        if effective_count < self._min_events:
            return None

        # 只报最新一次触发，避免同一 burst 重复报警
        # 如果这是窗口内此路径的第一个事件，抑制后续
        # 简化: 每次匹配都报 — 原型容忍少量重复

        return PerceptionHint(
            hint_id=uuid.uuid4().hex[:12],
            hint=f"{effective_count} 次修改同一文件 {path}, {self._window_ms}ms 内",
            handle=[e.event_id for e in same_path_events],
            rule_id=self.rule_id,
            bus_timestamp=event.bus_timestamp,
            frozen_semantic=True,
            type_a_probe=self.type_a_probe,
            regime="Regime 1",
        )
