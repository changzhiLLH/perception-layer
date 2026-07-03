"""3a 规则基类。

3a 规则层: 确定性条件→动作，零幻觉，0 延迟。
所有规则 Type A 可验 — 不同实现同输入必产出同输出。

关键约束 (文档 L3 修正):
  引擎只报结构特征，不报语义结论。
  hint 字段内容必须是结构特征描述 — "N 文件同目录 T ms 内"。
  不能是语义判断 — "这是重构" / "这是格式化" / "这是泄漏"。

宪法审查:
  每条规则必须声明 type_a_probe (可验证的探针)。
  hint 标记 frozen_semantic: True (显式登记)。
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional

from perception_layer.models.event import StampedEvent, MergedEvent, PerceptionHint


@dataclass
class RuleContext:
    """提供给 3a 规则的查询上下文。只读，不修改。

    原型实现: 持有最近 N 个事件的引用。
    后续可扩展: 索引 by path / by directory。
    """

    recent_events: list[StampedEvent | MergedEvent]

    def events_in_window(self, window_ms: float) -> list[StampedEvent | MergedEvent]:
        """最近 window_ms 内的事件 (按 bus_timestamp 筛选)。

        bus_timestamp 是 ns-since-bus-start 字符串，需要解析回整数。
        """
        if not self.recent_events:
            return []

        latest_ns = int(self.recent_events[-1].bus_timestamp)
        window_ns = int(window_ms * 1_000_000)
        cutoff = latest_ns - window_ns

        return [
            e for e in self.recent_events
            if int(e.bus_timestamp) >= cutoff
        ]

    def events_by_path(self, path: str) -> list[StampedEvent | MergedEvent]:
        """指定路径的历史事件。"""
        normalized = path.replace("\\", "/")
        return [
            e for e in self.recent_events
            if e.source.path and e.source.path.replace("\\", "/") == normalized
        ]

    def events_by_directory(self, directory: str) -> list[StampedEvent | MergedEvent]:
        """指定目录下所有文件的历史事件。"""
        import os
        normalized = directory.replace("\\", "/").rstrip("/") + "/"
        return [
            e for e in self.recent_events
            if e.source.path
            and e.source.path.replace("\\", "/").startswith(normalized)
        ]


class Rule3A(ABC):
    """3a 规则基类。确定性条件→动作，零幻觉，0 延迟。

    每条规则必须声明:
      - rule_id: 唯一标识
      - type_a_probe: Type A 探针描述 (宪法第 1 条)
      - description: 人类可读的描述

    match() 返回 PerceptionHint，不返回语义结论。
    """

    @property
    @abstractmethod
    def rule_id(self) -> str:
        """规则唯一标识。"""
        ...

    @property
    @abstractmethod
    def type_a_probe(self) -> str:
        """Type A 探针描述。

        例如: "N 事件同路径 source.path 字符串相等 +
               max(bus_timestamp) - min(bus_timestamp) < window_ms → 确定性可算"
        """
        ...

    @property
    @abstractmethod
    def description(self) -> str:
        """人类可读的规则描述。"""
        ...

    @abstractmethod
    def match(
        self,
        event: MergedEvent | StampedEvent,
        context: RuleContext,
    ) -> PerceptionHint | None:
        """尝试匹配规则。

        Args:
            event: 当前事件 (可以是 MergedEvent 或 StampedEvent)
            context: 查询上下文 (recent_events)

        Returns:
            PerceptionHint: 规则匹配成功，产出结构特征描述
            None: 不匹配
        """
        ...
