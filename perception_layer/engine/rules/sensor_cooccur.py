"""跨传感器共现检测。

检测两个以上不同传感器在时间窗口内同时活跃的 pattern。
这是 perception-layer 核心差异化——跨感官关联。

冻结语义:
  Type A 探针: sensor_id 字符串集合 + 计数 + bus_timestamp 差 < window_ms。
  确定性可算: 不同实现同输入必同输出。
  Regime 1。

hint 只报结构特征 (传感器名 + 计数值 + 时间窗),不报因果推断。
不说 "git commit 引起了文件变更" —— 那是因果,没有 Type A 探针。
"""

import uuid
from typing import Optional

from perception_layer.models.event import StampedEvent, MergedEvent, PerceptionHint
from perception_layer.engine.rules.base import Rule3A, RuleContext


class SensorCooccurRule(Rule3A):
    """检测不同传感器在时间窗内的共现 pattern。

    触发条件:
      - window_ms 内 >= 2 个不同 sensor_id 各出现 >= 1 次
      - 且总事件数 >= min_events

    hint 示例: "传感器 fs-watch-01 有 5 个 + 传感器 git-01 有 1 个事件在 500ms 内"
    """

    def __init__(
        self,
        window_ms: int = 500,
        min_events: int = 3,
    ) -> None:
        self._window_ms = window_ms
        self._min_events = min_events
        self._last_signature: str = ""  # 去重: 同一窗口不重复报同一种子计数

    @property
    def rule_id(self) -> str:
        return "sensor_cooccur"

    @property
    def type_a_probe(self) -> str:
        return (
            f"不同 sensor_id 计数各 >= 1 + "
            f"总事件数 >= {self._min_events} + "
            f"max(bus_timestamp) - min(bus_timestamp) < {self._window_ms}ms"
        )

    @property
    def description(self) -> str:
        return (
            f"窗口 {self._window_ms}ms 内 >= 2 个传感器同时活跃,"
            f"总事件数 >= {self._min_events}"
        )

    def match(
        self,
        event: MergedEvent | StampedEvent,
        context: RuleContext,
    ) -> PerceptionHint | None:
        """检查当前窗口内是否有跨传感器共现。

        算法:
          1. 取 window_ms 内所有事件
          2. 按 sensor_id 分组计数
          3. 不同 sensor_id >= 2 + 总数 >= min_events → 触发
        """
        window_events = context.events_in_window(self._window_ms)
        if len(window_events) < self._min_events:
            return None

        # 按 sensor_id 分组计数
        sensor_counts: dict[str, int] = {}
        for e in window_events:
            sid = getattr(e, "sensor_id", "unknown")
            sensor_counts[sid] = sensor_counts.get(sid, 0) + 1

        distinct_sensors = len(sensor_counts)
        if distinct_sensors < 2:
            return None  # 只有一个传感器活跃，不触发

        total = sum(sensor_counts.values())
        if total < self._min_events:
            return None

        # 去重: 同一窗口内相同传感器计数组合不重复报
        sig = (
            ",".join(
                f"{sid}={cnt}"
                for sid, cnt in sorted(sensor_counts.items())
            )
        )
        if sig == self._last_signature:
            return None
        self._last_signature = sig

        # 构建 hint 文本: 列出各传感器的计数
        count_parts = [
            f"传感器 {sid} 有 {cnt} 个"
            for sid, cnt in sorted(sensor_counts.items())
        ]
        hint_text = (
            f"共 {total} 个事件来自 {distinct_sensors} 个传感器"
            f" ({', '.join(count_parts)}), 在 {self._window_ms}ms 内"
        )

        # handle 收集: 窗口内所有事件的 event_id
        all_handles: list[str] = []
        for e in window_events:
            all_handles.append(e.event_id)
            if isinstance(e, MergedEvent):
                all_handles.extend(e.merged_from)

        return PerceptionHint(
            hint_id=uuid.uuid4().hex[:12],
            hint=hint_text,
            handle=all_handles,
            rule_id=self.rule_id,
            bus_timestamp=event.bus_timestamp,
            frozen_semantic=True,
            type_a_probe=self.type_a_probe,
            regime="Regime 1",
        )
