"""编辑时间聚类检测。

Type A 探针: 连续事件间隔 < gap_threshold_ms，聚类长度 >= min_cluster_size。
Regime 1。
"""

import uuid
from typing import Optional

from perception_layer.models.event import StampedEvent, MergedEvent, PerceptionHint
from perception_layer.engine.rules.base import Rule3A, RuleContext


class EditClusterRule(Rule3A):
    """检测编辑活动的时序聚类。

    触发条件: 连续事件间隔 < gap_threshold_ms，聚类长度 >= min_cluster_size。

    Type A 探针:
      - 连续事件 bus_timestamp 差 < gap_threshold_ms (确定性)
      - 聚类中事件数 >= min_cluster_size (确定性)
    Regime 1。

    hint 示例: "8 次编辑事件在 1200ms 内聚集, 最大间隔 180ms"
    不报语义: 不说"这是活跃编码期"或"这是批量操作"。
    """

    def __init__(
        self,
        gap_threshold_ms: int = 500,
        min_cluster_size: int = 5,
    ) -> None:
        self._gap_threshold = gap_threshold_ms
        self._min_cluster_size = min_cluster_size

    @property
    def rule_id(self) -> str:
        return "edit_cluster"

    @property
    def type_a_probe(self) -> str:
        return (
            f"连续事件 bus_timestamp 差 < {self._gap_threshold}ms + "
            f"聚类长度 >= {self._min_cluster_size}"
        )

    @property
    def description(self) -> str:
        return (
            f"连续事件间隔 < {self._gap_threshold}ms 且 "
            f"聚类长度 >= {self._min_cluster_size}"
        )

    def match(
        self,
        event: MergedEvent | StampedEvent,
        context: RuleContext,
    ) -> PerceptionHint | None:
        """检测编辑时间聚类。

        算法: 遍历 recent_events，用贪心聚类:
          1. 两个连续事件间隔 < gap_threshold → 同一聚类
          2. 间隔 >= gap_threshold → 新聚类开始
          3. 聚类长度 >= min_cluster_size → 触发
        """
        events = context.recent_events
        if len(events) < self._min_cluster_size:
            return None

        # 贪心聚类
        clusters: list[list[StampedEvent | MergedEvent]] = []
        current_cluster: list[StampedEvent | MergedEvent] = [events[0]]

        for i in range(1, len(events)):
            prev_ts = int(events[i - 1].bus_timestamp)
            curr_ts = int(events[i].bus_timestamp)
            gap_ms = (curr_ts - prev_ts) / 1_000_000

            if gap_ms < self._gap_threshold:
                current_cluster.append(events[i])
            else:
                clusters.append(current_cluster)
                current_cluster = [events[i]]

        clusters.append(current_cluster)

        # 检查最后一个聚类 (当前事件所属的聚类)
        last_cluster = clusters[-1]
        if len(last_cluster) < self._min_cluster_size:
            return None

        # 计算聚类元数据
        first_ts = int(last_cluster[0].bus_timestamp)
        last_ts = int(last_cluster[-1].bus_timestamp)
        duration_ms = (last_ts - first_ts) / 1_000_000

        # 最大间隔
        max_gap = 0.0
        for i in range(1, len(last_cluster)):
            gap = (int(last_cluster[i].bus_timestamp) - int(last_cluster[i - 1].bus_timestamp)) / 1_000_000
            if gap > max_gap:
                max_gap = gap

        all_handles: list[str] = []
        for e in last_cluster:
            all_handles.append(e.event_id)
            if isinstance(e, MergedEvent):
                all_handles.extend(e.merged_from)

        return PerceptionHint(
            hint_id=uuid.uuid4().hex[:12],
            hint=(
                f"{len(last_cluster)} 个事件在 {duration_ms:.0f}ms 内聚集, "
                f"最大间隔 {max_gap:.0f}ms"
            ),
            handle=all_handles,
            rule_id=self.rule_id,
            bus_timestamp=event.bus_timestamp,
            frozen_semantic=True,
            type_a_probe=self.type_a_probe,
            regime="Regime 1",
        )
