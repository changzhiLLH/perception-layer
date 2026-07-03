"""同目录共变检测。

Type A 探针: 事件 source.path 的目录部分字符串相等 + 时间窗。
Regime 1。
"""

import os
import uuid
from typing import Optional

from perception_layer.models.event import StampedEvent, MergedEvent, PerceptionHint
from perception_layer.engine.rules.base import Rule3A, RuleContext


class SameDirCoModifyRule(Rule3A):
    """检测同一目录下多个文件在短时间内被修改。

    触发条件: >= min_files 个不同文件在同一目录下，window_ms 内被修改。

    Type A 探针:
      - source.path 的目录部分字符串相等 (确定性)
      - bus_timestamp 差 < window_ms (确定性)
      - 不同文件数 >= min_files (确定性)
    Regime 1。

    hint 示例: "5 文件同目录 src/auth/, 180ms 内修改"
    不报语义: 不说"这是重构"或"这是 git checkout"。
    """

    def __init__(self, window_ms: int = 200, min_files: int = 3) -> None:
        self._window_ms = window_ms
        self._min_files = min_files

    @property
    def rule_id(self) -> str:
        return "same_dir_comodify"

    @property
    def type_a_probe(self) -> str:
        return (
            f"不同文件的 os.path.dirname(source.path) 字符串相等 + "
            f"max(bus_timestamp) - min(bus_timestamp) < {self._window_ms}ms + "
            f"distinct_file_count >= {self._min_files}"
        )

    @property
    def description(self) -> str:
        return (
            f"同一目录下 >= {self._min_files} 个不同文件 "
            f"在 {self._window_ms}ms 内被修改"
        )

    def match(
        self,
        event: MergedEvent | StampedEvent,
        context: RuleContext,
    ) -> PerceptionHint | None:
        if event.source.path is None:
            return None

        event_dir = os.path.dirname(event.source.path.replace("\\", "/"))

        # 统计同目录下的事件
        window_events = context.events_in_window(self._window_ms)
        same_dir_events = [
            e for e in window_events
            if e.source.path
            and os.path.dirname(e.source.path.replace("\\", "/")) == event_dir
        ]

        # 不同文件数
        distinct_paths: set[str] = set()
        all_handles: list[str] = []
        for e in same_dir_events:
            if e.source.path:
                distinct_paths.add(e.source.path.replace("\\", "/"))
            all_handles.append(e.event_id)
            if isinstance(e, MergedEvent):
                all_handles.extend(e.merged_from)

        if len(distinct_paths) < self._min_files:
            return None

        return PerceptionHint(
            hint_id=uuid.uuid4().hex[:12],
            hint=(
                f"{len(distinct_paths)} 文件同目录 {event_dir}, "
                f"{self._window_ms}ms 内修改"
            ),
            handle=all_handles,
            rule_id=self.rule_id,
            bus_timestamp=event.bus_timestamp,
            frozen_semantic=True,
            type_a_probe=self.type_a_probe,
            regime="Regime 1",
        )
