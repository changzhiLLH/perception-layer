"""去抖窗口 — Regime 3 自动+审计。

冻结语义: 合并决策 — 把 N 个时间邻近的事件合并为 1 个 MergedEvent。
Type A 探针: 无 — 没有确定性判据说"合并对不对"。
  200ms 窗口是经验值，换编辑器/文件系统/网络文件系统，最佳窗口就变了。
处置: Regime 3 自动+审计。
  前提 (a) 信息不灭: merged_from + superseded marker 保留原始事件
  前提 (b) 窗口内损害可逆: Agent 可通过 handle 取回被合并的原始事件

设计 (补强1修正后):
  - 单事件不合并 — 不无谓违反原子性原则
  - 多事件合并 → MergedEvent 带 merged_from 字段 (信息不灭)
  - 被合并的原始事件标 superseded marker
  - atomicity_violation: True (钉死的冻结点)

关键区分 (来自审方案时的纠正):
  Type A 探针验的是"决策对不对"，不是"决策依据可不可算"。
  "时间戳差 < 200ms"是可算的 — 但"200ms 内的多次 modify 该不该合并"没有判据。
  因为信息丢失 (中间事件的 payload 不同) 是真实损害，
  而"200ms 是不是正确窗口"没有确定性答案。
  所以: 不是 Regime 1 (无 Type A 探针)，不是第四格 (信息不灭降低代价)，
  是 Regime 3 (自动+审计，Agent 可取回)。
"""

import asyncio
import time
import uuid
from dataclasses import dataclass, field

from perception_layer.models.event import (
    StampedEvent,
    MergedEvent,
    EventType,
    EventPayload,
    EventSource,
    SupersededMarker,
)


@dataclass
class DebounceConfig:
    """去抖窗口配置。

    冻结语义: 这些数值是经验值，没有 Type A 探针验证其正确性。
    """
    window_ms: int = 200           # 去抖窗口 (此窗口内的同路径事件被合并)
    max_wait_ms: int = 2000        # 最大等待时间 (防止连续写入永不触发)
    tick_interval_ms: int = 50     # 内部检查间隔


@dataclass
class _PendingWindow:
    """去抖窗口内部状态 — 一个路径对应一个。"""
    path: str
    events: list[StampedEvent] = field(default_factory=list)
    first_seen: float = 0.0        # 第一个事件到达时间 (time.monotonic)
    last_event: float = 0.0         # 最后一个事件到达时间


class DebounceWindow:
    """Regime 3 去抖窗口。

    核心逻辑:
      1. 摄入事件 → 按 source.path 分组
      2. 同一路径的新事件重置去抖计时器
      3. 计时器到期 (window_ms) → 产出 MergedEvent (如果 events > 1)
      4. 如果仅 1 个事件 → 原样通过，不合并 (不无谓违反原子性)
      5. 达到 max_wait_ms → 强制产出 (防止连续写入永不触发)
    """

    def __init__(self, config: DebounceConfig | None = None) -> None:
        self._config = config or DebounceConfig()
        self._pending: dict[str, _PendingWindow] = {}
        self._superseded_buffer: list[SupersededMarker] = []

    def ingest(self, event: StampedEvent) -> list[MergedEvent | StampedEvent]:
        """摄入事件。返回应被进一步处理的事件。

        Args:
            event: 总线盖戳后的事件

        Returns:
            []: 事件被吸收进去抖窗口，等待更多事件
            [MergedEvent]: 窗口到期，产出合并事件 (merge_count > 1)
            [StampedEvent]: 窗口到期，仅 1 个事件 — 不合并，原样通过
        """
        path = self._event_key(event)

        if path not in self._pending:
            # 新窗口
            now = time.monotonic()
            self._pending[path] = _PendingWindow(
                path=path,
                events=[event],
                first_seen=now,
                last_event=now,
            )
            return []

        # 已有窗口 → 加入事件，重置计时器
        window = self._pending[path]
        window.events.append(event)
        window.last_event = time.monotonic()

        return []

    def tick(self) -> list[MergedEvent | StampedEvent]:
        """周期性检查过期窗口。由引擎主循环调用。

        Returns:
            到期的窗口产出的事件列表
        """
        now = time.monotonic()
        results: list[MergedEvent | StampedEvent] = []

        expired_paths: list[str] = []
        for path, window in self._pending.items():
            elapsed = (now - window.first_seen) * 1000  # ms
            elapsed_since_last = (now - window.last_event) * 1000  # ms

            should_flush = (
                elapsed >= self._config.max_wait_ms  # 最大等待到
                or elapsed_since_last >= self._config.window_ms  # 去抖窗口到
            )

            if should_flush:
                expired_paths.append(path)
                result = self._build_result(window)
                results.append(result)

        for path in expired_paths:
            del self._pending[path]

        return results

    def flush(self) -> list[MergedEvent | StampedEvent]:
        """强制清空所有窗口 (停止时调用)。"""
        results: list[MergedEvent | StampedEvent] = []

        for window in list(self._pending.values()):
            results.append(self._build_result(window))

        self._pending.clear()
        return results

    def drain_superseded(self) -> list[SupersededMarker]:
        """取出本轮产生的 superseded marker，调用方写入 event_log.jsonl。
        调用后清空内部缓冲区。
        """
        markers = self._superseded_buffer[:]
        self._superseded_buffer.clear()
        return markers

    @property
    def pending_count(self) -> int:
        """当前等待中的窗口数 (调试/测试用)。"""
        return len(self._pending)

    @property
    def pending_event_count(self) -> int:
        """当前等待中的事件总数 (调试/测试用)。"""
        return sum(len(w.events) for w in self._pending.values())

    # --- 内部方法 ---

    @staticmethod
    def _event_key(event: StampedEvent) -> str:
        """事件的去抖分组 key。
        使用 source.path 作为唯一 key。
        无 path 的事件 (如 clock.tick) fallback 到 event_id →
        每个独立窗口，永不合并。
        正确: 时钟事件不该被去抖。不要改成固定 key——
        那样会把所有无 path 事件合并成一条，是灾难。
        原型阶段简化: 不区分 modify/create/delete 的事件类型合并，
        同路径所有文件事件合并在一起。
        """
        return event.source.path or event.event_id

    def _build_result(
        self, window: _PendingWindow
    ) -> MergedEvent | StampedEvent:
        """构建去抖窗口的输出事件。

        单事件 → 不合并，原样返回 StampedEvent (不违反原子性)
        多事件 → 合并为 MergedEvent (Regime 3，信息不灭)
        """
        events = window.events

        if len(events) == 1:
            # 单事件: 不合并，不违反原子性
            return events[0]

        # 多事件: 合并，保留信息
        last = events[-1]
        merged_from_ids = [e.event_id for e in events]

        merged = MergedEvent(
            event_id=uuid.uuid4().hex[:12],
            event_type=last.event_type,
            bus_timestamp=last.bus_timestamp,
            sensor_timestamp=last.sensor_timestamp,
            source=last.source,
            payload=last.payload,
            merged_from=merged_from_ids,
            merge_count=len(events),
            atomicity_violation=True,
            atomicity_violation_reason=(
                f"Windows 文件保存非原子性 + 去抖防抖动 {self._config.window_ms}ms"
            ),
        )

        # 生成 superseded marker (补强1: 信息不灭)
        for eid in merged_from_ids:
            self._superseded_buffer.append(
                SupersededMarker(
                    event_id=eid,
                    merged_into=merged.event_id,
                    bus_timestamp=merged.bus_timestamp,
                )
            )

        return merged
