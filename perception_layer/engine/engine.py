"""L3 关联引擎主循环。

数据流:
  总线 (StampedEvent) → 去抖 (MergedEvent | StampedEvent) → 3a 规则 (PerceptionHint)

引擎输出写入 perception_log.jsonl (独立通道，不回总线 — 物理隔离，坑6)。
去抖产生的 superseded marker 写入 event_log.jsonl (不是感知通道 — 修正2)。
MergedEvent 不在 perception_log 中单独保存 — 它是引擎内部中间产物 (修正2)。
其信息通过 superseded marker + merged_from 在 event_log 中保留，Agent 可追溯。
"""

import asyncio
import time
from typing import AsyncIterator

from perception_layer.models.event import StampedEvent, MergedEvent, PerceptionHint, SupersededMarker
from perception_layer.bus.bus import EventBus
from perception_layer.engine.debounce import DebounceWindow
from perception_layer.engine.rules.base import Rule3A, RuleContext


class CorrelationEngine:
    """L3 关联引擎。

    主循环同时消费两个来源:
      - bus.stream(): 实时传感器事件
      - debounce tick: 去抖窗口过期事件

    两个来源的产出合并后依次应用 3a 规则。

    引擎输出走独立通道 (perception_log.jsonl)，不进总线。
    去抖 superseded marker 走 event_log.jsonl (修正2)。
    """

    def __init__(
        self,
        bus: EventBus,
        debounce: DebounceWindow,
        rules: list[Rule3A],
        context_window_size: int = 500,
    ) -> None:
        self._bus = bus
        self._debounce = debounce
        self._rules = rules
        self._context_window_size = context_window_size

        # 最近事件窗口 (供 3a 规则查询)
        self._recent_events: list[StampedEvent | MergedEvent] = []

    async def run(self) -> AsyncIterator[PerceptionHint]:
        """引擎主循环。

        Yields:
            PerceptionHint: 3a 规则匹配产出
        """

        # 去抖 tick 产出的事件通过此队列合并到主循环
        tick_queue: asyncio.Queue[StampedEvent | MergedEvent] = asyncio.Queue(
            maxsize=64
        )
        tick_interval = self._debounce._config.tick_interval_ms / 1000.0

        async def tick_producer() -> None:
            """后台周期性检查去抖窗口过期，产出事件推入队列。"""
            try:
                while True:
                    await asyncio.sleep(tick_interval)
                    for event in self._debounce.tick():
                        try:
                            tick_queue.put_nowait(event)
                        except asyncio.QueueFull:
                            # 队列满 → 丢弃 (原型容忍)
                            pass
            except asyncio.CancelledError:
                pass

        tick_task = asyncio.create_task(tick_producer())

        try:
            async for stamped in self._bus.stream():
                # RING_ONLY 事件不进引擎 — 它们未落盘，关联无意义
                # 且去抖合并生成的 superseded marker 会指向不存在的 event_id
                if stamped.routing_action == "ring_only":
                    continue

                # 1. 去抖摄入
                debounced = self._debounce.ingest(stamped)

                # 2. 处理去抖直接产出
                for event in debounced:
                    for hint in self._process_event(event):
                        yield hint

                # 3. 处理 tick 队列中的过期事件
                while not tick_queue.empty():
                    try:
                        event = tick_queue.get_nowait()
                        for hint in self._process_event(event):
                            yield hint
                    except asyncio.QueueEmpty:
                        break

        except asyncio.CancelledError:
            pass
        finally:
            tick_task.cancel()
            try:
                await tick_task
            except asyncio.CancelledError:
                pass

            # 停止时清空所有去抖窗口
            for event in self._debounce.flush():
                for hint in self._process_event(event):
                    yield hint

            # 清空 tick 队列残余
            while not tick_queue.empty():
                try:
                    event = tick_queue.get_nowait()
                    for hint in self._process_event(event):
                        yield hint
                except asyncio.QueueEmpty:
                    break

    def _process_event(
        self, event: StampedEvent | MergedEvent
    ) -> list[PerceptionHint]:
        """处理单个事件: 更新窗口 → 应用规则 → 返回 hints。"""
        # 添加到最近事件窗口
        self._recent_events.append(event)

        # 裁剪窗口大小
        if len(self._recent_events) > self._context_window_size:
            self._recent_events = self._recent_events[-self._context_window_size :]

        # 构建规则上下文
        context = RuleContext(recent_events=list(self._recent_events))

        # 依次应用所有规则
        hints: list[PerceptionHint] = []
        for rule in self._rules:
            try:
                hint = rule.match(event, context)
                if hint is not None:
                    hints.append(hint)
            except Exception as e:
                # 规则异常不阻塞引擎 — 跳过此规则
                import sys

                print(
                    f"[ENGINE] rule {rule.rule_id} threw:"
                    f" {type(e).__name__}: {e}",
                    file=sys.stderr,
                )

        return hints

    def drain_superseded(self) -> list[SupersededMarker]:
        """取出本轮去抖产生的 superseded marker。
        调用方负责写入 event_log.jsonl。
        """
        return self._debounce.drain_superseded()

    @property
    def recent_events(self) -> list[StampedEvent | MergedEvent]:
        """最近事件窗口快照 (调试/测试用)。"""
        return list(self._recent_events)
