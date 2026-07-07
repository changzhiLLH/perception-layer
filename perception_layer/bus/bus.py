"""L2 事件总线 — 哑管道。

职责 (只做这四件事):
  1. 接收传感器事件 (入口唯一: ingest)
  2. 盖单调时钟戳 (地基 1)
  3. 按路由规则返回 PersistAction (不做"是否落盘"的判断)
  4. 分发事件给引擎订阅者

不做:
  - 不判断: 不做"这个事件重不重要"的判断
  - 不去重: 不合并重复事件 (去抖是引擎的事)
  - 不聚合: 不把多个事件合成一个
  - 不推断因果: 不做"这个事件引发了那个事件"
  - 不接受非传感器来源事件 (修正1: inject_system_event 已删除)

硬约束:
  总线入口只有一个 — ingest(event: RawEvent)
  系统标记 (sensor.offline / bus.restart) 走 PerceptionLog.write_system_event，
  不经总线环形缓冲 (自报通道独立)。
"""

import asyncio
import time
import uuid
from typing import AsyncIterator

from perception_layer.models.event import RawEvent, StampedEvent, EventType
from perception_layer.bus.ring_buffer import RingBuffer
from perception_layer.bus.routing import RoutingRules, PersistAction


class EventBus:
    """L2 事件总线。哑管道 — 对事件内容不做任何判断。

    总线不接受非传感器来源事件。
    系统标记 (sensor.offline / bus.restart) 不经此总线。
    """

    def __init__(
        self,
        routing_rules: RoutingRules,
        ring_buffer_size: int = 2048,
    ) -> None:
        self._routing = routing_rules
        self._ring: RingBuffer[StampedEvent] = RingBuffer(ring_buffer_size)
        self._start_monotonic = time.monotonic_ns()

        # 引擎订阅者列表 (每个订阅者是一个 asyncio.Queue)
        self._subscribers: list[asyncio.Queue[StampedEvent]] = []

    async def ingest(self, event: RawEvent) -> tuple[StampedEvent, PersistAction]:
        """传感器 → 总线入口。入口唯一。

        处理步骤:
          1. 盖单调时钟戳 (地基 1: 统一参照系)
          2. 推入环形缓冲
          3. 根据路由规则返回 PersistAction
          4. 分发给所有引擎订阅者

        Args:
            event: 传感器产出的原始事件 (RawEvent)

        Returns:
            (stamped_event, persist_action)
            - stamped_event: 盖戳后的事件
            - persist_action: 调用方根据此决定是否写 event_log
              调用方应机械执行 if-else，不做判断

        调用方职责 (main.py):
          if action == PERSIST:
              await log.write_event(stamped_event)
          # else: 事件已在环形缓冲，不落盘
          这个 if-else 是机械执行，不是判断 — main.py 不评估"路径是否关键"。
        """
        # 1. 盖单调时钟戳 (地基 1)
        now_ns = time.monotonic_ns()
        bus_timestamp = str(now_ns - self._start_monotonic)

        # 3. 路由规则匹配 (机械执行)
        action = self._routing.match(event.source.path)

        stamped = StampedEvent(
            event_id=event.event_id,
            sensor_id=event.sensor_id,
            event_type=event.event_type,
            bus_timestamp=bus_timestamp,
            sensor_timestamp=event.sensor_timestamp,
            source=event.source,
            payload=event.payload,
            routing_action=action.value,  # 路由结果 Type A 可验
        )

        # 2. 推入环形缓冲
        self._ring.push(stamped)

        # 4. 分发给引擎订阅者
        for queue in self._subscribers:
            try:
                queue.put_nowait(stamped)
            except asyncio.QueueFull:
                # 订阅者消费太慢，跳过——不阻塞总线
                pass

        return stamped, action

    async def stream(
        self, shutdown: asyncio.Event | None = None
    ) -> AsyncIterator[StampedEvent]:
        """引擎订阅接口。从环形缓冲消费事件。

        引擎通过此方法获取事件流，送入去抖 → 3a 规则。
        返回的 AsyncIterator 持续产出新事件。

        Args:
            shutdown: 关闭信号。设置后，stream 在下一次 queue.get 超时后退出。
                      用 timeout 轮询，不用永久阻塞，以便检查 shutdown。
        """
        queue: asyncio.Queue[StampedEvent] = asyncio.Queue(maxsize=256)
        self._subscribers.append(queue)

        try:
            while True:
                if shutdown is not None:
                    # 用 timeout 轮询，不永久阻塞 — 以便检查 shutdown
                    try:
                        event = await asyncio.wait_for(
                            queue.get(), timeout=1.0
                        )
                    except asyncio.TimeoutError:
                        if shutdown.is_set():
                            break
                        continue
                else:
                    event = await queue.get()
                yield event
        finally:
            if queue in self._subscribers:
                self._subscribers.remove(queue)

    def monotonic_now(self) -> str:
        """返回当前单调时钟戳 (ns since bus start)。
        供外部组件 (引擎、去抖) 获取统一参照系时间。
        """
        return str(time.monotonic_ns() - self._start_monotonic)

    def inject_sensor_event(self, event: StampedEvent) -> None:
        """仅用于传感器故障恢复重放: 传感器重启后回灌已落盘事件。
        这不是非传感器来源——这是传感器事件的延迟到达。

        与 ingest 的区别: 不再次盖戳 (事件已有 bus_timestamp)，
        不再次路由 (已有 PersistAction 历史)。
        仅推入环形缓冲 + 分发给订阅者。
        """
        self._ring.push(event)
        for queue in self._subscribers:
            try:
                queue.put_nowait(event)
            except asyncio.QueueFull:
                pass

    @property
    def ring_snapshot(self) -> list[StampedEvent]:
        """返回环形缓冲当前快照 (调试/测试用)。"""
        return self._ring.snapshot()

    @property
    def subscriber_count(self) -> int:
        """当前订阅者数量 (调试/测试用)。"""
        return len(self._subscribers)
