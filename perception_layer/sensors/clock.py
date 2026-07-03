"""时钟传感器 — Tier 1 始终在线。

产出:
  - clock.tick: 周期性心跳 (tick_interval_sec)
  - clock.idle_too_long: 系统空闲超过阈值

地层 1 (单调时钟戳) 由总线实现，不由此传感器实现。
此传感器产出的事件仍由总线盖戳——传感器时间戳只作参考保留。
"""

import asyncio
import time
import uuid
from datetime import datetime, timezone
from typing import AsyncIterator

from perception_layer.models.event import EventSource, EventPayload, RawEvent, EventType
from perception_layer.sensors.base import SensorBase


class ClockSensor(SensorBase):
    """时钟传感器。

    Tier 1: 始终在线 — 不依赖任务上下文。
    health_check 始终返回 True (不依赖外部资源)。
    """

    def __init__(
        self,
        tick_interval_sec: float = 10.0,
        idle_threshold_sec: float = 300.0,
        sensor_id: str = "clock-01",
        tier: int = 1,
    ) -> None:
        self._tick_interval = tick_interval_sec
        self._idle_threshold = idle_threshold_sec
        self._sensor_id = sensor_id
        self._tier = tier
        self._running = False
        self._last_event_time = time.time()  # 上一个非 clock 事件的时间 (由外部更新)

    @property
    def sensor_id(self) -> str:
        return self._sensor_id

    @property
    def tier(self) -> int:
        return self._tier

    async def watch(self) -> AsyncIterator[RawEvent]:
        """周期性产出 tick 事件 + idle_too_long 检测。"""
        self._running = True

        queue: asyncio.Queue[RawEvent] = asyncio.Queue(maxsize=32)

        async def tick_producer() -> None:
            while self._running:
                await asyncio.sleep(self._tick_interval)
                if self._running:
                    event = self._make_event(EventType.CLOCK_TICK)
                    try:
                        queue.put_nowait(event)
                    except asyncio.QueueFull:
                        pass  # 队列满丢弃 (clock tick 高频低价值，可丢)

        async def idle_producer() -> None:
            while self._running:
                await asyncio.sleep(self._tick_interval)
                if self._running:
                    elapsed = time.time() - self._last_event_time
                    if elapsed > self._idle_threshold:
                        event = self._make_event(EventType.CLOCK_IDLE)
                        try:
                            queue.put_nowait(event)
                        except asyncio.QueueFull:
                            pass

        producers = [
            asyncio.create_task(tick_producer()),
            asyncio.create_task(idle_producer()),
        ]

        try:
            while self._running:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=1.0)
                    yield event
                except asyncio.TimeoutError:
                    continue
        finally:
            self._running = False
            for p in producers:
                p.cancel()
            # 等待生产者任务结束
            await asyncio.gather(*producers, return_exceptions=True)

    async def stop(self) -> None:
        self._running = False

    async def health_check(self) -> bool:
        """Clock 不依赖外部资源，始终返回 True。"""
        return True

    def notify_activity(self) -> None:
        """由 run_sensor 调用 — 当非 clock 事件到达时，重置空闲计时器。"""
        self._last_event_time = time.time()

    def _make_event(self, event_type: EventType) -> RawEvent:
        return RawEvent(
            event_id=uuid.uuid4().hex[:12],
            sensor_id=self._sensor_id,
            event_type=event_type,
            sensor_timestamp=time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()),
            source=EventSource(),
            payload=EventPayload(),
        )
