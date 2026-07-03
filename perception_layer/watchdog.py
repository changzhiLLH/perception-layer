"""健康看门狗 — 补强3 最小实现。

职责: 监测传感器线程/协程存活。挂了 → 写 sensor.offline 到感知日志。
不做: 完整 health-watchdog (一致性校验、恢复协议不做)。

关键 (修正1): sensor.offline 走 PerceptionLog.write_system_event → perception_log.jsonl。
不经 EventBus。总线不接受非传感器来源事件。
"""

import asyncio
from typing import Optional

from perception_layer.sensors.base import SensorBase
from perception_layer.interface.perception_log import PerceptionLog


class HealthWatchdog:
    """最小 health-watchdog。

    补强3: 每个传感器线程挂了，要能被感知到。
    从沉默失败变成可感知失败。

    监测周期: check_interval_sec
    连续失败阈值: 连续 max_failures 次 health_check() 返回 False 后才报告
    (防止瞬态误报)
    """

    def __init__(
        self,
        sensors: list[SensorBase],
        perception_log: PerceptionLog,
        check_interval_sec: float = 5.0,
        max_failures: int = 2,
    ) -> None:
        self._sensors = sensors
        self._log = perception_log
        self._check_interval = check_interval_sec
        self._max_failures = max_failures

        # 跟踪连续失败次数
        self._failure_counts: dict[str, int] = {}
        # 跟踪已报告的传感器 (避免重复报告)
        self._reported_offline: set[str] = set()

    async def monitor(self) -> None:
        """主循环。定期检查各传感器 health_check()。

        返回 False → 递增失败计数
        连续失败 >= max_failures → 写 sensor.offline 到 perception_log
        恢复 → 重置失败计数
        """
        while True:
            await asyncio.sleep(self._check_interval)

            for sensor in self._sensors:
                try:
                    alive = await sensor.health_check()
                except Exception:
                    alive = False

                if alive:
                    # 传感器恢复
                    if sensor.sensor_id in self._failure_counts:
                        del self._failure_counts[sensor.sensor_id]
                    if sensor.sensor_id in self._reported_offline:
                        self._reported_offline.discard(sensor.sensor_id)
                    continue

                # 传感器不健康
                self._failure_counts[sensor.sensor_id] = (
                    self._failure_counts.get(sensor.sensor_id, 0) + 1
                )

                if (
                    self._failure_counts[sensor.sensor_id] >= self._max_failures
                    and sensor.sensor_id not in self._reported_offline
                ):
                    self._reported_offline.add(sensor.sensor_id)
                    await self._report_offline(sensor)

    async def _report_offline(self, sensor: SensorBase) -> None:
        """报告传感器离线。

        直接写 perception_log.write_system_event — 不经总线 (修正1)。
        """
        import time

        await self._log.write_system_event(
            event_type="sensor.offline",
            reason=(
                f"sensor {sensor.sensor_id} health check failed "
                f"{self._max_failures} consecutive times"
            ),
            timestamp=str(time.monotonic_ns()),
        )

    def check_once(self) -> list[str]:
        """同步单次检查 (用于测试)。返回不健康的传感器 id 列表。"""
        unhealthy: list[str] = []
        for sensor in self._sensors:
            try:
                # 在同步上下文中运行异步 health_check
                import asyncio as _asyncio
                alive = _asyncio.run(sensor.health_check())
            except Exception:
                alive = False
            if not alive:
                unhealthy.append(sensor.sensor_id)
        return unhealthy
