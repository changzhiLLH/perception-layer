"""传感器基类。

每个传感器实例 = 独立异步任务。
原型阶段: 协程/线程，非独立进程 (弱违反文档"传感器即进程")。
补偿: HealthWatchdog 监测传感器存活 (补强3)，挂了写 sensor.offline。
"""

from abc import ABC, abstractmethod
from typing import AsyncIterator

from perception_layer.models.event import RawEvent


class SensorBase(ABC):
    """传感器基类。

    职责: 监听 → 序列化 → 发射。不判断、不过滤、不关联。

    具体传感器必须实现:
      - sensor_id: 唯一标识
      - tier: 激活层级 (1=始终在线, 2=任务上下文, 3=按需)
      - watch(): 启动监听，持续产出 RawEvent
      - stop(): 停止监听，清理资源
      - health_check(): 返回传感器是否存活 (补强3)
    """

    @property
    @abstractmethod
    def sensor_id(self) -> str:
        """传感器唯一标识，如 'fs-watch-01', 'clock-01'。"""
        ...

    @property
    @abstractmethod
    def tier(self) -> int:
        """激活层级: 1=始终在线, 2=任务上下文激活, 3=按需深度激活。"""
        ...

    @abstractmethod
    async def watch(self) -> AsyncIterator[RawEvent]:
        """启动监听，持续产出 RawEvent。

        零判断原则:
          - 不判断"这个是不是异常"
          - 不判断"这个重不重要"
          - 不合并事件 (去抖是引擎的事)

        Yields:
            RawEvent: 每次 yield 是一个原子事件
        """
        ...

    @abstractmethod
    async def stop(self) -> None:
        """停止传感器，清理资源。"""
        ...

    @abstractmethod
    async def health_check(self) -> bool:
        """检查传感器是否存活。

        Returns:
            True: 传感器正常运行
            False: 传感器已挂 (HealthWatchdog 会将此报告为 sensor.offline)

        补强3: 这是 health-watchdog 的最小实现接口。
        不做完整 health-watchdog (一致性校验、恢复协议不做)。
        """
        ...
