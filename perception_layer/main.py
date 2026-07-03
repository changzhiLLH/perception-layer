"""感知层入口 — 编排所有组件。

物理隔离 (坑6 + 修正1 + 修正2):
  总线输出 → event_log.jsonl       (StampedEvent + MergedEvent + SupersededMarker)
  引擎输出 → perception_log.jsonl   (PerceptionHint)
  系统标记 → perception_log.jsonl   (sensor.offline / bus.restart — 不经总线)

路由规则执行: main.py 机械执行 if-else，不做判断。
  if action == PERSIST: await log.write_event(stamped)
  # 这是机械执行路由规则的结果，不是 main.py 在做判断。
  # 判断在路由规则配置里 (补强2: Agent 可读写配置覆盖)。
"""

import asyncio
import signal
import sys
import time
from pathlib import Path

from perception_layer.models.event import EventType, SupersededMarker
from perception_layer.bus.bus import EventBus
from perception_layer.bus.routing import RoutingRules, PersistAction
from perception_layer.sensors.base import SensorBase
from perception_layer.sensors.clock import ClockSensor
from perception_layer.sensors.fs_watch import FsWatchSensor
from perception_layer.engine.debounce import DebounceWindow, DebounceConfig
from perception_layer.engine.engine import CorrelationEngine
from perception_layer.engine.rules import (
    Rule3A,
    SamePathBurstRule,
    SameDirCoModifyRule,
    EditClusterRule,
)
from perception_layer.interface.perception_log import PerceptionLog
from perception_layer.watchdog import HealthWatchdog


async def main(
    watch_paths: list[str] | None = None,
    config_dir: str = "config",
    data_dir: str = "data",
) -> None:
    """感知层主入口。

    编排顺序:
      1. 加载路由规则配置
      2. 初始化感知日志 (两个物理隔离的文件)
      3. 初始化总线
      4. 初始化传感器
      5. 初始化去抖 + 3a 规则 + 关联引擎
      6. 初始化健康看门狗
      7. 启动所有协程
      8. 主循环: 引擎产出 → perception_log, 总线事件 → event_log

    Args:
        watch_paths: 监听的目录列表 (默认: 当前目录)
        config_dir: 配置目录路径
        data_dir: 数据输出目录
    """
    # 解析路径
    base = Path.cwd()
    config_path = base / config_dir
    data_path = base / data_dir
    data_path.mkdir(parents=True, exist_ok=True)

    if watch_paths is None:
        watch_paths = [str(base)]

    # === 1. 加载路由规则 ===
    routing_file = config_path / "routing_rules.json"
    if routing_file.exists():
        routing_rules = RoutingRules.from_file(str(routing_file))
        print(f"[感知层] 已加载路由规则: {len(routing_rules.rules)} 条")
    else:
        # 默认规则
        from perception_layer.bus.routing import RoutingRule
        routing_rules = RoutingRules([
            RoutingRule(prefix="/", action=PersistAction.PERSIST, reason="默认落盘"),
        ])
        print(f"[感知层] 使用默认路由规则 (全部落盘)")

    # === 2. 初始化感知日志 (两个物理隔离的文件) ===
    log = PerceptionLog(
        perception_log_path=str(data_path / "perception_log.jsonl"),
        event_log_path=str(data_path / "event_log.jsonl"),
    )
    print(f"[感知层] 感知日志: {log._perception_path}")
    print(f"[感知层] 事件日志: {log._event_path}")

    # === 3. 初始化总线 ===
    bus = EventBus(routing_rules=routing_rules, ring_buffer_size=2048)

    # bus.restart 写入感知日志 (不经总线 — 修正1)
    await log.write_system_event(
        event_type=EventType.BUS_RESTART.value,
        reason="感知层启动",
        timestamp=bus.monotonic_now(),
    )

    # === 4. 初始化传感器 ===
    sensors: list[SensorBase] = []

    # Clock 传感器 (Tier 1: 始终在线)
    clock = ClockSensor(
        tick_interval_sec=10.0,
        idle_threshold_sec=300.0,
    )
    sensors.append(clock)

    # FsWatch 传感器 (Tier 2: 任务上下文激活)
    # 忽略路径从 config/sensor_ignore.json 加载 — Regime 3 自动+审计 (可绕过)。
    # 原因: 自观测正反馈 — 写 event_log.jsonl 触发 fs-watch 事件形成雪崩。
    #   原型发现: 不加此排除，单次文件修改在 200ms 窗口内产生 1572 次自观测事件。
    ignore_config = str(config_path / "sensor_ignore.json")
    fs_watch = FsWatchSensor(
        watch_paths=watch_paths, ignore_config_path=ignore_config
    )
    sensors.append(fs_watch)

    print(f"[感知层] 已初始化 {len(sensors)} 个传感器")
    for s in sensors:
        print(f"  - {s.sensor_id} (Tier {s.tier})")

    # === 5. 初始化去抖 + 3a 规则 + 关联引擎 ===
    debounce = DebounceWindow(DebounceConfig(
        window_ms=200,
        max_wait_ms=2000,
        tick_interval_ms=50,
    ))

    rules: list[Rule3A] = [
        SamePathBurstRule(window_ms=200, min_events=3),
        SameDirCoModifyRule(window_ms=200, min_files=3),
        EditClusterRule(gap_threshold_ms=500, min_cluster_size=5),
    ]
    print(f"[感知层] 已加载 {len(rules)} 条 3a 规则")
    for r in rules:
        print(f"  - {r.rule_id}: {r.description}")

    engine = CorrelationEngine(
        bus=bus,
        debounce=debounce,
        rules=rules,
        context_window_size=500,
    )

    # === 6. 初始化健康看门狗 ===
    watchdog = HealthWatchdog(
        sensors=sensors,
        perception_log=log,
        check_interval_sec=5.0,
        max_failures=2,
    )

    # === 7. 启动所有协程 ===
    shutdown_event = asyncio.Event()

    # 传感器任务
    sensor_tasks: list[asyncio.Task] = []

    async def run_sensor(sensor: SensorBase) -> None:
        """传感器 → 总线 → event_log 的数据流。

        机械执行路由规则: if action == PERSIST → 写 event_log。
        不做判断 — 判断在路由规则配置里。
        """
        try:
            async for raw_event in sensor.watch():
                if shutdown_event.is_set():
                    break

                # 传感器 → 总线 (入口唯一: ingest)
                stamped, action = await bus.ingest(raw_event)

                # 时钟活动通知 (重置 idle 计时器)
                if isinstance(sensor, ClockSensor):
                    pass  # clock 自己的 tick 不重置自己的 idle
                elif hasattr(clock, 'notify_activity'):
                    clock.notify_activity()

                # 机械执行路由规则 — 这是 if-else，不是判断
                # 判断在 routing_rules.json 里 (Agent 可读写覆盖 — 补强2)
                if action == PersistAction.PERSIST:
                    await log.write_event(stamped)
                # else: RING_ONLY → 事件仅留环形缓冲，不落盘

        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"[感知层] 传感器 {sensor.sensor_id} 异常: {e}", file=sys.stderr)
        finally:
            await sensor.stop()

    for sensor in sensors:
        task = asyncio.create_task(run_sensor(sensor))
        sensor_tasks.append(task)

    # 看门狗任务
    watchdog_task = asyncio.create_task(watchdog.monitor())

    # 引擎 → 感知日志任务
    async def run_engine_to_log() -> None:
        """引擎主循环 → perception_log + event_log (superseded)。

        物理隔离:
          - PerceptionHint → perception_log.jsonl (感知通道)
          - SupersededMarker → event_log.jsonl (事件通道，标记去抖合并)
        """
        try:
            async for hint in engine.run():
                if shutdown_event.is_set():
                    break

                # 引擎产出 → perception_log (感知通道，独立于总线)
                await log.write_hint(hint)

                # 去抖 superseded marker → event_log (事件通道)
                for marker in engine.drain_superseded():
                    await log.write_superseded_marker(marker)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            print(f"[感知层] 引擎异常: {e}", file=sys.stderr)

    engine_task = asyncio.create_task(run_engine_to_log())

    print(f"[感知层] 已启动 {len(sensor_tasks)} 个传感器任务 + 引擎 + 看门狗")
    print(f"[感知层] 监听目录: {watch_paths}")
    print(f"[感知层] 按 Ctrl+C 停止")

    # === 8. 等待关闭信号 ===
    loop = asyncio.get_event_loop()

    def signal_handler() -> None:
        print("\n[感知层] 收到停止信号，正在关闭...")
        shutdown_event.set()

    try:
        loop.add_signal_handler(signal.SIGINT, signal_handler)
        loop.add_signal_handler(signal.SIGTERM, signal_handler)
    except NotImplementedError:
        # Windows 不支持 add_signal_handler，用 KeyboardInterrupt
        pass

    try:
        # 等待关闭信号
        await shutdown_event.wait()
    except KeyboardInterrupt:
        print("\n[感知层] 收到 KeyboardInterrupt，正在关闭...")
        shutdown_event.set()

    # === 关闭流程 ===
    print("[感知层] 正在停止传感器...")
    for task in sensor_tasks:
        task.cancel()
    await asyncio.gather(*sensor_tasks, return_exceptions=True)

    print("[感知层] 正在停止看门狗...")
    watchdog_task.cancel()
    try:
        await watchdog_task
    except asyncio.CancelledError:
        pass

    print("[感知层] 正在停止引擎...")
    engine_task.cancel()
    try:
        await engine_task
    except asyncio.CancelledError:
        pass

    # bus.stop 写入感知日志 (不经总线 — 修正1)
    await log.write_system_event(
        event_type=EventType.BUS_RESTART.value,
        reason="感知层停止",
        timestamp=bus.monotonic_now(),
    )

    print("[感知层] 已关闭。")


def run(watch_paths: list[str] | None = None) -> None:
    """同步入口。watch_paths 默认为当前目录。"""
    try:
        asyncio.run(main(watch_paths=watch_paths))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    # 从命令行参数获取监听路径
    paths = sys.argv[1:] if len(sys.argv) > 1 else None
    run(paths)
