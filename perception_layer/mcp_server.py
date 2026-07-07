"""MCP Server — 将感知层原型封装为 MCP 工具。

协议: stdio JSON-RPC (Claude Code 通过 command + args 启动子进程)。
运行模式: 单进程双轨 — asyncio 后台传感器 + stdio_server 前台查询。

自观测防护:
  sensor_ignore.json 排除 data/ 目录 (原型的 1572 次雪崩防护继续生效)。
  MCP server 启动时打印监听/排除目录到 stderr (不干扰 stdio JSON-RPC)。

Code reuse:
  L1 (sensors), L2 (bus), L3 (engine + rules), watchdog: 100% 复用。
  L5 (PerceptionLog): 复用文件写入 + query_by_handle。
  main.py: 编排逻辑提取到此文件，调用同一套组件。

冻结语义登记:
  所有 MCP 工具描述标注 frozen_semantic: true。
  hint 只报结构特征，不含语义结论 (如"重构"/"相关"/"同类")。
  如需原始事件, 用 query_by_handle 回查 — 信息不灭原则在 MCP 层落地。
"""

import asyncio
import json
import os
import sys
import time
from collections import deque
from pathlib import Path
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

from perception_layer.models.event import (
    EventType,
    StampedEvent,
    MergedEvent,
    PerceptionHint,
    SupersededMarker,
)
from perception_layer.bus.bus import EventBus
from perception_layer.bus.routing import RoutingRules, PersistAction
from perception_layer.sensors.base import SensorBase
from perception_layer.sensors.clock import ClockSensor
from perception_layer.sensors.fs_watch import FsWatchSensor
from perception_layer.sensors.git_sensor import GitSensor
from perception_layer.engine.debounce import DebounceWindow, DebounceConfig
from perception_layer.engine.engine import CorrelationEngine
from perception_layer.engine.rules import (
    Rule3A,
    SamePathBurstRule,
    SameDirCoModifyRule,
    EditClusterRule,
    SensorCooccurRule,
)
from perception_layer.interface.perception_log import PerceptionLog
from perception_layer.watchdog import HealthWatchdog


# ═══════════════════════════════════════════════════════════════════════
# MCP Server 初始化
# ═══════════════════════════════════════════════════════════════════════

server = Server("perception-layer")


# ═══════════════════════════════════════════════════════════════════════
# HintsBuffer — 引擎产出的 in-memory 缓冲 (FIFO + asyncio.Lock)
# ═══════════════════════════════════════════════════════════════════════

class HintsBuffer:
    """线程安全的 hint 内存缓冲。

    - deque 固定 maxlen，满了自动淘汰最老的 (FIFO)
    - asyncio.Lock 保护读写
    - 这是 MCP 工具 get_recent_hints 的数据源
    """

    def __init__(self, maxlen: int = 100) -> None:
        self._buffer: deque[PerceptionHint] = deque(maxlen=maxlen)
        self._lock = asyncio.Lock()

    async def push(self, hint: PerceptionHint) -> None:
        """追加 hint。满了自动淘汰最老的。"""
        async with self._lock:
            self._buffer.append(hint)

    async def latest(self, n: int) -> list[PerceptionHint]:
        """返回最近 n 条 hint (从老到新)。"""
        async with self._lock:
            items = list(self._buffer)
            return items[-n:] if n < len(items) else items

    async def snapshot(self) -> list[PerceptionHint]:
        """返回全部 hint 快照。"""
        async with self._lock:
            return list(self._buffer)

    async def clear(self) -> None:
        """清空缓冲 (测试用)。"""
        async with self._lock:
            self._buffer.clear()

    def __len__(self) -> int:
        return len(self._buffer)


# ═══════════════════════════════════════════════════════════════════════
# 共享状态
# ═══════════════════════════════════════════════════════════════════════

class AppState:
    """感知层运行态 — 所有组件共享此实例。"""

    def __init__(self) -> None:
        self.ready: bool = False
        self.event_bus: EventBus | None = None
        self.hints_buffer: HintsBuffer = HintsBuffer(maxlen=100)
        self.perception_log: PerceptionLog | None = None
        self.engine: CorrelationEngine | None = None
        self.watch_paths: list[str] = []


app_state = AppState()


# ═══════════════════════════════════════════════════════════════════════
# 序列化辅助
# ═══════════════════════════════════════════════════════════════════════

def _event_to_dict(event: StampedEvent | MergedEvent) -> dict[str, Any]:
    """将事件转为 JSON-serializable dict。"""
    result: dict[str, Any] = {
        "event_id": event.event_id,
        "event_type": event.event_type.value
        if hasattr(event.event_type, "value")
        else str(event.event_type),
        "bus_timestamp": event.bus_timestamp,
        "sensor_timestamp": event.sensor_timestamp,
        "source": {
            "path": event.source.path,
            "pid": event.source.pid,
        },
        "payload": {
            "prev_hash": event.payload.prev_hash,
            "new_hash": event.payload.new_hash,
            "delta_bytes": event.payload.delta_bytes,
        },
    }

    if hasattr(event, "sensor_id"):
        result["sensor_id"] = event.sensor_id
    if hasattr(event, "routing_action"):
        result["routing_action"] = event.routing_action

    if isinstance(event, MergedEvent):
        result["merged_from"] = event.merged_from
        result["merge_count"] = event.merge_count
        result["atomicity_violation"] = event.atomicity_violation
        result["atomicity_violation_reason"] = event.atomicity_violation_reason

    return result


def _hint_to_dict(hint: PerceptionHint) -> dict[str, Any]:
    """将 hint 转为 JSON-serializable dict。"""
    return {
        "hint_id": hint.hint_id,
        "hint": hint.hint,
        "handle": hint.handle,
        "rule_id": hint.rule_id,
        "bus_timestamp": hint.bus_timestamp,
        "frozen_semantic": hint.frozen_semantic,
        "type_a_probe": hint.type_a_probe,
        "regime": hint.regime,
    }


# ═══════════════════════════════════════════════════════════════════════
# 工具注册
# ═══════════════════════════════════════════════════════════════════════

@server.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="get_recent_events",
            description=(
                "获取最近 N 个文件事件 (RawEvent → Bus 盖戳 → 去抖后的 StampedEvent/MergedEvent)。\n"
                "返回事件 ID、类型 (file.modify/create/delete)、路径、时间戳、hash 等。\n"
                "frozen_semantic: true — 事件类型由 inode/hash 确定性可算，Regime 1。\n"
                "如需回查原始事件，用 query_by_handle。"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "返回最近多少条事件，默认 50",
                        "default": 50,
                    }
                },
            },
        ),
        Tool(
            name="get_recent_hints",
            description=(
                "获取最近的结构特征 hint (引擎 3a 规则产出)。\n"
                "hint 只报结构事实: 文件数/路径/时间窗 — 不含语义结论 (如 '重构'/'相关')。\n"
                "frozen_semantic: true — 每条 hint 显式登记冻结语义。\n"
                "hint.handle 是关联的原始事件 ID，可通过 query_by_handle 回查验证。"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "limit": {
                        "type": "integer",
                        "description": "返回最近多少条 hint，默认 20",
                        "default": 20,
                    }
                },
            },
        ),
        Tool(
            name="query_by_handle",
            description=(
                "通过事件 ID (handle) 回查原始事件/合并事件 — 信息不灭原则。\n"
                "支持单个 handle (str) 或批量 handles (list[str])。\n"
                "返回: 事件完整记录 或 null (事件未找到/已过期)。\n"
                "frozen_semantic: true — 回查是确定性查找，同 key 必同结果。\n"
                "Agent 不信任 hint 时可用此工具拉原始事件自己做判断。"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "handle": {
                        "type": "string",
                        "description": "单个事件 ID",
                    },
                    "handles": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "批量事件 ID 列表",
                    },
                },
            },
        ),
        Tool(
            name="get_event_count",
            description=(
                "获取时间窗口内的事件数量统计。\n"
                "返回维度: by_type (按事件类型计数), by_directory (按目录计数)。\n"
                "纯结构统计 — 不含 '重要性'/'异常度' 等语义判断。\n"
                "frozen_semantic: true — 计数是确定性可算的。"
            ),
            inputSchema={
                "type": "object",
                "properties": {
                    "window_ms": {
                        "type": "integer",
                        "description": "统计时间窗口 (毫秒)，默认 5000",
                        "default": 5000,
                    }
                },
            },
        ),
    ]


@server.call_tool()
async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
    """MCP 工具调用分发。"""

    if name == "get_recent_events":
        return await _get_recent_events(arguments)

    elif name == "get_recent_hints":
        return await _get_recent_hints(arguments)

    elif name == "query_by_handle":
        return await _query_by_handle(arguments)

    elif name == "get_event_count":
        return await _get_event_count(arguments)

    else:
        return [TextContent(type="text", text=f"Unknown tool: {name}")]


# ═══════════════════════════════════════════════════════════════════════
# 工具实现 (真实数据源)
# ═══════════════════════════════════════════════════════════════════════

async def _get_recent_events(arguments: dict[str, Any]) -> list[TextContent]:
    """从 RingBuffer 取最近 N 条事件。"""
    limit: int = arguments.get("limit", 50)

    events: list[dict[str, Any]] = []
    if app_state.event_bus is not None:
        raw = app_state.event_bus.ring_snapshot
        recent = raw[-limit:] if len(raw) > limit else raw
        events = [_event_to_dict(e) for e in recent]

    return [TextContent(
        type="text",
        text=json.dumps(
            {"events": events, "count": len(events), "limit": limit},
            ensure_ascii=False, indent=2,
        ),
    )]


async def _get_recent_hints(arguments: dict[str, Any]) -> list[TextContent]:
    """从 HintsBuffer 取最近 N 条 hint。"""
    limit: int = arguments.get("limit", 20)

    raw = await app_state.hints_buffer.latest(limit)
    hints = [_hint_to_dict(h) for h in raw]

    return [TextContent(
        type="text",
        text=json.dumps(
            {"hints": hints, "count": len(hints), "limit": limit},
            ensure_ascii=False, indent=2,
        ),
    )]


async def _query_by_handle(arguments: dict[str, Any]) -> list[TextContent]:
    """通过 handle 回查原始事件 (信息不灭)。

    先从环形缓冲查 (内存快)，未命中再从 event_log.jsonl 查 (文件扫描)。
    """
    single: str | None = arguments.get("handle")
    batch: list[str] | None = arguments.get("handles")

    if not single and not batch:
        return [TextContent(
            type="text",
            text=json.dumps(
                {"error": "需要 handle 或 handles 参数"},
                ensure_ascii=False,
            ),
        )]

    handles: list[str] = [single] if single else (batch or [])
    results: dict[str, Any] = {h: None for h in handles}

    # 1. 先从环形缓冲查 (内存，快)
    if app_state.event_bus is not None:
        ring = app_state.event_bus.ring_snapshot
        ring_by_id: dict[str, StampedEvent | MergedEvent] = {}
        for e in ring:
            ring_by_id[e.event_id] = e
        # MergedEvent 的 merged_from 也建立索引
        for e in ring:
            if isinstance(e, MergedEvent):
                for mf in e.merged_from:
                    if mf not in ring_by_id:
                        ring_by_id[mf] = e

        for h in handles:
            if h in ring_by_id:
                results[h] = _event_to_dict(ring_by_id[h])

    # 2. 环形缓冲未命中 → 查 event_log.jsonl (文件扫描)
    still_missing = [h for h, v in results.items() if v is None]
    if still_missing and app_state.perception_log is not None:
        file_results = await app_state.perception_log.query_by_handle_batch(
            still_missing
        )
        for h, record in file_results.items():
            if record is not None:
                results[h] = record

    return [TextContent(
        type="text",
        text=json.dumps({"results": results}, ensure_ascii=False, indent=2),
    )]


async def _get_event_count(arguments: dict[str, Any]) -> list[TextContent]:
    """从环形缓冲统计 by_type + by_directory。"""
    window_ms: int = arguments.get("window_ms", 5000)

    total = 0
    by_type: dict[str, int] = {}
    by_directory: dict[str, int] = {}

    if app_state.event_bus is not None:
        raw = app_state.event_bus.ring_snapshot
        window_ns = window_ms * 1_000_000

        if raw:
            latest_ns = int(raw[-1].bus_timestamp)
            cutoff = latest_ns - window_ns

            for event in raw:
                ts = int(event.bus_timestamp)
                if ts < cutoff:
                    continue

                total += 1

                # by_type
                etype = (
                    event.event_type.value
                    if hasattr(event.event_type, "value")
                    else str(event.event_type)
                )
                by_type[etype] = by_type.get(etype, 0) + 1

                # by_directory
                if event.source.path:
                    dirname = os.path.dirname(
                        event.source.path.replace("\\", "/")
                    )
                    by_directory[dirname] = by_directory.get(dirname, 0) + 1

    return [TextContent(
        type="text",
        text=json.dumps(
            {
                "window_ms": window_ms,
                "total": total,
                "by_type": by_type,
                "by_directory": by_directory,
            },
            ensure_ascii=False, indent=2,
        ),
    )]


# ═══════════════════════════════════════════════════════════════════════
# 后台任务 (传感器 + 引擎 + 看门狗)
# ═══════════════════════════════════════════════════════════════════════

_BG_SHUTDOWN_CHECK_INTERVAL = 1.0  # 后台任务检查 shutdown 标志的间隔 (秒)


async def _run_sensor_task(
    sensor: SensorBase,
    clock: ClockSensor,
    shutdown: asyncio.Event,
) -> None:
    """传感器 → 总线 → event_log 的数据流。

    与原 main.py 的 run_sensor 相同逻辑:
      - 传感器 watch() → bus.ingest() → 机械执行路由规则
      - 时钟活动通知 (重置 idle 计时器)
      - shutdown 检查
    """
    try:
        async for raw_event in sensor.watch():
            if shutdown.is_set():
                break

            if app_state.event_bus is None:
                continue

            # 传感器 → 总线 (入口唯一: ingest)
            stamped, action = await app_state.event_bus.ingest(raw_event)

            # 时钟活动通知 (重置 idle 计时器)
            if not isinstance(sensor, ClockSensor):
                clock.notify_activity()

            # 机械执行路由规则
            if action == PersistAction.PERSIST and app_state.perception_log:
                await app_state.perception_log.write_event(stamped)
            # else: RING_ONLY → 事件仅留环形缓冲，不落盘

    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(
            f"[perception-layer MCP] 传感器 {sensor.sensor_id} 异常: {e}",
            file=sys.stderr,
        )
    finally:
        await sensor.stop()


async def _run_engine_task(shutdown: asyncio.Event) -> None:
    """引擎主循环 → hints_buffer + perception_log + superseded markers。

    使用 asyncio.wait_for 包装 engine.run() 的每次迭代，
    以便周期性检查 shutdown 标志 (否则 bus.stream() 会永久阻塞)。
    """
    if app_state.engine is None:
        return

    engine_iter = app_state.engine.run().__aiter__()
    try:
        while not shutdown.is_set():
            try:
                hint = await asyncio.wait_for(
                    engine_iter.__anext__(),
                    timeout=_BG_SHUTDOWN_CHECK_INTERVAL,
                )
            except asyncio.TimeoutError:
                # 超时 → 检查 shutdown 标志，继续循环
                continue

            # 引擎产出 → hints_buffer (内存)
            await app_state.hints_buffer.push(hint)

            # 引擎产出 → perception_log.jsonl (文件)
            if app_state.perception_log:
                await app_state.perception_log.write_hint(hint)

            # 去抖 superseded marker → event_log.jsonl
            if app_state.engine and app_state.perception_log:
                for marker in app_state.engine.drain_superseded():
                    await app_state.perception_log.write_superseded_marker(
                        marker
                    )

    except StopAsyncIteration:
        pass
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"[perception-layer MCP] 引擎异常: {e}", file=sys.stderr)

    # 停止时清空去抖窗口残余
    # 引擎已停止，残余事件直接丢弃 (shutdown 时不做关联分析)。
    # 后续可加公开的 drain_remaining() 接口来安全处理，
    # 原型阶段丢弃 + 打印到 stderr 就够了。
    if app_state.engine:
        try:
            residual = app_state.engine._debounce.flush()
            if residual:
                count = len(residual)
                event_ids = [
                    e.event_id[:8]
                    for e in residual
                    if hasattr(e, "event_id")
                ]
                print(
                    f"[perception-layer MCP] shutdown: 丢弃 {count} 个"
                    f" 残余去抖事件 ({', '.join(event_ids[:5])}...)'",
                    file=sys.stderr,
                )
        except Exception:
            pass


async def _run_watchdog_task(shutdown: asyncio.Event) -> None:
    """看门狗 — 监测传感器健康。shutdown 时退出。"""
    if not hasattr(app_state, "_watchdog") or app_state._watchdog is None:
        return

    watchdog: HealthWatchdog = app_state._watchdog
    try:
        while not shutdown.is_set():
            try:
                await asyncio.wait_for(
                    watchdog.monitor().__anext__(),
                    timeout=_BG_SHUTDOWN_CHECK_INTERVAL,
                )
            except asyncio.TimeoutError:
                continue
            except StopAsyncIteration:
                break
    except asyncio.CancelledError:
        pass
    except Exception as e:
        print(f"[perception-layer MCP] 看门狗异常: {e}", file=sys.stderr)


# ═══════════════════════════════════════════════════════════════════════
# 组件初始化
# ═══════════════════════════════════════════════════════════════════════

def _init_components(
    base: Path,
    watch_paths: list[str],
    config_dir: Path,
    data_dir: Path,
) -> tuple[
    EventBus,
    PerceptionLog,
    list[SensorBase],
    ClockSensor,
    CorrelationEngine,
    HealthWatchdog,
]:
    """初始化所有感知层组件 (复用原 main.py 的编排逻辑)。

    Returns:
        (bus, log, sensors, clock, engine, watchdog)
    """

    # === 1. 加载路由规则 ===
    routing_file = config_dir / "routing_rules.json"
    if routing_file.exists():
        routing_rules = RoutingRules.from_file(str(routing_file))
        print(
            f"[perception-layer MCP] 已加载路由规则: {len(routing_rules.rules)} 条",
            file=sys.stderr,
        )
    else:
        from perception_layer.bus.routing import RoutingRule

        routing_rules = RoutingRules([
            RoutingRule(
                prefix="/", action=PersistAction.PERSIST, reason="默认落盘"
            ),
        ])
        print(
            "[perception-layer MCP] 使用默认路由规则 (全部落盘)",
            file=sys.stderr,
        )

    # === 2. 初始化感知日志 ===
    log = PerceptionLog(
        perception_log_path=str(data_dir / "perception_log.jsonl"),
        event_log_path=str(data_dir / "event_log.jsonl"),
    )

    # === 3. 初始化总线 ===
    bus = EventBus(routing_rules=routing_rules, ring_buffer_size=2048)

    # === 4. 初始化传感器 ===
    sensors: list[SensorBase] = []

    clock = ClockSensor(tick_interval_sec=10.0, idle_threshold_sec=300.0)
    sensors.append(clock)

    ignore_config = str(config_dir / "sensor_ignore.json")
    fs_watch = FsWatchSensor(
        watch_paths=watch_paths, ignore_config_path=ignore_config
    )
    sensors.append(fs_watch)

    # Git 传感器 (Tier 2: 任务上下文激活)
    git_sensor = GitSensor(
        watch_path=watch_paths[0],
        poll_interval_sec=3.0,
    )
    sensors.append(git_sensor)

    # === 5. 初始化引擎 ===
    debounce = DebounceWindow(DebounceConfig(
        window_ms=200, max_wait_ms=2000, tick_interval_ms=50,
    ))

    rules: list[Rule3A] = [
        SamePathBurstRule(window_ms=200, min_events=3),
        SameDirCoModifyRule(window_ms=200, min_files=3),
        EditClusterRule(gap_threshold_ms=500, min_cluster_size=5),
        SensorCooccurRule(window_ms=4000, min_events=3),
    ]
    print(
        f"[perception-layer MCP] 已加载 {len(rules)} 条 3a 规则",
        file=sys.stderr,
    )
    for r in rules:
        print(f"  - {r.rule_id}: {r.description}", file=sys.stderr)

    engine = CorrelationEngine(
        bus=bus, debounce=debounce, rules=rules, context_window_size=500,
    )

    # === 6. 初始化看门狗 ===
    watchdog = HealthWatchdog(
        sensors=sensors,
        perception_log=log,
        check_interval_sec=5.0,
        max_failures=2,
    )

    return bus, log, sensors, clock, engine, watchdog


# ═══════════════════════════════════════════════════════════════════════
# MCP Server 启动入口
# ═══════════════════════════════════════════════════════════════════════

async def main(
    watch_paths: list[str] | None = None,
    project_root: str | None = None,
) -> None:
    """MCP server 主入口。

    启动顺序:
      1. 解析路径 + 初始化所有组件
      2. bus.restart 事件写感知日志
      3. 启动后台任务 (传感器 + 引擎 + 看门狗)
      4. 运行 stdio_server (阻塞，直到 stdin 关闭)
      5. 发送 shutdown → 等待后台任务退出 → 清理

    Args:
        watch_paths: 监听的目录列表 (默认: 项目根目录)
        project_root: 项目根目录 (默认: 当前目录)
    """
    # 解析路径
    base = Path(project_root) if project_root else Path.cwd()
    config_dir = base / "config"
    data_dir = base / "data"
    data_dir.mkdir(parents=True, exist_ok=True)

    if watch_paths is None:
        watch_paths = [str(base)]

    # 加载 sensor_ignore.json 确认排除路径
    ignore_config = str(config_dir / "sensor_ignore.json")
    ignore_paths: set[str] = set()
    if Path(ignore_config).exists():
        try:
            with open(ignore_config, "r", encoding="utf-8") as f:
                ignore_data = json.load(f)
            raw = ignore_data.get("ignore_paths", [])
            ignore_paths = {str((base / p).resolve()) for p in raw}
        except (json.JSONDecodeError, KeyError, OSError):
            pass

    # 打印启动信息到 stderr
    print(
        f"[perception-layer MCP] 启动 — v0.2.0 (真实数据源)",
        file=sys.stderr,
    )
    print(
        f"[perception-layer MCP] 监听目录: {watch_paths}",
        file=sys.stderr,
    )
    print(
        f"[perception-layer MCP] 排除目录: "
        f"{ignore_paths if ignore_paths else '(无)'}",
        file=sys.stderr,
    )
    print(
        f"[perception-layer MCP] 数据目录: {data_dir}",
        file=sys.stderr,
    )
    print(
        f"[perception-layer MCP] 自观测防护: sensor_ignore.json → {ignore_config}",
        file=sys.stderr,
    )

    # 初始化所有组件
    bus, log, sensors, clock, engine, watchdog = _init_components(
        base=base,
        watch_paths=watch_paths,
        config_dir=config_dir,
        data_dir=data_dir,
    )

    # bus.restart 写入感知日志 (不经总线)
    await log.write_system_event(
        event_type=EventType.BUS_RESTART.value,
        reason="感知层 MCP server 启动",
        timestamp=bus.monotonic_now(),
    )

    # 写入共享状态
    app_state.event_bus = bus
    app_state.perception_log = log
    app_state.engine = engine
    app_state.watch_paths = watch_paths
    app_state._watchdog = watchdog  # type: ignore[attr-defined]
    app_state._sensors = sensors  # type: ignore[attr-defined]
    app_state._clock = clock  # type: ignore[attr-defined]

    print(
        f"[perception-layer MCP] 已初始化 {len(sensors)} 个传感器",
        file=sys.stderr,
    )
    for s in sensors:
        print(f"  - {s.sensor_id} (Tier {s.tier})", file=sys.stderr)

    # 启动后台任务
    shutdown = asyncio.Event()
    background_tasks: list[asyncio.Task] = []

    # 传感器任务
    for sensor in sensors:
        task = asyncio.create_task(
            _run_sensor_task(sensor, clock, shutdown)
        )
        background_tasks.append(task)

    # 引擎任务
    engine_task = asyncio.create_task(_run_engine_task(shutdown))
    background_tasks.append(engine_task)

    # 看门狗 — watchdog.monitor() 是无限循环，shutdown 时靠 task.cancel() 终止。
    # 后续可给 monitor() 加 shutdown 参数支持优雅退出，原型阶段 cancel 足够。
    async def _watchdog_wrapper() -> None:
        try:
            await watchdog.monitor()
        except asyncio.CancelledError:
            pass

    watchdog_task = asyncio.create_task(_watchdog_wrapper())
    background_tasks.append(watchdog_task)

    app_state.ready = True
    print(
        f"[perception-layer MCP] 已启动 {len(background_tasks)} 个后台任务",
        file=sys.stderr,
    )

    # 运行 MCP stdio server (阻塞直到 stdin 关闭)
    try:
        async with stdio_server() as (read_stream, write_stream):
            print(
                "[perception-layer MCP] stdio server 已启动，"
                "等待 MCP 客户端连接...",
                file=sys.stderr,
            )
            await server.run(
                read_stream,
                write_stream,
                server.create_initialization_options(),
            )
            print(
                "[perception-layer MCP] stdio server 已关闭",
                file=sys.stderr,
            )
    finally:
        # 发送 shutdown 信号
        shutdown.set()

        # 等待传感器任务退出 (它们检查 shutdown 后 break)
        print("[perception-layer MCP] 正在停止后台任务...", file=sys.stderr)
        sensor_tasks = [t for t in background_tasks[:len(sensors)]]
        for t in sensor_tasks:
            if not t.done():
                t.cancel()

        # 等待所有后台任务
        results = await asyncio.gather(
            *background_tasks, return_exceptions=True
        )
        for r in results:
            if isinstance(r, Exception) and not isinstance(
                r, asyncio.CancelledError
            ):
                print(
                    f"[perception-layer MCP] 后台任务异常: {r}",
                    file=sys.stderr,
                )

        # bus.stop 写入感知日志
        await log.write_system_event(
            event_type=EventType.BUS_RESTART.value,
            reason="感知层 MCP server 停止",
            timestamp=bus.monotonic_now(),
        )

        app_state.ready = False
        print("[perception-layer MCP] 已关闭。", file=sys.stderr)


def run(
    watch_paths: list[str] | None = None,
    project_root: str | None = None,
) -> None:
    """同步入口。watch_paths 默认为当前目录。"""
    try:
        asyncio.run(main(watch_paths=watch_paths, project_root=project_root))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    paths = sys.argv[1:] if len(sys.argv) > 1 else None
    run(paths)
