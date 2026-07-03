"""文件系统传感器 — Tier 2 任务上下文激活。

底层: watchdog → ReadDirectoryChangesW (Windows)
监听: 创建/修改/删除/重命名
不做: 权限变更 (原型不做)、不去抖 (引擎的事)、不判断 (零判断原则)

Windows 已知问题:
  - ReadDirectoryChangesW 缓冲区溢出会丢事件 → 写 sensor.gap 到日志
  - JetBrains 等编辑器非原子写入 → 触发多次 modify 事件 → 去抖由引擎处理
  - 重命名事件在 Windows 上可能触发 moved 而非 renamed → 都映射为 file.delete + file.create

放弃 pid (地基 2):
  - ReadDirectoryChangesW 拿不到 pid
  - 需要 ETW trace session 才能拿到 pid → 原型不做
  - 所有事件的 source.pid = None
  - 代价: 无法做文件↔进程硬关联，因果推理依赖时间窗 (地基 4) 兜底
"""

import asyncio
import hashlib
import json
import uuid
from pathlib import Path
from typing import AsyncIterator

from watchdog.observers import Observer
from watchdog.events import (
    FileSystemEventHandler,
    FileCreatedEvent,
    FileModifiedEvent,
    FileDeletedEvent,
    FileMovedEvent,
    DirCreatedEvent,
    DirModifiedEvent,
    DirDeletedEvent,
)

from perception_layer.models.event import EventSource, EventPayload, RawEvent, EventType
from perception_layer.sensors.base import SensorBase


def _compute_hash(file_path: str) -> str | None:
    """计算文件 hash (SHA-256 前 12 字符)。
    返回 None 如果文件不存在或不可读。
    """
    try:
        with open(file_path, "rb") as f:
            return hashlib.sha256(f.read()).hexdigest()[:12]
    except (FileNotFoundError, PermissionError, OSError):
        return None


class _WatchdogHandler(FileSystemEventHandler):
    """watchdog 事件 → RawEvent 适配器。

    零判断: 不判断事件重要性、不合并事件、不判断异常。
    一个 watchdog 事件 → 一个 RawEvent (事件原子性)。
    ignore_paths: 绝对路径前缀集合，匹配的路径不产生事件 (自观测防护)。
    """

    def __init__(
        self,
        sensor_id: str,
        queue: asyncio.Queue[RawEvent],
        ignore_paths: set[str] | None = None,
    ) -> None:
        super().__init__()
        self._sensor_id = sensor_id
        self._queue = queue
        self._ignore_paths = ignore_paths or set()

    def _is_ignored(self, path: str) -> bool:
        """检查路径是否在忽略列表中 (前缀匹配)。
        双方向规范化: watchdog 事件的路径使用正斜杠，ignore_paths 来自 Path.resolve() 使用反斜杠。
        统一规范化后再匹配。
        """
        # 规范化: 转为绝对路径 + 正斜杠
        abs_path = str(Path(path).resolve()).replace("\\", "/")
        for ignored in self._ignore_paths:
            normalized_ignored = ignored.replace("\\", "/")
            if abs_path.startswith(normalized_ignored):
                return True
        return False

    def _make_event(
        self, event_type: EventType, path: str, prev_hash: str | None = None
    ) -> RawEvent:
        new_hash = _compute_hash(path) if event_type != EventType.FILE_DELETE else None
        prev_hash_for_payload = prev_hash

        return RawEvent(
            event_id=uuid.uuid4().hex[:12],
            sensor_id=self._sensor_id,
            event_type=event_type,
            sensor_timestamp="",  # 由事件循环填充
            source=EventSource(
                path=path.replace("\\", "/"),
                pid=None,  # 原型阶段始终 None — 放弃 pid
            ),
            payload=EventPayload(
                prev_hash=prev_hash_for_payload,
                new_hash=new_hash,
                delta_bytes=None,
            ),
        )

    def on_modified(self, event: FileModifiedEvent) -> None:
        """文件修改 → file.modify。零判断: 不区分"真的改了"vs"时间戳更新"。"""
        path = event.src_path
        if self._is_ignored(path):
            return
        if Path(path).is_file():
            evt = self._make_event(EventType.FILE_MODIFY, path)
            self._queue.put_nowait(evt)

    def on_created(self, event: FileCreatedEvent | DirCreatedEvent) -> None:
        """文件/目录创建 → file.create。"""
        path = event.src_path
        if self._is_ignored(path):
            return
        if Path(path).is_dir():
            return  # 原型忽略目录事件
        evt = self._make_event(EventType.FILE_CREATE, path)
        self._queue.put_nowait(evt)

    def on_deleted(self, event: FileDeletedEvent | DirDeletedEvent) -> None:
        """文件删除 → file.delete。"""
        path = event.src_path
        if self._is_ignored(path):
            return
        if Path(path).is_dir():
            return
        evt = self._make_event(EventType.FILE_DELETE, path)
        self._queue.put_nowait(evt)

    def on_moved(self, event: FileMovedEvent) -> None:
        """文件移动 → file.delete (源) + file.create (目标)。
        拆成两个原子事件 — 事件原子性原则。
        """
        src = event.src_path
        dest = event.dest_path

        if not Path(src).is_dir() and not self._is_ignored(src):
            evt_del = self._make_event(EventType.FILE_DELETE, src)
            self._queue.put_nowait(evt_del)

        if not Path(dest).is_dir() and not self._is_ignored(dest):
            evt_create = self._make_event(EventType.FILE_CREATE, dest)
            self._queue.put_nowait(evt_create)


class FsWatchSensor(SensorBase):
    """文件系统传感器。

    Tier 2: 任务上下文激活 (Agent 开始操作文件时激活)。
    底层: watchdog → ReadDirectoryChangesW (Windows)。

    ignore_paths 从 config/sensor_ignore.json 加载 (Regime 3 自动+审计):
      冻结语义: 排除哪些路径不监听。
      Type A 探针: 无 — "排除 data/ 是否正确"依赖"数据目录是什么"的语义前提。
      处置: Regime 3 自动+审计。
        前提满足: (a) 信息不灭 — 配置文件可查
                  (b) 窗口内损害可逆 — Agent 修改配置即可恢复
        审计方式: Agent 可读写此配置文件；排除决策从此文件可追溯。
    """

    def __init__(
        self,
        watch_paths: list[str],
        sensor_id: str = "fs-watch-01",
        tier: int = 2,
        ignore_config_path: str | None = None,
    ) -> None:
        self._watch_paths = [str(Path(p).resolve()) for p in watch_paths]
        self._sensor_id = sensor_id
        self._tier = tier
        self._ignore_config_path = ignore_config_path
        self._ignore_paths: set[str] = self._load_ignore_paths()
        self._observer: Observer | None = None
        self._running = False
        self._queue: asyncio.Queue[RawEvent] = asyncio.Queue(maxsize=512)

    def _load_ignore_paths(self) -> set[str]:
        """从配置文件加载排除路径。
        配置格式见 config/sensor_ignore.json。
        """
        if self._ignore_config_path is None:
            return set()

        config_file = Path(self._ignore_config_path)
        if not config_file.exists():
            return set()

        try:
            with open(config_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            paths = data.get("ignore_paths", [])
            # 解析为绝对路径 (相对路径相对于配置文件所在目录)
            base = config_file.parent.parent  # config/ 的父目录 = 项目根
            return {
                str((base / p).resolve()) for p in paths
            }
        except (json.JSONDecodeError, KeyError, OSError):
            return set()

    @property
    def sensor_id(self) -> str:
        return self._sensor_id

    @property
    def tier(self) -> int:
        return self._tier

    async def watch(self) -> AsyncIterator[RawEvent]:
        """启动 watchdog Observer，持续产出 RawEvent。"""
        self._running = True
        self._queue = asyncio.Queue(maxsize=512)

        handler = _WatchdogHandler(
            self._sensor_id, self._queue, self._ignore_paths
        )
        self._observer = Observer()
        self._observer.start()
        # 使用 _WatchdogHandler 作为事件处理器
        for watch_path in self._watch_paths:
            self._observer.schedule(handler, watch_path, recursive=True)

        try:
            while self._running:
                try:
                    # 等待事件，超时 1s 以检查 _running 标志
                    event = await asyncio.wait_for(self._queue.get(), timeout=1.0)
                    # 填充传感器时间戳
                    import time
                    event.sensor_timestamp = time.strftime(
                        "%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()
                    )
                    yield event
                except asyncio.TimeoutError:
                    continue
        finally:
            if self._observer:
                self._observer.stop()
                self._observer.join(timeout=5)

    async def stop(self) -> None:
        self._running = False
        if self._observer:
            self._observer.stop()
            self._observer.join(timeout=5)
            self._observer = None

    async def health_check(self) -> bool:
        """检查 watchdog Observer 是否存活。"""
        if self._observer is None:
            # 还没启动 — 不算挂
            return True
        return self._observer.is_alive()

    @property
    def watch_paths(self) -> list[str]:
        return list(self._watch_paths)
