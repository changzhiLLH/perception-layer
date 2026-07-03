"""L5 最小文件接面。

两个物理隔离的文件:
  event_log.jsonl:      原始事件 + superseded marker (总线输出通道)
  perception_log.jsonl: hint + 系统事件 (感知通道 + 自报通道)

物理隔离 (坑6): 引擎输出不回总线 → perception_log.jsonl 独立于 event_log.jsonl。

修正1: 系统标记 (sensor.offline / bus.restart) 不经总线 → 直接写 perception_log.jsonl。
修正2: MergedEvent 不写 perception_log — 它是引擎内部中间产物，
       信息通过 superseded marker + merged_from 在 event_log 中可追溯。
修正3: 增加 query_by_handle — "可绕过"原则的最小实现。
"""

import json
import os
from typing import Optional
from pathlib import Path

import aiofiles


class PerceptionLog:
    """L5 最小文件接面。

    两个独立 JSONL 文件:
      - event_log.jsonl: 所有传感器事件 + superseded marker
      - perception_log.jsonl: hint (引擎产出) + 系统事件 (自报通道)

    Agent 通过读 perception_log.jsonl 实现"被动感知"。
    原型局限: Agent 仍需"主动读文件"——真正的被动感知需要改 harness。
    """

    def __init__(
        self,
        perception_log_path: str,
        event_log_path: str,
    ) -> None:
        self._perception_path = Path(perception_log_path)
        self._event_path = Path(event_log_path)

        # 确保目录存在
        self._perception_path.parent.mkdir(parents=True, exist_ok=True)
        self._event_path.parent.mkdir(parents=True, exist_ok=True)

        # 简单的查询缓存 (event_id → 事件行)
        self._cache: dict[str, dict] = {}
        self._cache_loaded = False

    # === 感知通道 (引擎输出 → perception_log.jsonl) ===

    async def write_hint(self, hint) -> None:
        """追加 hint 到 perception_log.jsonl。

        引擎 3a 规则产出 — 这是 Agent 的主要消费对象。
        """
        line = json.dumps(
            {
                "type": "hint",
                "hint_id": hint.hint_id,
                "hint": hint.hint,
                "handle": hint.handle,
                "rule_id": hint.rule_id,
                "bus_timestamp": hint.bus_timestamp,
                "frozen_semantic": hint.frozen_semantic,
                "type_a_probe": hint.type_a_probe,
                "regime": hint.regime,
            },
            ensure_ascii=False,
        )
        await self._append(self._perception_path, line)

    # === 自报通道 (系统事件 → perception_log.jsonl, 不经总线 — 修正1) ===

    async def write_system_event(
        self,
        event_type: str,
        reason: str,
        timestamp: str,
    ) -> None:
        """写入系统事件到 perception_log.jsonl。

        系统事件不经置信度门控，总是送达。
        不经总线环形缓冲 (修正1: 总线不接受非传感器来源事件)。

        系统事件类型:
          - sensor.offline: 传感器健康检查失败
          - bus.restart: 总线重启
        """
        line = json.dumps(
            {
                "type": "system_event",
                "event_type": event_type,
                "reason": reason,
                "bus_timestamp": timestamp,
            },
            ensure_ascii=False,
        )
        await self._append(self._perception_path, line)

    # === 原始事件通道 (总线输出 → event_log.jsonl) ===

    async def write_event(self, event) -> None:
        """追加原始事件到 event_log.jsonl。

        所有传感器事件 (含 clock.tick) 经过总线后在此落盘。
        StampedEvent 和 MergedEvent 均写入此文件。

        注意: MergedEvent 写入 event_log (非 perception_log — 修正2)。
        """
        if hasattr(event, "merged_from"):
            # MergedEvent
            line = json.dumps(
                {
                    "type": "merged_event",
                    "event_id": event.event_id,
                    "event_type": event.event_type.value,
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
                    "merged_from": event.merged_from,
                    "merge_count": event.merge_count,
                    "atomicity_violation": event.atomicity_violation,
                    "atomicity_violation_reason": event.atomicity_violation_reason,
                },
                ensure_ascii=False,
            )
        else:
            # StampedEvent
            line = json.dumps(
                {
                    "type": "event",
                    "event_id": event.event_id,
                    "event_type": event.event_type.value,
                    "bus_timestamp": event.bus_timestamp,
                    "sensor_timestamp": event.sensor_timestamp,
                    "sensor_id": event.sensor_id,
                    "source": {
                        "path": event.source.path,
                        "pid": event.source.pid,
                    },
                    "payload": {
                        "prev_hash": event.payload.prev_hash,
                        "new_hash": event.payload.new_hash,
                        "delta_bytes": event.payload.delta_bytes,
                    },
                },
                ensure_ascii=False,
            )
        await self._append(self._event_path, line)

    async def write_superseded_marker(self, marker) -> None:
        """标记原始事件已被去抖合并。写入 event_log.jsonl。

        Agent 通过 query_by_handle 回查原始事件时，
        会发现此 marker 并追踪到 merged_into。
        """
        line = json.dumps(
            {
                "type": marker.marker_type,
                "event_id": marker.event_id,
                "merged_into": marker.merged_into,
                "bus_timestamp": marker.bus_timestamp,
            },
            ensure_ascii=False,
        )
        await self._append(self._event_path, line)

    # === 查询接口 (修正3: "可绕过"原则的最小实现) ===

    async def query_by_handle(self, handle: str) -> dict | None:
        """通过 event_id 回查事件。

        从 event_log.jsonl 查找原始事件或合并事件。
        这是"可绕过"原则的最小实现: Agent 不需信任 hint，
        可通过 handle 拉原始事件做自己的判断。

        Args:
            handle: event_id (来自 PerceptionHint.handle 或 MergedEvent.merged_from)

        Returns:
            事件 dict (type, event_id, event_type, source, payload, ...)
            None — 事件未找到 (已过期/已驱逐/无效 handle)
        """
        # 1. 检查缓存
        if handle in self._cache:
            return self._cache[handle]

        # 2. 线性扫描 event_log.jsonl
        try:
            async with aiofiles.open(self._event_path, "r", encoding="utf-8") as f:
                async for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    # 检查 event_id 或 merged_from
                    if record.get("event_id") == handle:
                        self._cache[handle] = record
                        return record

                    # 也检查 merged_from 列表
                    if "merged_from" in record and handle in record.get("merged_from", []):
                        self._cache[handle] = record
                        return record
        except FileNotFoundError:
            return None

        return None

    async def query_by_handle_batch(
        self, handles: list[str]
    ) -> dict[str, dict | None]:
        """批量回查事件。"""
        results: dict[str, dict | None] = {}
        # 先查缓存
        remaining = []
        for h in handles:
            if h in self._cache:
                results[h] = self._cache[h]
            else:
                remaining.append(h)

        if not remaining:
            return results

        # 扫描文件
        remaining_set = set(remaining)
        try:
            async with aiofiles.open(self._event_path, "r", encoding="utf-8") as f:
                async for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    eid = record.get("event_id")
                    if eid in remaining_set:
                        self._cache[eid] = record
                        results[eid] = record
                        remaining_set.discard(eid)

                    # 检查 merged_from
                    for mf in record.get("merged_from", []):
                        if mf in remaining_set:
                            self._cache[mf] = record
                            results[mf] = record
                            remaining_set.discard(mf)

                    if not remaining_set:
                        break
        except FileNotFoundError:
            pass

        # 未找到的 handle
        for h in remaining:
            if h not in results:
                results[h] = None

        return results

    # === 内部方法 ===

    @staticmethod
    async def _append(file_path: Path, line: str) -> None:
        """追加一行 JSON 到文件。同步 append (原型阶段，后续可改异步缓冲)。"""
        async with aiofiles.open(file_path, "a", encoding="utf-8") as f:
            await f.write(line + "\n")
            await f.flush()
