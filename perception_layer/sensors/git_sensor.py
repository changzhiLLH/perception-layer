"""Git 传感器 — Tier 2 任务上下文激活。

监听 git 事件: 分支切换 / commit / 暂存区变更 / 冲突变化。
轮询式 (不侵入用户 git 配置)，间隔 3 秒可配。

冻结语义:
  所有事件 Type A 可验 — 每个事件的探针是"特定 git 命令输出变了"。
  确定性可算: 同一 git 仓库状态,任何人跑同命令必同输出。
  Regime 1。

零判断: 只报 git 事实,不判断"重要/异常"。
事件原子性: 一个 git 操作一个事件,不做合并。
path=None: git 事件不关联文件路径 — 被去抖窗口 fallback 到 event_id,永不合并。
"""

import asyncio
import json
import os
import shutil
import subprocess
import time
import uuid
from pathlib import Path
from typing import AsyncIterator, Optional

from perception_layer.models.event import (
    EventSource,
    EventPayload,
    RawEvent,
    EventType,
)
from perception_layer.sensors.base import SensorBase


def _run_git(
    args: list[str],
    cwd: str,
    timeout: float = 5.0,
) -> tuple[bool, str]:
    """安全执行 git 命令。

    Args:
        args: git 命令参数 (不含 'git')，如 ['branch', '--show-current']
        cwd: 工作目录
        timeout: 超时秒数

    Returns:
        (ok, output) — ok=False 表示命令失败 (git 不可用/超时/错误)
        成功时 output 是 strip 后的 stdout，失败时 output 是错误信息
    """
    try:
        result = subprocess.run(
            ["git"] + args,
            cwd=cwd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout,
        )
        if result.returncode == 0:
            return True, result.stdout.strip()
        else:
            return True, ""  # 正常情况: 无输出 (如空 repo 的 branch --show-current)
    except subprocess.TimeoutExpired:
        return False, f"git {' '.join(args)} timed out"
    except FileNotFoundError:
        return False, "git not found"
    except Exception as e:
        return False, str(e)


class GitSensor(SensorBase):
    """Git 传感器 — 轮询 git 状态，检测变化。

    原理:
      - 每 poll_interval_sec 秒跑一组 git 命令
      - 对比上次快照，变化则报 RawEvent
      - 非 git repo 时 health_check 返回 False，看门狗报 sensor.offline
      - git 不存在时同上

    Tier 2: 任务上下文激活 (跟 fs-watch 同级)。
    """

    def __init__(
        self,
        watch_path: str,
        poll_interval_sec: float = 3.0,
        sensor_id: str = "git-01",
        tier: int = 2,
    ) -> None:
        self._watch_path = str(Path(watch_path).resolve())
        self._poll_interval = poll_interval_sec
        self._sensor_id = sensor_id
        self._tier = tier
        self._running = False

        # 当前快照 (上一轮 poll 的结果)
        self._snapshot: dict[str, Optional[str]] = {
            "branch": None,
            "head_commit": None,
            "staged_files": None,   # JSON 数组字符串
            "conflict_files": None,  # JSON 数组字符串
        }
        self._snapshot_initialized = False

        # 可用性检查
        self._git_available = shutil.which("git") is not None
        self._in_repo = False

    # --- SensorBase interface ---

    @property
    def sensor_id(self) -> str:
        return self._sensor_id

    @property
    def tier(self) -> int:
        return self._tier

    async def watch(self) -> AsyncIterator[RawEvent]:
        """启动轮询。首次 poll 只建基线不报事件。"""
        self._running = True

        # 初始化检查
        if not self._git_available:
            return  # git 不在 PATH，不产事件。health_check → False
        if not self._check_in_repo():
            return  # 不在 git repo，不产事件。health_check → False

        try:
            while self._running:
                events = self._poll()
                for event in events:
                    yield event
                await asyncio.sleep(self._poll_interval)
        except asyncio.CancelledError:
            pass

    async def stop(self) -> None:
        self._running = False

    async def health_check(self) -> bool:
        """健康检查: git 可用 + 在 git repo 内。"""
        if not self._git_available:
            return False
        return self._check_in_repo()

    # --- 内部方法 ---

    def _check_in_repo(self) -> bool:
        """检查当前工作目录是否在 git repo 内。缓存结果。"""
        ok, out = _run_git(["rev-parse", "--git-dir"], cwd=self._watch_path)
        self._in_repo = ok and len(out) > 0
        return self._in_repo

    def _poll(self) -> list[RawEvent]:
        """执行一轮 poll: 跑 git 命令 → 对比快照 → 报事件。

        Returns:
            本轮检测到的事件列表。首次 poll 返回空 (只建基线)。
        """
        # 1. 跑 git 命令
        current: dict[str, Optional[str]] = {
            "branch": self._get_branch(),
            "head_commit": self._get_head_commit(),
            "staged_files": self._get_staged_files(),
            "conflict_files": self._get_conflict_files(),
        }

        # 2. 首次 poll: 建基线
        if not self._snapshot_initialized:
            self._snapshot = current
            self._snapshot_initialized = True
            return []

        # 3. 对比变化
        events: list[RawEvent] = []

        # branch_switch
        if current["branch"] != self._snapshot["branch"]:
            events.append(self._make_event(
                EventType.GIT_BRANCH_SWITCH,
                prev_hash=self._snapshot.get("branch") or "",
                new_hash=current.get("branch") or "",
            ))

        # commit
        if current["head_commit"] != self._snapshot["head_commit"]:
            # head_commit 变了 → 新 commit
            # 只要求 current 非空 (None→"abc" 报首次 commit, "abc"→None 不报)
            if current["head_commit"]:
                prev_short = (
                    self._snapshot["head_commit"][:12]
                    if self._snapshot.get("head_commit")
                    else "(none)"
                )
                events.append(self._make_event(
                    EventType.GIT_COMMIT,
                    prev_hash=prev_short,
                    new_hash=current["head_commit"][:12],
                ))

        # staged_change
        if current["staged_files"] != self._snapshot["staged_files"]:
            events.append(self._make_event(
                EventType.GIT_STAGED_CHANGE,
                prev_hash=self._snapshot.get("staged_files") or json.dumps([]),
                new_hash=current.get("staged_files") or json.dumps([]),
            ))

        # conflict
        if current["conflict_files"] != self._snapshot["conflict_files"]:
            prev_list = self._parse_file_list(self._snapshot.get("conflict_files"))
            curr_list = self._parse_file_list(current.get("conflict_files"))
            events.append(self._make_event(
                EventType.GIT_CONFLICT,
                prev_hash=json.dumps(prev_list),
                new_hash=json.dumps(curr_list),
            ))

        # 4. 更新快照
        self._snapshot = current

        return events

    # --- git 命令封装 ---

    def _get_branch(self) -> Optional[str]:
        ok, out = _run_git(["branch", "--show-current"], cwd=self._watch_path)
        return out if ok and out else None

    def _get_head_commit(self) -> Optional[str]:
        ok, out = _run_git(
            ["log", "-1", "--format=%H"], cwd=self._watch_path
        )
        return out if ok and out else None

    def _get_staged_files(self) -> Optional[str]:
        ok, out = _run_git(
            ["diff", "--cached", "--name-only"], cwd=self._watch_path
        )
        if ok and out:
            files = sorted(out.split("\n"))
            return json.dumps(files)
        return json.dumps([])

    def _get_conflict_files(self) -> Optional[str]:
        ok, out = _run_git(
            ["diff", "--name-only", "--diff-filter=U"], cwd=self._watch_path
        )
        if ok and out:
            files = sorted(out.split("\n"))
            return json.dumps(files)
        return json.dumps([])

    # --- 辅助 ---

    def _make_event(
        self,
        event_type: EventType,
        prev_hash: str = "",
        new_hash: str = "",
    ) -> RawEvent:
        """创建 RawEvent。path=None (git 事件不关联文件路径)。"""
        return RawEvent(
            event_id=uuid.uuid4().hex[:12],
            sensor_id=self._sensor_id,
            event_type=event_type,
            sensor_timestamp=time.strftime(
                "%Y-%m-%dT%H:%M:%S.000Z", time.gmtime()
            ),
            source=EventSource(
                path=None,  # git 事件不关联文件路径
                pid=None,
            ),
            payload=EventPayload(
                prev_hash=prev_hash,
                new_hash=new_hash,
                delta_bytes=None,
            ),
        )

    @staticmethod
    def _parse_file_list(raw: Optional[str]) -> list[str]:
        """解析 JSON 文件列表。"""
        if not raw:
            return []
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return []
