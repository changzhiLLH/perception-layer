"""GitSensor 单元测试。

测试:
  1. 轮询逻辑 — git 命令输出变化触发事件
  2. 变化方向 — staged_change 涵盖 stage 和 unstage
  3. 冲突检测 — 冲突出现和消失都触发
  4. 去抖隔离 — git 事件 path=None,不会跟 fs-watch 事件合并
  5. 非 git repo — health_check 返回 False,不产事件
  6. 首次 poll — 建基线不报事件
"""

import asyncio
import json
from unittest.mock import patch, MagicMock

import pytest

from perception_layer.sensors.git_sensor import GitSensor, _run_git
from perception_layer.models.event import EventType, RawEvent


def _fake_git_outputs(commands: dict[tuple[str, ...], str]):
    """构建一个 mock _run_git 函数，根据 args 返回预设输出。

    commands 的 key 是带 git 前缀的参数 tuple,
    如 (branch, --show-current) → 输出。
    """

    def mock_run(args: list[str], cwd: str, timeout: float = 5.0):
        key = tuple(args)  # args 已经不含 'git'，只有子命令
        if key in commands:
            return True, commands[key]
        # 对于未预设的命令,返回空 (不是错误)
        return True, ""

    return mock_run


class TestGitSensorPolling:
    """轮询逻辑: 命令输出变化触发事件。"""

    def test_branch_switch_triggers(self):
        """分支名变了 → GIT_BRANCH_SWITCH 事件。"""
        sensor = GitSensor(watch_path="/tmp/test-repo")
        sensor._git_available = True
        sensor._in_repo = True

        outputs = [
            {  # poll 1: baseline
                ("branch", "--show-current"): "main",
                ("log", "-1", "--format=%H"): "aaa111",
                ("diff", "--cached", "--name-only"): "",
                ("diff", "--name-only", "--diff-filter=U"): "",
            },
            {  # poll 2: branch changed
                ("branch", "--show-current"): "feature-x",
                ("log", "-1", "--format=%H"): "aaa111",
                ("diff", "--cached", "--name-only"): "",
                ("diff", "--name-only", "--diff-filter=U"): "",
            },
        ]

        with patch(
            "perception_layer.sensors.git_sensor._run_git",
            side_effect=[
                _fake_git_outputs(outputs[0]),
                _fake_git_outputs(outputs[1]),
                # Wrap to make the dict-based responder callable
            ][0] if False else None,
        ):
            pass

        # Use simpler approach: directly test _poll
        sensor._get_branch = lambda: "main"
        sensor._get_head_commit = lambda: "aaa111"
        sensor._get_staged_files = lambda: "[]"
        sensor._get_conflict_files = lambda: "[]"

        # First poll: baseline
        events = sensor._poll()
        assert len(events) == 0, "首次 poll 应建基线,不报事件"

        # Second poll: branch changed
        sensor._get_branch = lambda: "feature-x"
        events = sensor._poll()
        assert len(events) == 1
        assert events[0].event_type == EventType.GIT_BRANCH_SWITCH
        assert events[0].payload.prev_hash == "main"
        assert events[0].payload.new_hash == "feature-x"

    def test_commit_triggers(self):
        """HEAD hash 变了 → GIT_COMMIT 事件。"""
        sensor = GitSensor(watch_path="/tmp/test-repo")
        sensor._git_available = True
        sensor._in_repo = True

        sensor._get_branch = lambda: "main"
        sensor._get_head_commit = lambda: "aaa111"
        sensor._get_staged_files = lambda: "[]"
        sensor._get_conflict_files = lambda: "[]"

        sensor._poll()  # baseline

        sensor._get_head_commit = lambda: "bbb222"
        events = sensor._poll()
        assert len(events) == 1
        assert events[0].event_type == EventType.GIT_COMMIT
        assert events[0].payload.prev_hash == "aaa111"[:12]
        assert events[0].payload.new_hash == "bbb222"[:12]

    def test_staged_change_triggers(self):
        """staged 文件列表变了 → GIT_STAGED_CHANGE。"""
        sensor = GitSensor(watch_path="/tmp/test-repo")
        sensor._git_available = True
        sensor._in_repo = True

        sensor._get_branch = lambda: "main"
        sensor._get_head_commit = lambda: "aaa111"
        sensor._get_staged_files = lambda: json.dumps([])
        sensor._get_conflict_files = lambda: json.dumps([])

        sensor._poll()  # baseline

        sensor._get_staged_files = lambda: json.dumps(["file1.ts", "file2.ts"])
        events = sensor._poll()
        assert len(events) == 1
        assert events[0].event_type == EventType.GIT_STAGED_CHANGE
        assert "file1.ts" in events[0].payload.new_hash
        assert "file2.ts" in events[0].payload.new_hash

    def test_staged_change_covers_unstage(self):
        """staged 从非空变空 (unstage) 也触发 GIT_STAGED_CHANGE。"""
        sensor = GitSensor(watch_path="/tmp/test-repo")
        sensor._git_available = True
        sensor._in_repo = True

        sensor._get_branch = lambda: "main"
        sensor._get_head_commit = lambda: "aaa111"
        sensor._get_staged_files = lambda: json.dumps(["file1.ts"])
        sensor._get_conflict_files = lambda: json.dumps([])

        sensor._poll()  # baseline: 1 staged file

        sensor._get_staged_files = lambda: json.dumps([])  # unstage
        events = sensor._poll()
        assert len(events) == 1
        assert events[0].event_type == EventType.GIT_STAGED_CHANGE

    def test_conflict_appears_and_disappears(self):
        """冲突出现 (空→非空) 和消失 (非空→空) 都触发。"""
        sensor = GitSensor(watch_path="/tmp/test-repo")
        sensor._git_available = True
        sensor._in_repo = True

        sensor._get_branch = lambda: "main"
        sensor._get_head_commit = lambda: "aaa111"
        sensor._get_staged_files = lambda: json.dumps([])
        sensor._get_conflict_files = lambda: json.dumps([])

        sensor._poll()  # baseline: no conflicts

        # conflict appears
        sensor._get_conflict_files = lambda: json.dumps(["merged.ts"])
        events = sensor._poll()
        assert len(events) == 1
        assert events[0].event_type == EventType.GIT_CONFLICT
        assert "merged.ts" in events[0].payload.new_hash

        # conflict resolved
        sensor._get_conflict_files = lambda: json.dumps([])
        events = sensor._poll()
        assert len(events) == 1
        assert events[0].event_type == EventType.GIT_CONFLICT
        assert events[0].payload.new_hash == json.dumps([])

    def test_multiple_changes_in_one_poll(self):
        """一次 poll 检测到多个变化 → 各报各的。"""
        sensor = GitSensor(watch_path="/tmp/test-repo")
        sensor._git_available = True
        sensor._in_repo = True

        sensor._get_branch = lambda: "main"
        sensor._get_head_commit = lambda: "aaa111"
        sensor._get_staged_files = lambda: json.dumps([])
        sensor._get_conflict_files = lambda: json.dumps([])

        sensor._poll()  # baseline

        sensor._get_branch = lambda: "feature"
        sensor._get_head_commit = lambda: "bbb222"
        events = sensor._poll()
        assert len(events) == 2
        types = {e.event_type for e in events}
        assert EventType.GIT_BRANCH_SWITCH in types
        assert EventType.GIT_COMMIT in types


class TestGitSensorDebounceIsolation:
    """git 事件 path=None → 去抖窗口 fallback 到 event_id → 永不合并。"""

    def test_git_event_path_is_none(self):
        sensor = GitSensor(watch_path="/tmp/test-repo")
        sensor._git_available = True
        sensor._in_repo = True

        sensor._get_branch = lambda: "main"
        sensor._get_head_commit = lambda: "aaa111"
        sensor._get_staged_files = lambda: json.dumps([])
        sensor._get_conflict_files = lambda: json.dumps([])

        sensor._poll()  # baseline

        sensor._get_branch = lambda: "feature"
        events = sensor._poll()
        event = events[0]

        # path=None → 去抖永不合并 git 事件
        assert event.source.path is None


class TestGitSensorNotInRepo:
    """非 git repo 时: health_check=False, 不产事件。"""

    def test_health_check_false_when_not_in_repo(self):
        sensor = GitSensor(watch_path="/tmp/not-a-repo")
        sensor._git_available = True
        sensor._in_repo = False
        result = asyncio.run(sensor.health_check())
        assert result is False

    def test_health_check_false_when_git_not_found(self):
        sensor = GitSensor(watch_path="/tmp/test-repo")
        sensor._git_available = False
        sensor._in_repo = False
        result = asyncio.run(sensor.health_check())
        assert result is False

    def test_no_events_when_not_in_repo(self):
        sensor = GitSensor(watch_path="/tmp/not-a-repo")
        sensor._git_available = True
        sensor._in_repo = False

        async def _run():
            events = []
            async for event in sensor.watch():
                events.append(event)
            return events

        events = asyncio.run(_run())
        assert len(events) == 0


class TestRunGit:
    """_run_git 辅助函数。"""

    def test_run_git_success(self):
        ok, out = _run_git(["version"], cwd=".")
        assert ok is True
        assert "git version" in out.lower() or out == ""

    def test_run_git_not_found(self):
        with patch("subprocess.run", side_effect=FileNotFoundError):
            ok, out = _run_git(["branch"], cwd=".")
            assert ok is False
            assert "not found" in out
