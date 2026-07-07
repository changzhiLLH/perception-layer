"""关 2 集成测试: git-sensor 真实运行 (简化版 — 内存组件,避免 Windows pipe 问题)。

直接在 Python 里启动组件,做 git 操作,验证数据流。
"""

import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path

# 项目根
PROJECT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_DIR))


def run_git(cmd: list[str]) -> str:
    result = subprocess.run(
        ["git"] + cmd, cwd=str(PROJECT_DIR),
        capture_output=True, text=True,
    )
    return result.stdout.strip()


async def main_async():
    from perception_layer.sensors.git_sensor import GitSensor
    from perception_layer.models.event import EventType

    test_file = PROJECT_DIR / "_git_test.txt"
    if test_file.exists():
        test_file.unlink()

    # 保存原始状态
    orig_branch = run_git(["branch", "--show-current"]) or "main"
    print(f"[TEST] 当前分支: {orig_branch}")

    # 1. 初始化 GitSensor
    sensor = GitSensor(
        watch_path=str(PROJECT_DIR),
        poll_interval_sec=2.0,
    )
    sensor._git_available = True
    sensor._in_repo = True

    # 2. 首次 poll 建基线
    events = sensor._poll()
    assert len(events) == 0, f"首次 poll 应为空, 但收到 {len(events)} 个事件"
    print("[TEST] 首次 poll 建基线: 0 事件 (正确)")

    # 3. git checkout -b test-branch
    print("[TEST] git checkout -b test-branch...")
    run_git(["checkout", "-b", "test-branch"])
    await asyncio.sleep(0.5)
    events = sensor._poll()
    print(f"[TEST] 轮询结果: {len(events)} 个事件")
    for e in events:
        print(f"  - {e.event_type}: prev={e.payload.prev_hash} new={e.payload.new_hash}")
    branch_events = [e for e in events if e.event_type == EventType.GIT_BRANCH_SWITCH]
    assert len(branch_events) == 1, f"应有 1 个 branch_switch 事件, 实际 {len(branch_events)}"
    e = branch_events[0]
    assert e.payload.prev_hash == orig_branch
    assert e.payload.new_hash == "test-branch"
    print(f"[TEST] [OK] git.branch_switch: {orig_branch} -> test-branch")

    # 4. git add + commit
    print("[TEST] git add + commit...")
    test_file.write_text("integration test\n")
    run_git(["add", str(test_file)])
    run_git(["commit", "-m", "test: git sensor integration"])
    await asyncio.sleep(0.5)
    events = sensor._poll()
    print(f"[TEST] 轮询结果: {len(events)} 个事件")
    for e in events:
        print(f"  - {e.event_type}: prev={e.payload.prev_hash[:20] if e.payload.prev_hash else '?'} new={e.payload.new_hash[:20] if e.payload.new_hash else '?'}")
    types = {e.event_type for e in events}
    assert EventType.GIT_STAGED_CHANGE in types, f"应有 git.staged_change, 实际: {types}"
    assert EventType.GIT_COMMIT in types, f"应有 git.commit, 实际: {types}"
    print("[TEST] [OK] git.staged_change + git.commit")

    # 5. checkout main
    print("[TEST] git checkout main...")
    run_git(["checkout", orig_branch])
    await asyncio.sleep(0.5)
    events = sensor._poll()
    branch_events = [e for e in events if e.event_type == EventType.GIT_BRANCH_SWITCH]
    assert len(branch_events) >= 1, f"应有第二个 branch_switch, 实际 {len(branch_events)}"
    e = branch_events[0]
    assert e.payload.new_hash == orig_branch
    print(f"[TEST] [OK] git.branch_switch: test-branch -> {orig_branch}")

    # 6. 清理
    run_git(["branch", "-D", "test-branch"])
    if test_file.exists():
        test_file.unlink()

    print("\n[TEST] [PASS] 关 2 全部通过！")


def main():
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
