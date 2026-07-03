"""Phase 2 关 1 集成测试:
启动 MCP server → 等待传感器就绪 → 改文件 → JSON-RPC 调工具 → 验证有数据。

用法: cd perception-layer && python tests/test_mcp_integration.py
"""

import asyncio
import json
import os
import subprocess
import sys
import time


def main():
    # 1. 启动 MCP server 子进程
    project_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    test_file = os.path.join(project_dir, "_mcp_test_file.txt")

    # 清理旧测试文件
    for f in [test_file]:
        if os.path.exists(f):
            os.remove(f)

    print("[TEST] 启动 MCP server...")
    proc = subprocess.Popen(
        [sys.executable, "-m", "src.mcp_server"],
        cwd=project_dir,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        # 2. 发送 initialize
        print("[TEST] 发送 initialize...")
        init_req = json.dumps({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "initialize",
            "params": {
                "protocolVersion": "2024-11-05",
                "capabilities": {},
                "clientInfo": {"name": "test", "version": "1.0"},
            },
        })
        proc.stdin.write(init_req + "\n")
        proc.stdin.flush()

        # 读 initialize 响应
        init_resp = proc.stdout.readline()
        print(f"[TEST] Initialize 响应: {init_resp[:80]}...")

        # 发送 initialized 通知
        proc.stdin.write(json.dumps({
            "jsonrpc": "2.0",
            "method": "notifications/initialized",
            "params": {},
        }) + "\n")
        proc.stdin.flush()

        # 3. 等 3 秒让传感器启动
        print("[TEST] 等待传感器启动 (3 秒)...")
        time.sleep(3)

        # 4. 创建测试文件 (触发 file.create 事件)
        print(f"[TEST] 创建测试文件: {test_file}")
        with open(test_file, "w") as f:
            f.write("hello from mcp integration test\n")

        # 再等 2 秒让事件被处理
        print("[TEST] 等待事件处理 (2 秒)...")
        time.sleep(2)

        # 5. 测试: get_recent_events
        print("\n[TEST] === get_recent_events ===")
        proc.stdin.write(json.dumps({
            "jsonrpc": "2.0",
            "id": 10,
            "method": "tools/call",
            "params": {"name": "get_recent_events", "arguments": {"limit": 10}},
        }) + "\n")
        proc.stdin.flush()

        resp = proc.stdout.readline()
        data = json.loads(resp)
        content = json.loads(data["result"]["content"][0]["text"])
        events = content.get("events", [])
        print(f"[TEST] 事件数: {content['count']}")

        # 找 file.create 事件
        create_events = [
            e for e in events
            if e.get("event_type") == "file.create"
        ]
        print(f"[TEST] file.create 事件数: {len(create_events)}")
        for e in create_events[:3]:
            print(f"  - {e['source']['path']} ({e['event_id'][:8]})")

        # 6. 测试: get_event_count
        print("\n[TEST] === get_event_count ===")
        proc.stdin.write(json.dumps({
            "jsonrpc": "2.0",
            "id": 11,
            "method": "tools/call",
            "params": {"name": "get_event_count", "arguments": {"window_ms": 30000}},
        }) + "\n")
        proc.stdin.flush()

        resp = proc.stdout.readline()
        data = json.loads(resp)
        stats = json.loads(data["result"]["content"][0]["text"])
        print(f"[TEST] 总计: {stats['total']}")
        print(f"[TEST] by_type: {stats['by_type']}")
        print(f"[TEST] by_directory keys: {list(stats['by_directory'].keys())}")

        # 7. 测试: query_by_handle (如果有事件)
        if events:
            test_handle = events[0]["event_id"]
            print(f"\n[TEST] === query_by_handle ({test_handle[:8]}...) ===")
            proc.stdin.write(json.dumps({
                "jsonrpc": "2.0",
                "id": 12,
                "method": "tools/call",
                "params": {
                    "name": "query_by_handle",
                    "arguments": {"handle": test_handle},
                },
            }) + "\n")
            proc.stdin.flush()

            resp = proc.stdout.readline()
            data = json.loads(resp)
            results = json.loads(data["result"]["content"][0]["text"])["results"]
            found = results.get(test_handle)
            if found:
                print(f"[TEST] [OK] query_by_handle: {found.get('event_type')} {found.get('source',{}).get('path','?')}")
            else:
                print(f"[TEST] [FAIL] query_by_handle: not found {test_handle[:8]}")

        # 8. 验证结果
        print("\n[TEST] === 验证 ===")
        passed = True

        if content["count"] == 0:
            print("[TEST] [FAIL] 无事件 — 传感器可能没正常启动或没捕获到文件变更")
            passed = False
        else:
            print(f"[TEST] [OK] 有 {content['count']} 个事件")

        if stats["total"] == 0:
            print("[TEST] [FAIL] 统计为 0 — 窗口内无数据")
            passed = False
        else:
            print(f"[TEST] [OK] 统计有数据: total={stats['total']}")

        if passed:
            print("\n[TEST] [PASS] 关 1 通过！")
        else:
            print("\n[TEST] [FAIL] 关 1 失败 — 需排查")
            # 打印 stderr 帮助排查
            print("\n[TEST] stderr (最后 10 行):")
            import select
            stderr_lines = []
            try:
                while True:
                    ready, _, _ = select.select([proc.stderr], [], [], 0.5)
                    if ready:
                        line = proc.stderr.readline()
                        if line:
                            stderr_lines.append(line.strip())
                    else:
                        break
            except Exception:
                pass
            for l in stderr_lines[-10:]:
                print(f"  [stderr] {l}")

        return passed

    finally:
        # 清理
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        if os.path.exists(test_file):
            os.remove(test_file)


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
