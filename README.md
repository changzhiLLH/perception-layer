# Perception Layer

给 coding agent 装上"文件感知"的 MCP server。

Agent 能自己查到最近哪些文件变了，不用你每次手动告诉它。

## 为什么

coding agent 不知道你刚改了文件。你改了 `auth.ts`，切回对话，agent 还在说旧代码的事——因为它不知道文件变了。

装上 perception-layer 后，agent 能自己查：
- 最近哪些文件变了（事件流）
- 结构特征（"5 文件同目录 200ms 内修改"）
- 通过事件 ID 回查原始详情

## 安装

### 1. 安装依赖

```bash
pip install perception-layer
```

如果 `perception-layer` 命令找不到（pip install --user 可能不在 PATH），用 `python -m perception_layer` 替代。

### 2. 配置 Claude Code

```bash
claude mcp add-json -s user perception-layer '{"command":"perception-layer"}'
```

如果命令不在 PATH：

```bash
claude mcp add-json -s user perception-layer '{"command":"python","args":["-m","perception_layer"]}'
```

配置完成后重启 Claude Code，会话中即可使用。

### 3. 自定义配置（可选）

配置文件自动创建在 `~/.perception-layer/config/` 下：
- `sensor_ignore.json` — 忽略不想监听的目录（node_modules、.git 等）
- `routing_rules.json` — 控制哪些事件落盘

首次运行时会自动创建默认配置。监听目录默认是启动时的工作目录。如需指定其他目录，在 MCP 配置的 `args` 里加路径：

```bash
claude mcp add-json -s user perception-layer '{"command":"perception-layer","args":["/path/to/project"]}'
```

## 兼容性

| 平台 | 状态 |
|------|------|
| Windows 10/11 | ✅ 已验证 (ReadDirectoryChangesW) |
| Claude Code v2.1+ | ✅ 已验证 (stdio MCP) |
| Python 3.11+ | ✅ 必需 (asyncio.TaskGroup) |
| macOS / Linux | ⚠️ 理论支持 (watchdog 跨平台)，未经实测 |

## 工具

安装后在 Claude Code 中可用以下四个工具：

| 工具 | 入参 | 返回 | 说明 |
|------|------|------|------|
| `get_recent_events` | `limit` (默认 50) | 最近 N 个文件事件 | 哪个文件变了、什么类型（创建/修改/删除）、时间戳、hash |
| `get_recent_hints` | `limit` (默认 20) | 结构特征描述 | 如 "5 文件同目录 200ms 内修改"，只报事实不作判断 |
| `query_by_handle` | `handle` 或 `handles` | 原始事件详情 | 通过事件 ID 回查完整记录，可批量查询 |
| `get_event_count` | `window_ms` (默认 5000) | 按类型+目录统计 | 这段时间内变了多少文件、分布在哪些目录 |

## 示例

在 Claude Code 对话中直接说：

> "看看最近有什么文件变化"

Claude Code 会调用 `get_recent_events` 并告诉你结果。

> "刚才是不是改了很多同目录的文件？"

Claude Code 会调用 `get_recent_hints`，如果触发了同目录共变规则，它会告诉你具体模式。

## 工作原理（简要）

perception-layer 监听文件系统事件（fs-watch → 去抖 → 关联规则），将事件和结构特征暴露为 MCP 工具。agent 通过工具查询，而非被动接收通知。

MCP server 在 Claude Code 会话期间持续运行，监听启动时的工作目录（通常是你的项目目录）。Claude Code 关闭时 server 自动停止。

## 开发

```bash
git clone <repo-url>
cd perception-layer
pip install -e .
pip install -e ".[dev]"  # 包含测试依赖
pytest tests/
```

`python -m perception_layer.main` 提供独立运行模式（直接写 JSONL 文件，不通过 MCP）。

## License

MIT
