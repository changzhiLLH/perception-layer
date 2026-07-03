"""MCP Server 启动脚本 — 从源码目录运行 (开发模式)。

使用方式:
  python run_mcp.py [watch_paths...]

pypi 安装后请用 perception-layer 命令或 python -m perception_layer。
"""

import os
import sys
from pathlib import Path

# 项目根: 此脚本所在目录
_PROJECT_ROOT = Path(__file__).resolve().parent

# 1. 让 perception_layer 包可导入
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# 2. 切到项目根 (config/ 和 data/ 的相对路径依赖)
os.chdir(str(_PROJECT_ROOT))

# 3. 启动
from perception_layer.cli import main

if __name__ == "__main__":
    sys.argv[1:] = sys.argv[1:]  # 保持原样传给 cli.main()
    main()
