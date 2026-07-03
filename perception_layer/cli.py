"""perception-layer CLI 入口点。

pip install 后用户可通过 "perception-layer" 命令启动 MCP server。
也支持 python -m perception_layer。

工作原理:
  1. 判断运行模式 (源码 or pip install)
  2. 确定 config/data 路径
  3. 安全默认: 警告监听 home/root 目录
  4. 启动 MCP server
"""

import os
import sys
from pathlib import Path


def _get_run_mode() -> tuple[str, Path, Path, Path]:
    """判断运行模式，返回 (mode, project_root, config_dir, data_dir)。

    mode: "source" (源码运行) or "installed" (pip install)
    """
    # 检查是否有 PERCEPTION_LAYER_HOME 环境变量
    env_home = os.environ.get("PERCEPTION_LAYER_HOME")
    if env_home:
        home = Path(env_home)
        home.mkdir(parents=True, exist_ok=True)
        return "installed", home, home / "config", home / "data"

    # 检查是否从源码运行 (项目根有 config/ 目录)
    pkg_dir = Path(__file__).resolve().parent  # perception_layer/
    project_root = pkg_dir.parent               # perception-layer/

    if (project_root / "config").is_dir():
        # 源码运行: 用项目根的 config/ 和 data/
        return "source", project_root, project_root / "config", project_root / "data"

    # pip install 运行: 用 ~/.perception-layer/
    home = Path.home() / ".perception-layer"
    home.mkdir(parents=True, exist_ok=True)
    return "installed", home, home / "config", home / "data"


def _ensure_config(config_dir: Path) -> None:
    """首次运行时从 default_config 复制默认配置文件。"""
    if config_dir.is_dir() and list(config_dir.glob("*.json")):
        return  # 已有配置文件，不覆盖

    config_dir.mkdir(parents=True, exist_ok=True)

    # 从包内 default_config/ 复制
    pkg_dir = Path(__file__).resolve().parent
    default_dir = pkg_dir / "default_config"

    if default_dir.is_dir():
        for src_file in default_dir.glob("*.json"):
            dst_file = config_dir / src_file.name
            if not dst_file.exists():
                dst_file.write_text(
                    src_file.read_text(encoding="utf-8"), encoding="utf-8"
                )
                print(
                    f"[perception-layer] 已创建默认配置: {dst_file}",
                    file=sys.stderr,
                )


def _check_watch_paths(watch_paths: list[str]) -> None:
    """安全默认: 监听 home 或根目录时警告。

    不阻止——用户可能真想监听整个 home，但必须让用户知道风险。
    fs-watch 在大量文件变更时可能丢事件或产生高 CPU 负载。
    """
    suspicious = {str(Path.home()), "/", "C:\\", "C:"}
    for p in watch_paths:
        resolved = str(Path(p).resolve())
        if resolved in suspicious or resolved.rstrip("\\/") in suspicious:
            print(
                f"[perception-layer] ⚠ 警告: 正在监听 {p}\n"
                f"[perception-layer]   监听整个 home 或根目录可能产生大量事件。\n"
                f"[perception-layer]   建议指定具体项目目录: perception-layer /path/to/project",
                file=sys.stderr,
            )
            break


def main() -> None:
    """CLI 入口。"""
    watch_paths = sys.argv[1:] if len(sys.argv) > 1 else None

    # 1. 判断运行模式
    mode, project_root, config_dir, data_dir = _get_run_mode()

    # 2. pip install 模式: 首次运行创建默认配置
    if mode == "installed":
        _ensure_config(config_dir)

    # 3. 安全默认检查
    if watch_paths is None:
        watch_paths = [str(Path.cwd())]
    _check_watch_paths(watch_paths)

    # 4. 打印运行信息
    print(f"[perception-layer] 运行模式: {mode}", file=sys.stderr)
    print(
        f"[perception-layer] 配置目录: {config_dir}", file=sys.stderr
    )
    print(
        f"[perception-layer] 数据目录: {data_dir}", file=sys.stderr
    )
    print(
        f"[perception-layer] 监听目录: {watch_paths}", file=sys.stderr
    )

    # 5. 启动 MCP server
    from perception_layer.mcp_server import run as run_server

    run_server(
        watch_paths=watch_paths,
        project_root=str(project_root),
    )


if __name__ == "__main__":
    main()
