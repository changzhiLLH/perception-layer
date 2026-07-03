"""事件数据模型。

五类事件：
- RawEvent:     传感器产出，零判断，无 severity
- StampedEvent: 总线盖戳后的事件 (地基1: 单调时钟戳)
- MergedEvent:  去抖合并事件 (Regime 3 自动执行，信息不灭)
- PerceptionHint: 引擎3a规则产出 (只报结构特征，不报语义结论)
- SupersededMarker: 标记原始事件已被去抖合并

冻结语义登记：
- EventType 分类: Regime 1 (Type A 探针: hash/inode 变化)
- MergedEvent.atomicity_violation: 钉死的冻结点 (违反原子性原则)
- PerceptionHint.frozen_semantic: 始终 True (显式登记)
"""

from dataclasses import dataclass, field
from typing import Optional
from enum import StrEnum


class EventType(StrEnum):
    """事件类型枚举。

    冻结语义: event_type 分类。
    Type A 探针:
      file.modify  → 文件 hash 变化 (确定性可算)
      file.create  → inode 从无到有 (确定性可算)
      file.delete  → inode 消失 (确定性可算)
      clock.tick   → 定时器到期 (确定性可算)
      clock.idle_too_long → 最后事件时间戳差超阈值 (确定性可算)
      sensor.offline → 健康检查返回 False (确定性可算)
      bus.restart   → 总线进程重启 (确定性可算)
    Regime 1。
    """
    FILE_MODIFY = "file.modify"
    FILE_CREATE = "file.create"
    FILE_DELETE = "file.delete"
    CLOCK_TICK = "clock.tick"
    CLOCK_IDLE = "clock.idle_too_long"
    SENSOR_OFFLINE = "sensor.offline"
    BUS_RESTART = "bus.restart"


@dataclass
class EventSource:
    """事件来源描述。

    pid 字段原型阶段始终为 None — 放弃 pid 主轴承 (地基2)，
    用时间窗 (地基4) 兜底，代价是因果关联弱。
    """
    path: Optional[str] = None
    pid: Optional[int] = None       # 原型阶段始终 None
    url: Optional[str] = None


@dataclass
class EventPayload:
    """事件负载。零判断 — 没有 severity, 没有 is_anomaly。"""
    prev_hash: Optional[str] = None
    new_hash: Optional[str] = None
    delta_bytes: Optional[int] = None


@dataclass
class RawEvent:
    """传感器产出。零判断原则 — 只报事实，不报判断。

    传感器不做:
      - "这个是不是异常" → 无 severity 字段
      - "这个是不是重要" → 无 priority 字段
      - "这 3 个修改其实是一次保存" → 不去抖 (去抖是引擎的事)
    """
    event_id: str                   # uuid4 hex
    sensor_id: str                  # "fs-watch-01" | "clock-01"
    event_type: EventType
    sensor_timestamp: str           # 传感器本地时间戳 (ISO 8601)
    source: EventSource
    payload: EventPayload


@dataclass
class StampedEvent:
    """总线盖戳后的事件。

    地基 1: bus_timestamp 是单调时钟戳 — 统一参照系，100% 覆盖。
    sensor_timestamp 保留原始传感器时间戳 (不丢弃信息)。
    routing_action 是路由规则机械执行结果 — Type A 可验 (路径前缀匹配)。
    """
    event_id: str
    sensor_id: str
    event_type: EventType
    bus_timestamp: str              # 总线单调时钟戳 (ns since bus start)
    sensor_timestamp: str           # 保留原始传感器时间戳
    source: EventSource
    payload: EventPayload
    routing_action: str = ""        # "persist" | "ring_only" — 路由规则机械执行结果


@dataclass
class MergedEvent:
    """去抖合并事件 (Regime 3 自动执行产物)。

    冻结语义: 合并决策。
    Type A 探针: 无 — 没有确定性判据说"合并对不对"。
      200ms 是经验值，换编辑器/文件系统/网络文件系统，最佳窗口就变了。
    处置: Regime 3 自动+审计。
      前提 (a) 信息不灭: merged_from + superseded marker 保留原始事件
      前提 (b) 窗口内损害可逆: Agent 可通过 handle 取回原始事件

    钉死的冻结点:
      atomicity_violation: true — 违反文档"事件原子性"原则。
      这是有理由的违反 (Windows 文件保存非原子性 + 去抖防抖动)，
      但不隐藏 — Agent 看到此标记知道"这不是一个原子事件，要小心解读"。
    """
    event_id: str                   # 合并后新 id
    event_type: EventType           # 与构成事件相同
    bus_timestamp: str              # 最后被合并事件的 bus_timestamp
    sensor_timestamp: str           # 最后被合并事件的 sensor_timestamp
    source: EventSource
    payload: EventPayload           # 最后被合并事件的 payload
    merged_from: list[str]          # 被合并的原始事件 handle (信息不灭)
    merge_count: int                # 合并了多少个原始事件
    atomicity_violation: bool       # 始终 True — 钉死的冻结点
    atomicity_violation_reason: str # "Windows 文件保存非原子性 + 去抖防抖动 200ms"


@dataclass
class PerceptionHint:
    """引擎 3a 规则产出。只报结构特征，不报语义结论。

    文档 L3 关键修正:
      旧: "函数签名变化" → 无 Type A 探针 → 不合法冻结
      新: "diff 含 export/fn/def 关键字行" → Type A 可验 → 合法

    hint 字段内容必须是结构特征描述，不能是语义结论。
    违反示例: "这是重构" / "这是格式化" / "这是内存泄漏"
    合法示例: "5 文件同目录 180ms 内" / "同路径 3 次修改 150ms 内"

    写入 perception_log.jsonl (独立通道，不回总线)。
    """
    hint_id: str                    # uuid4 hex
    hint: str                       # 结构特征描述 (不是语义结论)
    handle: list[str]               # event_id 列表 (MergedEvent 或 StampedEvent 的 id)
    rule_id: str                    # 产生此 hint 的 3a 规则 id
    bus_timestamp: str
    frozen_semantic: bool           # 始终 True — 显式登记，不隐藏
    type_a_probe: str               # 本规则的 Type A 探针描述 (审计用)
    regime: str                     # "Regime 1"


@dataclass
class SupersededMarker:
    """去抖合并后，标记原始事件已被取代。

    写入 event_log.jsonl。
    单次代价: 低 — 标记本身不丢信息，Agent 可追踪到 merged_into。
    """
    event_id: str                   # 被取代的事件 id
    merged_into: str                # 合并后事件 id
    bus_timestamp: str = ""
    marker_type: str = "superseded"
