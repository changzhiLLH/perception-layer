"""路由规则模块。

冻结语义: 路径前缀 → 持久化策略。
Type A 探针: 路径字符串前缀匹配 (确定性可算)。
Regime 1。

可绕过 (补强2): 路由规则从 config/routing_rules.json 加载，
Agent 可读写此文件来修改规则。不是硬编码。
"""

import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path


class PersistAction(StrEnum):
    PERSIST = "persist"         # 落盘 (写入 event_log.jsonl)
    RING_ONLY = "ring_only"     # 仅内存环形缓冲 (不落盘)


@dataclass
class RoutingRule:
    prefix: str                 # 路径前缀 (最长前缀匹配)
    action: PersistAction
    reason: str                 # 为什么这条规则存在 (审计用)


class RoutingRules:
    """路由规则集合。最长前缀匹配。

    冻结语义: 路由决策 (什么路径落盘)。
    Type A 探针: 路径字符串前缀匹配 — 两个独立实现同输入必同输出。
    Regime 1。
    可绕过: 从 JSON 文件加载/保存，Agent 可修改。

    原型简化: 不做分级持久化 (文档的异步落盘旁路 + 分级持久化)。
    所有 PERSIST 事件同步写入同一 JSONL。
    """

    def __init__(self, rules: list[RoutingRule]) -> None:
        # 按前缀长度降序排列，保证最长前缀优先匹配
        self._rules = sorted(rules, key=lambda r: len(r.prefix), reverse=True)

    @classmethod
    def from_file(cls, path: str) -> "RoutingRules":
        """从 JSON 配置文件加载路由规则。

        JSON 格式:
        {
          "version": 1,
          "frozen_semantic": true,
          "type_a_probe": "path_prefix_match",
          "rules": [
            {"prefix": "/tmp/", "action": "ring_only", "reason": "..."},
            {"prefix": "/", "action": "persist", "reason": "默认落盘"}
          ]
        }
        """
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        rules = [
            RoutingRule(
                prefix=r["prefix"],
                action=PersistAction(r["action"]),
                reason=r["reason"],
            )
            for r in data["rules"]
        ]
        return cls(rules)

    def to_file(self, path: str) -> None:
        """写回 JSON 配置文件 (Agent 修改规则后的持久化)。"""
        data = {
            "version": 1,
            "frozen_semantic": True,
            "type_a_probe": "path_prefix_match",
            "rules": [
                {
                    "prefix": r.prefix,
                    "action": r.action.value,
                    "reason": r.reason,
                }
                for r in self._rules
            ],
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.write("\n")

    def match(self, event_path: str | None) -> PersistAction:
        """最长前缀匹配。

        Args:
            event_path: 事件关联的文件路径 (None → 默认 PERSIST)

        Returns:
            PERSIST 或 RING_ONLY
        """
        if event_path is None:
            return PersistAction.PERSIST

        # 规范化路径分隔符
        normalized = event_path.replace("\\", "/")

        for rule in self._rules:
            if normalized.startswith(rule.prefix) or normalized.startswith(
                rule.prefix.lstrip("/")
            ):
                return rule.action

        # 无匹配 → 默认 PERSIST
        return PersistAction.PERSIST

    @property
    def rules(self) -> list[RoutingRule]:
        """返回规则列表 (只读视图)。"""
        return list(self._rules)
