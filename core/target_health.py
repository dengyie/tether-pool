"""
core/target_health.py - Destination-class health tracking for gateway steering

背景 (2026-11-14 宝可梦机场故障): 探活只测 ipify/CF-trace, 机场出口对 Google/OpenAI
网段选择性故障时, 池仍报 healthy, 业务打这些目标 ~0% 可达。拨号阶段又必然成功
(mihomo 桥就在本机), 网关现有的 R1 排除重试永远不触发。

兜底: 按目标类 (google/openai/claude) 周期探活全池健康节点; 网关选路时把
"该类已知坏"的节点软排除 (临时 lease, 不迁移绑定), 候选被排除空时降级回绑定
节点 —— 业务宁可慢/差, 不可 503。上游修复后探活转绿, 排除自动消失, 零人工。
"""

import time
from typing import Dict, Optional, Set, Tuple

# 目标类 → canary URL。语义对齐 probe_target_service: 401/404 容忍, CF challenge
# 与 >=400 判失败。canary 必须是"稳定长期可达"的端点 —— 一个会对健康节点间歇
# 403 的 canary 会造成假阳性排除, 因此只收录实测稳定的三类 (grok 未收录)。
TARGET_CLASS_CANARIES: Dict[str, str] = {
    "google": "https://www.google.com/generate_204",
    "openai": "https://api.openai.com/v1/models",
    "claude": "https://www.anthropic.com",
}

# 域名后缀 → 目标类。只匹配注册域及其子域; IP 直连与未知域名无类 (不引导)。
TARGET_DOMAIN_SUFFIXES: Dict[str, str] = {
    "googleapis.com": "google",
    "google.com": "google",
    "gstatic.com": "google",
    "googleusercontent.com": "google",
    "openai.com": "openai",
    "chatgpt.com": "openai",
    "anthropic.com": "claude",
    "claude.ai": "claude",
}


def classify_target(target_host: str) -> Optional[str]:
    """Map a relay destination host to its target class (None = no steering).

    裸 IP (v4/v6) 与未知域名返回 None —— 后缀表只可能命中注册域及其子域,
    IPv6 字符串不可能以字母后缀结尾, 无需特判。
    """
    if not target_host:
        return None
    host = target_host.lower().rstrip(".")
    for suffix, cls in TARGET_DOMAIN_SUFFIXES.items():
        if host == suffix or host.endswith("." + suffix):
            return cls
    return None


class TargetHealthTracker:
    """
    In-memory per-(node, target-class) health with hysteresis (滑窗 + 连败双路):

    - 连败路: 最近 ``bad_streak`` 次全失败 -> 立即判坏 (稳态故障快速保护)
    - 滑窗路: 满窗 (4 次) 内失败 >=3 (75%) 且最新一次失败 -> 判坏 —— 治理
      f/o/f/f 这类振颤偏坏节点 (纯 f/o/f/o 50% 振颤仍不判坏, 防假阳性)
    - 最新一次成功 -> 立即不坏 (快速恢复, 恢复永远优先于惩罚)
    - bad 记录带 TTL: 探活循环若崩溃, 排除不至于永久化
    """

    WINDOW = 4
    RATE_BAD_FAILS = 3

    def __init__(self, bad_streak: int = 2, bad_ttl_seconds: float = 300.0):
        self.bad_streak = max(1, bad_streak)
        self.bad_ttl_seconds = bad_ttl_seconds
        # (node, class) -> {"window": deque(maxlen=4), "ts": float, "err": str}
        self._state: Dict[Tuple[str, str], Dict] = {}

    def record(self, node_name: str, cls: str, ok: bool, error: str = "", now: Optional[float] = None) -> None:
        now = time.time() if now is None else now
        entry = self._state.get((node_name, cls))
        if entry is None:
            from collections import deque
            entry = {"window": deque(maxlen=self.WINDOW), "ts": now, "err": ""}
            self._state[(node_name, cls)] = entry
        entry["window"].append(bool(ok))
        entry["ts"] = now
        entry["err"] = "" if ok else (error or "probe failed")

    def _entry_bad(self, entry: Dict, now: float) -> bool:
        if now - entry["ts"] > self.bad_ttl_seconds:
            return False
        window = list(entry["window"])
        if not window or window[-1]:
            return False  # 最新一次成功 -> 立即恢复
        if len(window) >= self.bad_streak and all(not r for r in window[-self.bad_streak:]):
            return True  # 连败路
        if (
            len(window) == self.WINDOW
            and sum(1 for r in window if not r) >= self.RATE_BAD_FAILS
        ):
            return True  # 滑窗路 (振颤偏坏)
        return False

    def is_bad(self, node_name: str, cls: str, now: Optional[float] = None) -> bool:
        entry = self._state.get((node_name, cls))
        if not entry:
            return False
        now = time.time() if now is None else now
        return self._entry_bad(entry, now)

    def bad_nodes(self, cls: str, now: Optional[float] = None) -> Set[str]:
        if not cls:
            return set()
        return {
            node for (node, c) in self._state
            if c == cls and self.is_bad(node, cls, now=now)
        }

    def bad_counts(self, now: Optional[float] = None) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for (node, cls) in list(self._state):
            if self.is_bad(node, cls, now=now):
                counts[cls] = counts.get(cls, 0) + 1
        return counts

    def error_of(self, node_name: str, cls: str) -> str:
        entry = self._state.get((node_name, cls))
        return entry["err"] if entry else ""
