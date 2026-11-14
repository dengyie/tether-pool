"""
ban-aware allocation 共享过滤层 (2026-11-14 code review 抽出)。

Router (sticky 引擎) 与 Allocator (batch/least-loaded 引擎) 各自消费同样的
quarantine 语义, 但组装与过滤代码此前在两个引擎里各写一份 —— 有漂移风险
(改一处漏另一处)。本模块是唯一事实来源:

  - node_isp()               tags `isp=<name>` 约定 (需求 §5.3)
  - build_ban_excluded_networks()  被封 exit_ip 的 /24 + 显式 avoid_cidr (需求 §5.2)
  - ip_in_excluded_nets()    exit_ip 是否落入排除网段
  - collect_ban_exclusions() 两引擎共用的 quarantine 集合组装 (opt-in)

关键不变量 (勿破坏):
  ban 名集合是独立的 per-call 过滤集, 绝不并入网关 R1 的 excluded ——
  excluded 触发持久重绑, ban 过滤是 per-call 临时性 (opt-in), 两者语义分离。
"""

from typing import Any, Dict, List, Optional, Set, Tuple

import ipaddress

from core.models import ProxyNode
from core.storage import Storage


# 已知家宽 ISP 出口段 (消费方实测, 2026-11-14): pigeonw 同一节点的上游出口会在
# Comcast / AT&T 段间漂移, 静态 isp tag 会过时 —— 探活成功时按 exit_ip 段刷新
# (update_probe_result 调 refresh_isp_tag)。未知段不动作 (宁缺毋滥, 不误标)。
ISP_SEGMENT_MAP: Tuple[Tuple[str, str], ...] = (
    ("23.230.200.0/24", "comcast"),
    ("23.132.180.0/24", "att"),
)


def detect_isp_by_exit_ip(exit_ip: Optional[str]) -> Optional[str]:
    """exit_ip 落入已知 ISP 段则返回 isp 名, 否则 None。"""
    if not exit_ip:
        return None
    try:
        addr = ipaddress.ip_address(exit_ip)
    except ValueError:
        return None
    for cidr, isp in ISP_SEGMENT_MAP:
        try:
            if addr in ipaddress.ip_network(cidr):
                return isp
        except ValueError:
            continue
    return None


def refresh_isp_tag(node: ProxyNode, exit_ip: Optional[str]) -> Optional[ProxyNode]:
    """探活成功后按 exit_ip 段校正 isp tag。

    返回携带新 tags 的 node 副本 (调用方负责落库), tag 已正确或无法判定时返回
    None (无需写库)。已知段与现 tag 不一致才改 —— 段未知时保留现 tag。"""
    detected = detect_isp_by_exit_ip(exit_ip)
    if detected is None:
        return None
    if node_isp(node) == detected:
        return None
    tags = [t for t in (node.tags or []) if not t.lower().startswith("isp=")]
    tags.append(f"isp={detected}")
    return node.model_copy(update={"tags": tags})


def node_isp(node: ProxyNode) -> str:
    """ISP 元数据走 tags 约定 (需求 §5.3): 节点 tags 中 isp=<name> (小写)。
    无该 tag 的节点不参与 avoid_same_isp 判定、也不因此被排除。"""
    for tag in (node.tags or []):
        if tag.lower().startswith("isp="):
            return tag.split("=", 1)[1].strip().lower()
    return ""


def build_ban_excluded_networks(quarantines, avoid_cidrs: Optional[List[str]]) -> List[Any]:
    """被封 exit_ip 的 /24 + 显式 avoid_cidr 网段 (非法 CIDR 跳过, 不炸请求)。"""
    nets: List[Any] = []
    for q in quarantines:
        try:
            if q.exit_ip:
                nets.append(ipaddress.ip_network(f"{q.exit_ip}/24", strict=False))
        except ValueError:
            continue
    for cidr in avoid_cidrs or []:
        try:
            nets.append(ipaddress.ip_network(cidr, strict=False))
        except ValueError:
            continue
    return nets


def ip_in_excluded_nets(exit_ip: Optional[str], nets: List[Any]) -> bool:
    if not exit_ip or not nets:
        return False
    try:
        addr = ipaddress.ip_address(exit_ip)
    except ValueError:
        return False
    return any(addr in net for net in nets)


def collect_ban_exclusions(
    storage: Storage,
    service: str,
    avoid_banned: bool = False,
    avoid_cidrs: Optional[List[str]] = None,
    avoid_same_isp: bool = False,
) -> Tuple[Set[str], List[Any], Set[str]]:
    """quarantine 集合组装, 返回 (节点名集, 排除网段集, ISP 集)。

    opt-in 语义 (用户拍板 2026-11-14): 三个 avoid_* 全部缺省时零查询、空集合,
    调用方完全不受 quarantine 影响。avoid_banned 才取 quarantine 行;
    avoid_cidrs 显式网段独立生效; ISP 集从 quarantine 行派生 —— 单独 avoid_same_isp
    (不带 avoid_banned) 也取行 (只派生 ISP 集, 不产节点名/网段排除)。"""
    if not (avoid_banned or avoid_cidrs or avoid_same_isp):
        return set(), [], set()
    quarantines = storage.get_active_quarantines(service=service) if (avoid_banned or avoid_same_isp) else []
    names: Set[str] = {q.node_name for q in quarantines} if avoid_banned else set()
    isps: Set[str] = {q.isp for q in quarantines if q.isp} if avoid_same_isp else set()
    # build_ban_excluded_networks 同时处理 quarantine /24 与显式 CIDR (str -> ip_network),
    # avoid_banned=False 时传空 quarantine 行, 显式 CIDR 仍走同一转换, 不得绕过。
    nets = build_ban_excluded_networks(quarantines if avoid_banned else [], avoid_cidrs)
    return names, nets, isps
