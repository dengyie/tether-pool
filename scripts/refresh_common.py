"""scripts/refresh_common.py - 刷新脚本共享工具 (2026-11-23 优化 9)。

实测四脚本重复面 (AST + 源码哈希, 取最小充分集):
  - _unbool_proxies: sub/skyward/teleapi 逐字相同, pigeonw 仅 docstring 不同
  - backup_with_rotation: 四份仅 suffix 与 glob 过滤器不同 —— 参数化
    sub 版本旧 glob `huawei-bridge.yaml.*` 会跨脚本误删带 -skyward 后缀的备份
  - sub_url: 三单源脚本仅 env 名与告警前缀不同 —— 参数化

有意不抽: fetch_and_parse (单源版与 sub 多源版语义分叉, 10-48 行差异)、
main (147-380 行差异, 各脚本核心业务)、probe_via_listeners (薄封装, 已经
共享 bridge_probe.py 实现)。审计报告的 "~800 行重复" 高估, 按实证收敛。
"""

import glob
import os
import sys
import time
from pathlib import Path
from typing import Iterable


def unbool_proxies(proxies):
    """YAML 1.1 陷阱: 凭据恰为 no/yes/on/off/y/n 时被 load 成 bool, dump 后变
    true/false 损坏凭据 (且匹配逻辑恒判"变更")。回退为字符串原值。"""
    for p in proxies:
        for k in ("username", "password", "server"):
            v = p.get(k)
            if isinstance(v, bool):
                p[k] = "true" if v else "false"
    return proxies


def backup_with_rotation(backup_dir: Path, sources: Iterable[Path],
                         keep: int, *, suffix: str = "") -> None:
    """时间戳备份 + 轮转, suffix 圈定本脚本命名空间 (防跨脚本误删)。

    suffix="" (sub) 时 glob 过滤排除任何带 "-" 后缀的备份 —— 旧版
    `huawei-bridge.yaml.*` 跨脚本匹配, 轮转会删掉 skyward/teleapisub/pigeonw
    的备份 (生产实测风险)。"""
    backup_dir.mkdir(exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S")
    for src in sources:
        name = f"{src.name}.{stamp}{suffix}"
        (backup_dir / name).write_bytes(src.read_bytes())
    tag = f"{suffix}" if suffix else ""
    prefix = "*-" + tag if tag else ""
    # 用显式 fnmatch: sub(无后缀) 只收纯时间戳, 带后缀脚本只收自己那份
    import fnmatch
    olds = sorted(
        str(p) for p in backup_dir.iterdir()
        if p.is_file() and (
            fnmatch.fnmatch(p.name, f"*{tag}") if tag
            else not any(p.name.endswith("-" + s) for s in ("skyward", "teleapisub", "pigeonw", "sub"))
        )
    )
    for old in olds[:-keep * 2]:
        os.unlink(old)


def sub_url(env_name: str, fail_tag: str) -> str:
    """bearer-token 订阅 URL 即凭据, 仓库不落盘: 从 /etc/ip-pool-secrets.env 读。"""
    url = os.environ.get(env_name, "").strip()
    if not url:
        print(f"[{fail_tag}] {env_name} not set (see /etc/ip-pool-secrets.env)",
              file=sys.stderr)
        sys.exit(1)
    return url
