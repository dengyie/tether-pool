"""refresh_death_state.py — 刷新脚本死链接计数器 (2026-11-19 可用性根修)。

订阅 yml 链接失效 (404/410/冻结/上游清空) 时, 刷新脚本旧逻辑只 keep current state
—— 死链接的节点永续 carry-over (gen 僵尸批次 2877 节点即此类残留)。本模块提供
跨轮计数: 连续 DEATH_AFTER 轮失效 → 调用方判源死亡, 淘汰该源全部节点。

状态文件单实例 (/opt/ip-pool/data/refresh_death_fails.json), 各脚本以自身名
为 key 共存。原子写 (tmp + os.replace), 坏文件按空状态降级。
"""
import json
import os
from pathlib import Path

STATE_FILE = os.environ.get(
    "REFRESH_DEATH_STATE",
    "/opt/ip-pool/data/refresh_death_fails.json",
)


def _load(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            d = json.loads(f.read())
        return d if isinstance(d, dict) else {}
    except Exception:
        return {}


def _save(path: str, state: dict) -> None:
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(json.dumps(state, ensure_ascii=False, indent=1))
    os.replace(tmp, path)


def bump_failure(path: str, key: str) -> int:
    """失效计数 +1 (fetch 失败/空订阅), 返回当前连续失效轮数。"""
    path = str(Path(path))
    state = _load(path)
    n = int(state.get(key, 0)) + 1
    state[key] = n
    os.makedirs(os.path.dirname(path), exist_ok=True)
    _save(path, state)
    return n


def clear_failure(path: str, key: str) -> None:
    """成功拉取 → 计数清零 (链接复活是新一轮)。"""
    path = str(Path(path))
    state = _load(path)
    if key in state:
        state.pop(key)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        _save(path, state)
