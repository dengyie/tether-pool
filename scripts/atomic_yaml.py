"""scripts/atomic_yaml.py - yaml 配置文件原子落盘 helper (2026-11-15 review P2)。

3400 条目的大文件绝不能 Path.write_text (truncate-then-write): 崩溃/并发读会
拿到半截 yaml, reload 触发毁灭性 prune。统一 tmp + os.replace 原子替换,
与 api/server.py::_remove_from_config 同一模式。
"""

import errno
import fcntl
import os
import tempfile
from pathlib import Path
from typing import Union

BRIDGE_WRITER_LOCK = "/tmp/ip-pool-bridge-writer.lock"


def acquire_bridge_writer_lock(timeout_s: int = 600):
    """跨脚本互斥: 读-改-写 huawei-bridge.yaml 的全 span 串行化 (2026-11-23)。

    sub/skyward/teleapi 三脚本各持独立 flock 只防自身重入; 三者并发时对同一份
    CFG 做 read-modify-write, 后写者把先写者的部署整份覆盖回去 (丢更新竞态)。
    共享一把写锁, 读 CFG 前获取、main finally 释放。带超时阻塞获取 (非 LOCK_NB
    抢锁): 某脚本卡死也不至于永久堵死其余刷新, 超时视为拿锁失败返回 None,
    调用方按自身 SKIP 口径退出本轮 (下轮 cron 重试)。"""
    fd = open(BRIDGE_WRITER_LOCK, "w")
    try:
        import time as _time
        deadline = _time.time() + timeout_s
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return fd
            except OSError as e:
                if e.errno not in (errno.EAGAIN, errno.EACCES):
                    raise
                if _time.time() >= deadline:
                    fd.close()
                    return None
                _time.sleep(1)
    except Exception:
        fd.close()
        raise


def release_bridge_writer_lock(fd) -> None:
    try:
        fcntl.flock(fd, fcntl.LOCK_UN)
    finally:
        fd.close()


def atomic_write_text(path: Union[str, Path], text: str) -> None:
    """Atomically replace `path` with `text`: write to a tmp file in the same
    directory (same filesystem, os.replace is atomic) then rename over target.
    Content is fsync'd before rename so a crash right after replace can't leave
    an empty/short file."""
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(
        dir=str(target.parent), prefix=target.name + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, str(target))
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
