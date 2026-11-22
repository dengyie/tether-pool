"""tests/test_bridge_writer_lock.py - 跨脚本桥配置写锁单测 (2026-11-23 优化 2)。

sub/skyward/teleapi 三脚本并发 read-modify-write 同一份 huawei-bridge.yaml,
各脚本独立 flock 防不了互相丢更新 —— 共享 /tmp/ip-pool-bridge-writer.lock 串行化。
"""
import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.atomic_yaml import (  # noqa: E402
    BRIDGE_WRITER_LOCK, acquire_bridge_writer_lock, release_bridge_writer_lock)


def setup_function(function):
    Path(BRIDGE_WRITER_LOCK).unlink(missing_ok=True)


def test_acquire_is_exclusive_across_processes(tmp_path):
    """子进程持锁时, 本进程非阻塞探测必须拿不到锁; 释放后能拿到。"""
    holder = subprocess.Popen(
        [sys.executable, "-c",
         "import sys, time; sys.path.insert(0, %r); "
         "from scripts.atomic_yaml import acquire_bridge_writer_lock; "
         "fd = acquire_bridge_writer_lock(); assert fd is not None; "
         "print('HELD', flush=True); time.sleep(30)" % str(Path(__file__).resolve().parent.parent)],
        stdout=subprocess.PIPE, text=True)
    try:
        assert holder.stdout.readline().strip() == "HELD"
        assert acquire_bridge_writer_lock(timeout_s=1) is None  # 拿不到 → None
    finally:
        holder.kill()
        holder.wait()
    fd = acquire_bridge_writer_lock(timeout_s=5)
    assert fd is not None
    release_bridge_writer_lock(fd)


def test_acquire_blocks_until_released():
    """带超时阻塞: 持锁释放后等待方应拿到锁 (不 busy-skip)。"""
    fd1 = acquire_bridge_writer_lock()
    assert fd1 is not None
    import threading
    got = []

    def waiter():
        fd2 = acquire_bridge_writer_lock(timeout_s=5)
        got.append(fd2 is not None)

    t = threading.Thread(target=waiter)
    t.start()
    time.sleep(0.5)          # waiter 进入等待
    release_bridge_writer_lock(fd1)
    t.join(timeout=10)
    assert got == [True]


def test_release_is_idempotent_safe():
    fd = acquire_bridge_writer_lock()
    release_bridge_writer_lock(fd)
    # 释放后立即可再获取 (同进程 flock 语义: 新 fd 重新竞争)
    fd2 = acquire_bridge_writer_lock(timeout_s=2)
    assert fd2 is not None
    release_bridge_writer_lock(fd2)
