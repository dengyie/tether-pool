"""scripts/atomic_yaml.py - yaml 配置文件原子落盘 helper (2026-11-15 review P2)。

3400 条目的大文件绝不能 Path.write_text (truncate-then-write): 崩溃/并发读会
拿到半截 yaml, reload 触发毁灭性 prune。统一 tmp + os.replace 原子替换,
与 api/server.py::_remove_from_config 同一模式。
"""

import os
import tempfile
from pathlib import Path
from typing import Union


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
