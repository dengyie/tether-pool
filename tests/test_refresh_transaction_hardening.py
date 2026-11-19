import json
import threading
from pathlib import Path

import pytest

from scripts import refresh_death_state


def test_death_state_parallel_updates_do_not_overwrite_or_collide(tmp_path):
    state_path = tmp_path / "refresh-fails.json"
    workers = 16
    start = threading.Barrier(workers)
    failures = []

    def bump(index):
        try:
            start.wait(timeout=5)
            refresh_death_state.bump_failure(str(state_path), f"source-{index}")
            refresh_death_state.bump_failure(str(state_path), "shared-source")
        except Exception as exc:
            failures.append(exc)

    threads = [threading.Thread(target=bump, args=(i,)) for i in range(workers)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert all(not thread.is_alive() for thread in threads)
    assert not failures
    state = json.loads(state_path.read_text(encoding="utf-8"))
    expected = {f"source-{i}": 1 for i in range(workers)}
    expected["shared-source"] = workers
    assert state == expected


def test_death_state_uses_unique_temp_files_and_atomic_lock(tmp_path):
    source = Path(refresh_death_state.__file__).read_text(encoding="utf-8")
    assert "flock" in source or "FileLock" in source
    assert "mkstemp" in source
