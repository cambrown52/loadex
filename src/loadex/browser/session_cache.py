"""
In-memory per-session dataset cache for local single-process Dash usage.
"""
from __future__ import annotations

from dataclasses import dataclass
from threading import RLock
from time import time
from typing import Optional

from loadex.classes.dataset import DataSet


@dataclass
class CacheEntry:
    dataset: DataSet
    last_access: float


_LOCK = RLock()
_SESSION_DATASETS: dict[str, CacheEntry] = {}


def set_dataset(session_id: str, dataset: DataSet) -> None:
    """Store or replace a dataset for a session."""
    with _LOCK:
        _SESSION_DATASETS[session_id] = CacheEntry(dataset=dataset, last_access=time())


def get_dataset(session_id: str) -> Optional[DataSet]:
    """Get dataset for a session and refresh last-access time."""
    with _LOCK:
        entry = _SESSION_DATASETS.get(session_id)
        if entry is None:
            return None
        entry.last_access = time()
        return entry.dataset


def pop_dataset(session_id: str) -> Optional[DataSet]:
    """Remove dataset for a session and return it if present."""
    with _LOCK:
        entry = _SESSION_DATASETS.pop(session_id, None)
    return None if entry is None else entry.dataset


def cleanup_expired(max_age_seconds: int = 3600) -> int:
    """Remove entries not accessed within max_age_seconds."""
    now = time()
    removed = 0
    with _LOCK:
        expired_ids = [
            sid for sid, entry in _SESSION_DATASETS.items()
            if (now - entry.last_access) > max_age_seconds
        ]
        for sid in expired_ids:
            _SESSION_DATASETS.pop(sid, None)
            removed += 1
    return removed


def cache_size() -> int:
    """Return number of active session entries."""
    with _LOCK:
        return len(_SESSION_DATASETS)
