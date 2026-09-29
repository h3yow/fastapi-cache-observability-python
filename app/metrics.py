"""线程安全的运行时观测：按 key 与全局聚合的延迟、命中与计算次数。

设计要点：
- 所有计数在同一把锁内累加，高并发下不会因竞态失真；
- key 维度表有上限，超出时按最近访问时间淘汰，长时间运行内存可控；
- 快照为普通 dict，可直接 JSON 序列化导出。
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass

from .models import ReadStatus


@dataclass
class KeyStats:
    key: str
    requests: int = 0
    hits: int = 0            # 命中（含新鲜命中与过期直返）
    computes: int = 0        # 实际调用数据源的次数
    coalesced: int = 0       # 并发合并：等待他人计算的次数
    errors: int = 0          # 数据源异常
    not_found: int = 0       # 数据不存在
    total_latency_ms: float = 0.0
    max_latency_ms: float = 0.0
    last_access: float = 0.0

    @property
    def avg_latency_ms(self) -> float:
        return self.total_latency_ms / self.requests if self.requests else 0.0

    def snapshot(self) -> dict:
        return {
            "key": self.key,
            "requests": self.requests,
            "hits": self.hits,
            "computes": self.computes,
            "coalesced": self.coalesced,
            "errors": self.errors,
            "not_found": self.not_found,
            "total_latency_ms": round(self.total_latency_ms, 3),
            "avg_latency_ms": round(self.avg_latency_ms, 3),
            "max_latency_ms": round(self.max_latency_ms, 3),
        }


class MetricsRegistry:
    """有界注册表：key 数超过 max_keys 时按最近访问时间淘汰最久未用的条目。"""

    def __init__(self, max_keys: int = 10_000, clock=time.monotonic) -> None:
        if max_keys < 1:
            raise ValueError("max_keys must be >= 1")
        self._max_keys = max_keys
        self._clock = clock
        self._lock = threading.Lock()
        self._keys: dict[str, KeyStats] = {}
        self._global = KeyStats(key="__global__")
        self._evicted_keys = 0

    def record(self, key: str, status: ReadStatus, latency_ms: float) -> None:
        now = self._clock()
        with self._lock:
            stats = self._keys.get(key)
            if stats is None:
                if len(self._keys) >= self._max_keys:
                    self._evict_lru_locked()
                stats = KeyStats(key=key)
                self._keys[key] = stats
            for s in (stats, self._global):
                self._accumulate(s, status, latency_ms, now)

    @staticmethod
    def _accumulate(s: KeyStats, status: ReadStatus, latency_ms: float, now: float) -> None:
        s.requests += 1
        s.total_latency_ms += latency_ms
        s.max_latency_ms = max(s.max_latency_ms, latency_ms)
        s.last_access = now
        if status in (ReadStatus.HIT, ReadStatus.STALE_HIT, ReadStatus.FALLBACK_STALE):
            s.hits += 1
        if status in (ReadStatus.MISS_COMPUTED, ReadStatus.STALE_REFRESHED):
            s.computes += 1
        if status is ReadStatus.COALESCED:
            s.coalesced += 1
        if status in (ReadStatus.ERROR, ReadStatus.FALLBACK_STALE):
            # FALLBACK_STALE 背后也是一次真实的数据源失败，必须可见
            s.errors += 1
        if status is ReadStatus.NOT_FOUND:
            s.not_found += 1

    def _evict_lru_locked(self) -> None:
        victim = min(self._keys.values(), key=lambda s: s.last_access)
        del self._keys[victim.key]
        self._evicted_keys += 1

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "global": self._global.snapshot(),
                "tracked_keys": len(self._keys),
                "evicted_keys": self._evicted_keys,
                "keys": {k: v.snapshot() for k, v in sorted(self._keys.items())},
            }

    def per_key(self, key: str) -> dict | None:
        with self._lock:
            stats = self._keys.get(key)
            return stats.snapshot() if stats else None

    def reset(self) -> None:
        with self._lock:
            self._keys.clear()
            self._global = KeyStats(key="__global__")
            self._evicted_keys = 0
