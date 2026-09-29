"""运行时观测：按请求与按 key 的延迟、命中与计算次数统计。

- 所有计数在单把锁内完成，高并发下不会因竞态失真。
- 按 key 的统计表有上限（max_keys），慢请求日志为定长环形缓冲，
  长时间运行内存占用可控。
- outcome 取值：hit / miss_compute / stale / negative / error。
"""
from __future__ import annotations

import threading
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, replace


@dataclass
class KeyStats:
    hits: int = 0
    misses: int = 0          # 触发真实计算的请求数
    computes: int = 0        # 实际计算次数（单飞合并后 <= misses）
    compute_failures: int = 0
    stale_serves: int = 0    # 刷新期间/失败回退时提供旧值的次数
    total_latency_ms: float = 0.0
    max_latency_ms: float = 0.0

    @property
    def avg_latency_ms(self) -> float:
        total = self.hits + self.misses + self.stale_serves
        return self.total_latency_ms / total if total else 0.0


@dataclass
class RequestRecord:
    request_id: str
    key: str
    latency_ms: float
    outcome: str
    ts: float = 0.0


class MetricsRegistry:
    def __init__(
        self,
        max_keys: int = 10_000,
        slow_log_capacity: int = 1_000,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._max_keys = max_keys
        self._clock = clock
        self._lock = threading.Lock()
        self._keys: dict[str, KeyStats] = {}
        self._slow_log: deque[RequestRecord] = deque(maxlen=slow_log_capacity)

    def record(
        self,
        request_id: str,
        key: str,
        latency_ms: float,
        outcome: str,
        slow_threshold_ms: float = 100.0,
    ) -> None:
        with self._lock:
            stats = self._keys.get(key)
            if stats is None:
                if len(self._keys) >= self._max_keys:
                    # 表满时丢弃新 key 的明细，防止内存无限增长
                    stats = self._keys.setdefault("__overflow__", KeyStats())
                    key = "__overflow__"
                else:
                    stats = self._keys[key] = KeyStats()
            if outcome == "hit":
                stats.hits += 1
            elif outcome == "miss_compute":
                stats.misses += 1
            elif outcome == "stale":
                stats.stale_serves += 1
            stats.total_latency_ms += latency_ms
            if latency_ms > stats.max_latency_ms:
                stats.max_latency_ms = latency_ms
            if latency_ms >= slow_threshold_ms:
                self._slow_log.append(
                    RequestRecord(
                        request_id=request_id,
                        key=key,
                        latency_ms=latency_ms,
                        outcome=outcome,
                        ts=self._clock(),
                    )
                )

    def record_compute(self, key: str, failed: bool = False) -> None:
        """由缓存层在真实计算发生时调用，统计实际计算次数。"""
        with self._lock:
            stats = self._keys.get(key)
            if stats is None:
                if len(self._keys) >= self._max_keys:
                    stats = self._keys.setdefault("__overflow__", KeyStats())
                else:
                    stats = self._keys[key] = KeyStats()
            stats.computes += 1
            if failed:
                stats.compute_failures += 1

    def key_stats(self, key: str) -> KeyStats | None:
        with self._lock:
            stats = self._keys.get(key)
            return replace(stats) if stats is not None else None

    def snapshot(self) -> dict[str, KeyStats]:
        with self._lock:
            return {k: replace(v) for k, v in self._keys.items()}

    def slow_requests(self) -> list[RequestRecord]:
        with self._lock:
            return list(self._slow_log)
