"""本地模拟数据源：可控延迟、可控失败、可统计真实调用次数。"""
from __future__ import annotations

import asyncio
import threading

from app.cache import DataNotFoundError, SourceUnavailableError

__all__ = ["SimulatedDataSource", "DataNotFoundError", "SourceUnavailableError"]


class SimulatedDataSource:
    """线程安全的模拟数据源。

    - put 预置数据；fetch 不存在的数据抛 DataNotFoundError。
    - set_latency 模拟数据源耗时。
    - fail_next 注入接下来 N 次调用的异常（模拟短暂异常）。
    - call_count 记录每个 key 的真实调用次数，用于断言单飞合并。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._data: dict[str, str] = {}
        self._latency: float = 0.0
        self._failures: list[Exception] = []
        self._calls: dict[str, int] = {}

    def put(self, key: str, value: str) -> None:
        with self._lock:
            self._data[key] = value

    def set_latency(self, seconds: float) -> None:
        with self._lock:
            self._latency = seconds

    def fail_next(self, exc: Exception, times: int = 1) -> None:
        with self._lock:
            self._failures.extend([exc] * times)

    def call_count(self, key: str) -> int:
        with self._lock:
            return self._calls.get(key, 0)

    def total_calls(self) -> int:
        with self._lock:
            return sum(self._calls.values())

    async def fetch(self, key: str) -> str:
        with self._lock:
            self._calls[key] = self._calls.get(key, 0) + 1
            latency = self._latency
            failure = self._failures.pop(0) if self._failures else None
        if latency > 0:
            await asyncio.sleep(latency)
        if failure is not None:
            raise failure
        with self._lock:
            if key not in self._data:
                raise DataNotFoundError(key)
            return self._data[key]
