"""本地模拟数据源：可控延迟、失败模式与调用计数，供测试与演示使用。

- latency_seconds：每次 load 的固定延迟，用于制造并发窗口；
- fail_next(n)：接下来 n 次调用抛 DataSourceUnavailableError（短暂异常）；
- flaky：交替成功/失败，模拟不稳定数据源；
- call_count：实际调用次数，用于断言单飞合并与刷新去重。
"""

from __future__ import annotations

import asyncio
import threading

from .models import DataNotFoundError, DataSourceUnavailableError


class SimulatedDataSource:
    def __init__(self, data: dict[str, object] | None = None, latency_seconds: float = 0.0) -> None:
        self._data: dict[str, object] = dict(data or {})
        self._latency = latency_seconds
        self._fail_next = 0
        self._flaky = False
        self._flaky_toggle = False
        self._count = 0
        self._lock = threading.Lock()

    async def load(self, key: str) -> object:
        with self._lock:
            self._count += 1
            fail = self._fail_next > 0
            if fail:
                self._fail_next -= 1
            if not fail and self._flaky:
                self._flaky_toggle = not self._flaky_toggle
                fail = self._flaky_toggle
            latency = self._latency
        if latency > 0:
            await asyncio.sleep(latency)
        if fail:
            raise DataSourceUnavailableError(f"data source temporarily unavailable for key={key!r}")
        with self._lock:
            if key not in self._data:
                raise DataNotFoundError(key)
            return self._data[key]

    def set(self, key: str, value: object) -> None:
        with self._lock:
            self._data[key] = value

    def remove(self, key: str) -> None:
        with self._lock:
            self._data.pop(key, None)

    def set_latency(self, seconds: float) -> None:
        with self._lock:
            self._latency = seconds

    def fail_next(self, n: int = 1) -> None:
        with self._lock:
            self._fail_next = n

    def set_flaky(self, enabled: bool) -> None:
        with self._lock:
            self._flaky = enabled

    @property
    def call_count(self) -> int:
        with self._lock:
            return self._count

    def reset_count(self) -> None:
        with self._lock:
            self._count = 0
