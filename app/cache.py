"""热点数据缓存：单飞合并、过期并发刷新、失败回退。

并发语义（单事件循环内，检查与登记之间无 await，天然原子）：
- 冷启动（无任何缓存值）：第一个请求成为 leader 实际计算，其余请求挂到
  同一个 flight future 上等待，全部拿到同一份结果（含一致的失败）。
- 过期刷新（有旧值）：持锁者同步刷新，期间到达的请求立即拿到旧值
  （STALE_HIT），不阻塞、不重复刷新。
- 失败处理：数据源异常不写入缓存；有旧值时回退旧值并短暂延长过期时间
  （retry_after），避免异常期间雪崩式重试；无旧值时返回 ERROR。
- 数据不存在（DataNotFoundError）写入短期负缓存，与数据源异常可区分。
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable

from .metrics import MetricsRegistry
from .models import (
    CacheEntry,
    DataNotFoundError,
    ReadResult,
    ReadStatus,
)

Loader = Callable[[str], Awaitable[object]]


class HotDataCache:
    def __init__(
        self,
        ttl_seconds: float = 5.0,
        negative_ttl_seconds: float = 1.0,
        retry_after_seconds: float = 0.5,
        max_entries: int = 10_000,
        metrics: MetricsRegistry | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._ttl = ttl_seconds
        self._neg_ttl = negative_ttl_seconds
        self._retry_after = retry_after_seconds
        self._max_entries = max_entries
        self._metrics = metrics or MetricsRegistry()
        self._clock = clock
        self._entries: dict[str, CacheEntry] = {}
        self._neg: dict[str, float] = {}            # key -> 负缓存过期时刻
        self._flights: dict[str, asyncio.Future] = {}  # 冷启动单飞
        self._locks: dict[str, asyncio.Lock] = {}   # 过期刷新互斥
        self._closed = False

    # ------------------------------------------------------------------ API

    async def get(self, key: str, loader: Loader) -> ReadResult:
        if self._closed:
            raise RuntimeError("cache is closed")
        start = self._clock()

        entry = self._entries.get(key)
        if entry is not None and entry.is_fresh(start):
            return self._result(key, entry.value, ReadStatus.HIT, False, 0.0, 0.0, entry.version, start)

        neg_until = self._neg.get(key)
        if neg_until is not None and start < neg_until:
            return self._result(key, None, ReadStatus.NOT_FOUND, False, 0.0, 0.0, 0, start)

        if entry is not None:
            return await self._refresh_path(key, entry, loader, start)
        return await self._cold_path(key, loader, start)

    async def close(self) -> None:
        self._closed = True

    def stats(self) -> dict:
        return {
            "entries": len(self._entries),
            "negative_entries": len(self._neg),
            "inflight": len(self._flights),
            "refresh_locks": len(self._locks),
        }

    # ------------------------------------------------------------- 冷启动

    async def _cold_path(self, key: str, loader: Loader, start: float) -> ReadResult:
        flight = self._flights.get(key)
        if flight is not None:
            return await self._await_flight(key, flight, start)

        loop = asyncio.get_running_loop()
        fut: asyncio.Future = loop.create_future()
        self._flights[key] = fut  # 登记与上面的读取之间无 await，单循环内原子
        try:
            t0 = self._clock()
            try:
                value = await loader(key)
            except DataNotFoundError as exc:
                self._set_negative(key)
                fut.set_exception(exc)
                return self._result(key, None, ReadStatus.NOT_FOUND, False, 0.0, 0.0, 0, start)
            except Exception as exc:
                fut.set_exception(exc)  # 失败不写入任何缓存
                return self._result(key, None, ReadStatus.ERROR, False, 0.0, 0.0, 0, start)
            compute_ms = (self._clock() - t0) * 1000.0
            entry = self._store(key, value, version=1)
            fut.set_result((value, entry.version))
            return self._result(key, value, ReadStatus.MISS_COMPUTED, False, compute_ms, 0.0, entry.version, start)
        finally:
            self._flights.pop(key, None)
            if fut.done() and not fut.cancelled() and fut.exception() is not None:
                pass  # 读取 exception() 以标记已取回，避免事件循环告警

    async def _await_flight(self, key: str, flight: asyncio.Future, start: float) -> ReadResult:
        t0 = self._clock()
        try:
            value, version = await asyncio.shield(flight)
        except DataNotFoundError:
            return self._result(key, None, ReadStatus.NOT_FOUND, False, 0.0, 0.0, 0, start)
        except Exception:
            # 与 leader 一致的失败结果，且未被缓存
            return self._result(key, None, ReadStatus.ERROR, False, 0.0, 0.0, 0, start)
        wait_ms = (self._clock() - t0) * 1000.0
        return self._result(key, value, ReadStatus.COALESCED, False, 0.0, wait_ms, version, start)

    # ------------------------------------------------------------- 过期刷新

    async def _refresh_path(self, key: str, entry: CacheEntry, loader: Loader, start: float) -> ReadResult:
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
            self._prune_locks()

        if lock.locked():
            # 已有刷新在进行：旧值继续可用，立即返回，不阻塞不重复刷新
            return self._result(key, entry.value, ReadStatus.STALE_HIT, True, 0.0, 0.0, entry.version, start)

        async with lock:
            now = self._clock()
            current = self._entries.get(key)
            if current is not None and current.is_fresh(now):
                # 等待锁期间已被他人刷新
                return self._result(key, current.value, ReadStatus.HIT, False, 0.0, 0.0, current.version, start)
            t0 = self._clock()
            try:
                value = await loader(key)
            except DataNotFoundError:
                self._entries.pop(key, None)  # 数据已不存在，旧值一并清除
                self._set_negative(key)
                return self._result(key, None, ReadStatus.NOT_FOUND, False, 0.0, 0.0, 0, start)
            except Exception:
                # 回退到上一份有效结果，并短暂延长过期时间避免雪崩式重试
                entry.expires_at = self._clock() + self._retry_after
                return self._result(key, entry.value, ReadStatus.FALLBACK_STALE, True, 0.0, 0.0, entry.version, start)
            compute_ms = (self._clock() - t0) * 1000.0
            new_entry = self._store(key, value, version=entry.version + 1)
            return self._result(key, value, ReadStatus.STALE_REFRESHED, False, compute_ms, 0.0, new_entry.version, start)

    # ------------------------------------------------------------- 内部工具

    def _store(self, key: str, value: object, version: int) -> CacheEntry:
        now = self._clock()
        if key not in self._entries and len(self._entries) >= self._max_entries:
            victim = min(self._entries.items(), key=lambda kv: kv[1].stored_at)[0]
            del self._entries[victim]
        entry = CacheEntry(value=value, version=version, stored_at=now, expires_at=now + self._ttl, last_ok_at=now)
        self._entries[key] = entry
        self._neg.pop(key, None)
        return entry

    def _set_negative(self, key: str) -> None:
        now = self._clock()
        if key not in self._neg and len(self._neg) >= self._max_entries:
            expired = [k for k, until in self._neg.items() if until <= now]
            for k in expired:
                del self._neg[k]
            if len(self._neg) >= self._max_entries:
                oldest = min(self._neg.items(), key=lambda kv: kv[1])[0]
                del self._neg[oldest]
        self._neg[key] = now + self._neg_ttl

    def _prune_locks(self) -> None:
        if len(self._locks) > self._max_entries:
            idle = [k for k, lk in self._locks.items() if not lk.locked()]
            for k in idle[: len(self._locks) - self._max_entries]:
                del self._locks[k]

    def _result(
        self,
        key: str,
        value: object | None,
        status: ReadStatus,
        stale: bool,
        compute_ms: float,
        wait_ms: float,
        version: int,
        start: float,
    ) -> ReadResult:
        latency_ms = (self._clock() - start) * 1000.0
        self._metrics.record(key, status, latency_ms)
        return ReadResult(
            key=key,
            value=value,
            status=status,
            stale=stale,
            compute_ms=compute_ms,
            wait_ms=wait_ms,
            version=version,
        )
