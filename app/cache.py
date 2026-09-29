"""热点数据缓存：单飞合并、TTL 过期、并发刷新与失败回退。

语义约定：
- 同一 key 同一时刻最多只有一次真实计算（单飞），不同 key 互不阻塞。
- 计算失败时，所有等待方拿到同一个异常实例；失败结果绝不写入缓存。
- 数据不存在（DataNotFoundError）会被缓存一小段时间（负缓存），
  避免对不存在数据的穿透式重复查询；数据源短暂异常不缓存。
- 过期后由第一个到达的读请求触发刷新，其余请求在刷新期间继续拿到旧值
  （stale-while-revalidate）；刷新失败时旧值继续可用（回退）。
- 没有旧值时，刷新失败直接向上抛出异常。
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any


class DataNotFoundError(Exception):
    """数据本身不存在（区别于数据源短暂异常）。"""


class SourceUnavailableError(Exception):
    """数据源短暂异常。"""


@dataclass(frozen=True)
class CacheEntry:
    value: Any
    stored_at: float
    ttl: float
    is_negative: bool = False  # True 表示“数据不存在”的负缓存


class HotDataCache:
    def __init__(
        self,
        loader: Callable[[str], Awaitable[Any]],
        ttl: float = 30.0,
        negative_ttl: float = 5.0,
        max_entries: int = 10_000,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._loader = loader
        self._ttl = ttl
        self._negative_ttl = negative_ttl
        self._max_entries = max_entries
        self._clock = clock
        self._entries: dict[str, CacheEntry] = {}
        self._inflight: dict[str, asyncio.Task[Any]] = {}
        self._lock = asyncio.Lock()

    def _is_fresh(self, entry: CacheEntry) -> bool:
        return self._clock() - entry.stored_at < entry.ttl

    async def get(self, key: str) -> Any:
        value, _ = await self.fetch(key)
        return value

    async def fetch(self, key: str) -> tuple[Any, str]:
        """读取并返回 (value, outcome)，outcome ∈ {hit, miss_compute, stale}。

        数据不存在时抛 DataNotFoundError；无旧值且数据源异常时抛原始异常。
        """
        async with self._lock:
            entry = self._entries.get(key)
            if entry is not None and self._is_fresh(entry):
                if entry.is_negative:
                    raise DataNotFoundError(key)
                return entry.value, "hit"
            task = self._inflight.get(key)
            if entry is not None:
                # 有过期旧值：确保后台刷新存在（不存在则创建），旧值立即返回
                if task is None:
                    task = asyncio.create_task(self._refresh(key))
                    self._inflight[key] = task
                if entry.is_negative:
                    raise DataNotFoundError(key)
                return entry.value, "stale"
            if task is None:
                task = asyncio.create_task(self._refresh(key))
                self._inflight[key] = task
        # 无旧值：所有并发方一起等待唯一的一次计算
        return await asyncio.shield(task), "miss_compute"

    async def _refresh(self, key: str) -> Any:
        try:
            value = await self._loader(key)
            self._store(key, CacheEntry(value=value, stored_at=self._clock(), ttl=self._ttl))
            return value
        except DataNotFoundError:
            self._store(
                key,
                CacheEntry(value=None, stored_at=self._clock(), ttl=self._negative_ttl, is_negative=True),
            )
            raise
        except Exception:
            # 数据源异常：不覆盖旧值（旧值自然继续可用），不向缓存写入失败结果
            raise
        finally:
            async with self._lock:
                self._inflight.pop(key, None)

    def _store(self, key: str, entry: CacheEntry) -> None:
        if len(self._entries) >= self._max_entries and key not in self._entries:
            # 简单逐出最旧的一条，保证内存可控
            oldest = min(self._entries, key=lambda k: self._entries[k].stored_at)
            del self._entries[oldest]
        self._entries[key] = entry

    async def invalidate(self, key: str) -> None:
        async with self._lock:
            self._entries.pop(key, None)

    def size(self) -> int:
        return len(self._entries)
