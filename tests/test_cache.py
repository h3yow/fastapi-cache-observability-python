"""HotDataCache 行为测试：每个用例打印关键输入与判定依据。"""
import asyncio
import logging

import pytest

from app.cache import DataNotFoundError, HotDataCache, SourceUnavailableError
from app.datasource import SimulatedDataSource

log = logging.getLogger("test.cache")


def make_cache(clock, ttl=10.0, negative_ttl=2.0, **kw):
    source = SimulatedDataSource()
    cache = HotDataCache(loader=source.fetch, ttl=ttl, negative_ttl=negative_ttl, clock=clock, **kw)
    return source, cache


async def test_cold_start_computes_once_then_hits(clock):
    source, cache = make_cache(clock)
    source.put("a", "v1")
    log.info("输入: 冷启动连续读 key=a 两次; 判定: 值正确且数据源只被调用 1 次")
    assert await cache.get("a") == "v1"
    assert await cache.get("a") == "v1"
    assert source.call_count("a") == 1, f"期望 1 次真实计算, 实际 {source.call_count('a')}"


async def test_concurrent_reads_same_key_merge(clock):
    source, cache = make_cache(clock)
    source.put("hot", "v")
    source.set_latency(0.05)
    n = 50
    log.info("输入: %d 个并发协程同时读 key=hot(数据源延迟50ms); 判定: 全部拿到同值且只计算 1 次", n)
    results = await asyncio.gather(*[cache.get("hot") for _ in range(n)])
    assert results == ["v"] * n
    assert source.call_count("hot") == 1, f"单飞失败: 实际计算 {source.call_count('hot')} 次"


async def test_concurrent_reads_different_keys_not_blocked(clock):
    source, cache = make_cache(clock)
    source.put("k1", "v1")
    source.put("k2", "v2")
    source.set_latency(0.05)
    log.info("输入: 并发读 k1,k2(各延迟50ms); 判定: 总耗时远小于串行 100ms 且各自计算 1 次")
    start = asyncio.get_event_loop().time()
    r1, r2 = await asyncio.gather(cache.get("k1"), cache.get("k2"))
    elapsed = asyncio.get_event_loop().time() - start
    log.info("判定依据: elapsed=%.3fs", elapsed)
    assert (r1, r2) == ("v1", "v2")
    assert elapsed < 0.1, "不同 key 之间发生了互相阻塞"
    assert source.call_count("k1") == 1 and source.call_count("k2") == 1


async def test_failure_propagates_to_all_waiters_and_not_cached(clock):
    source, cache = make_cache(clock)
    source.put("x", "ok")
    source.fail_next(SourceUnavailableError("boom"))
    source.set_latency(0.02)
    n = 10
    log.info("输入: %d 个并发读 key=x, 数据源首次抛 SourceUnavailableError; 判定: 全部拿到同一异常, 重试可成功", n)
    results = await asyncio.gather(*[cache.get("x") for _ in range(n)], return_exceptions=True)
    assert all(isinstance(r, SourceUnavailableError) for r in results)
    assert len({id(r) for r in results}) == 1, "等待方未拿到一致的失败结果"
    assert cache.size() == 0, "失败结果不应写入缓存"
    assert await cache.get("x") == "ok", "失败后重试应重新计算成功"
    assert source.call_count("x") == 2


async def test_expired_read_returns_stale_and_single_refresh(clock):
    source, cache = make_cache(clock, ttl=10.0)
    source.put("a", "old")
    assert await cache.get("a") == "old"
    clock.advance(10.0)  # 到达过期边界
    source.put("a", "new")
    source.set_latency(0.05)
    n = 20
    log.info("输入: key=a 已过期, 数据源新值 new(延迟50ms), %d 个并发读; 判定: 全部立即拿到旧值 old, 仅触发 1 次刷新", n)
    results = await asyncio.gather(*[cache.get("a") for _ in range(n)])
    assert results == ["old"] * n, "刷新期间应继续提供旧值"
    await asyncio.sleep(0.1)  # 等后台刷新完成
    assert source.call_count("a") == 2, f"雪崩: 刷新次数异常, 总调用 {source.call_count('a')}"
    assert await cache.get("a") == "new", "刷新完成后应提供新值"


async def test_refresh_failure_falls_back_to_stale(clock):
    source, cache = make_cache(clock, ttl=10.0)
    source.put("a", "stable")
    assert await cache.get("a") == "stable"
    clock.advance(10.0)
    source.fail_next(SourceUnavailableError("flaky"))
    log.info("输入: key=a 过期且刷新失败; 判定: 读取仍回退到旧值 stable")
    assert await cache.get("a") == "stable", "刷新失败应回退到上一份有效结果"
    await asyncio.sleep(0.01)
    assert await cache.get("a") == "stable"


async def test_cold_read_source_failure_raises(clock):
    source, cache = make_cache(clock)
    source.fail_next(SourceUnavailableError("down"))
    log.info("输入: 冷读 key=missing 且数据源异常; 判定: 无旧值可回退, 异常直接抛出")
    with pytest.raises(SourceUnavailableError):
        await cache.get("missing")


async def test_not_found_is_negative_cached_and_distinct(clock):
    source, cache = make_cache(clock, negative_ttl=2.0)
    log.info("输入: 读取不存在 key=ghost 三次; 判定: 抛 DataNotFoundError, 负缓存期内只查 1 次数据源")
    for _ in range(3):
        with pytest.raises(DataNotFoundError):
            await cache.get("ghost")
    assert source.call_count("ghost") == 1
    clock.advance(2.0)  # 负缓存过期
    with pytest.raises(DataNotFoundError):
        await cache.get("ghost")
    await asyncio.sleep(0.01)  # 等后台重新探测完成
    assert source.call_count("ghost") == 2, "负缓存过期后应重新探测"


async def test_max_entries_bounds_memory(clock):
    source, cache = make_cache(clock, max_entries=100)
    for i in range(500):
        source.put(f"k{i}", "v")
        await cache.get(f"k{i}")
    log.info("输入: 写入 500 个 key, max_entries=100; 判定: 缓存条数 <= 100, 实际 %d", cache.size())
    assert cache.size() <= 100
