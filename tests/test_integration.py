"""端到端：缓存 + 观测联动，验证观测数据与真实调用一致，长时间运行内存可控。"""
import asyncio
import logging

from app.cache import HotDataCache
from app.datasource import SimulatedDataSource
from app.metrics import MetricsRegistry

log = logging.getLogger("test.integration")


def make_stack(clock, ttl=5.0, max_entries=1000, slow_cap=100):
    source = SimulatedDataSource()
    metrics = MetricsRegistry(max_keys=max_entries, slow_log_capacity=slow_cap)

    async def loader(key: str):
        try:
            value = await source.fetch(key)
        except Exception:
            metrics.record_compute(key, failed=True)
            raise
        metrics.record_compute(key)
        return value

    cache = HotDataCache(loader=loader, ttl=ttl, max_entries=max_entries, clock=clock)
    return source, metrics, cache


async def test_metrics_match_real_calls_under_concurrency(clock):
    source, metrics, cache = make_stack(clock)
    source.put("hot", "v")
    source.set_latency(0.01)
    n = 200
    log.info("输入: %d 并发读 key=hot; 判定: metrics.computes == 数据源真实调用次数 == 1", n)
    await asyncio.gather(*[cache.get("hot") for _ in range(n)])
    stats = metrics.key_stats("hot")
    assert stats.computes == source.call_count("hot") == 1


async def test_long_run_memory_and_counts_stay_consistent(clock):
    source, metrics, cache = make_stack(clock, ttl=1.0, max_entries=200)
    rounds, keys = 30, 50
    log.info("输入: %d 轮 x %d 个 key, 每轮推进时钟强制过期; 判定: 每 key 每轮恰好 1 次计算, 缓存与指标表均有界",
             rounds, keys)
    for r in range(rounds):
        for i in range(keys):
            key = f"k{i}"
            source.put(key, f"v{r}")
            if r == 0:
                assert await cache.get(key) == "v0"  # 冷启动直接计算
            else:
                # 过期边界：先回旧值，后台刷新完成后读到新值
                assert await cache.get(key) == f"v{r - 1}"
                await asyncio.sleep(0)
                assert await cache.get(key) == f"v{r}"
        clock.advance(1.0)
    for i in range(keys):
        key = f"k{i}"
        stats = metrics.key_stats(key)
        real = source.call_count(key)
        log.info("key=%s computes=%d real_calls=%d", key, stats.computes, real)
        assert stats.computes == real == rounds, "观测计数与真实调用不一致"
    assert cache.size() <= 200
    assert len(metrics.snapshot()) <= 201  # 200 keys + 可能的 __overflow__
    assert len(metrics.slow_requests()) <= 100


async def test_failure_metrics_distinguish_not_found_and_source_error(clock):
    from app.cache import DataNotFoundError, SourceUnavailableError
    source, metrics, cache = make_stack(clock)
    source.put("flaky", "v")
    source.fail_next(SourceUnavailableError("boom"))
    try:
        await cache.get("flaky")
    except SourceUnavailableError:
        pass
    try:
        await cache.get("ghost")
    except DataNotFoundError:
        pass
    flaky = metrics.key_stats("flaky")
    ghost = metrics.key_stats("ghost")
    log.info("判定: flaky.compute_failures=1 且无负缓存; ghost 计入 computes(真实探测)且失败")
    assert flaky.compute_failures == 1
    assert ghost.computes == 1 and ghost.compute_failures == 1
    # 数据源异常不缓存：恢复后立即可读
    assert await cache.get("flaky") == "v"
