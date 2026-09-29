"""观测：计数与真实调用一致、高并发无竞态失真、内存有界、可按 key 定位慢请求。"""

import asyncio
import threading

from app.metrics import MetricsRegistry
from app.models import ReadStatus


async def test_metrics_match_real_calls(source, make_cache, metrics):
    cache = make_cache(ttl_seconds=0.1, retry_after_seconds=0.2)
    source.set("hot", "v")
    source.set_latency(0.05)
    print("\n[input] 混合场景: 5并发冷启动 + 1命中 + 过期后异常回退 + 1次不存在")

    # 冷启动合并：1 MISS_COMPUTED + 4 COALESCED
    await asyncio.gather(*[cache.get("hot", source.load) for _ in range(5)])
    # 新鲜命中
    await cache.get("hot", source.load)
    # 过期 + 数据源异常 -> FALLBACK_STALE
    await asyncio.sleep(0.12)
    source.set_latency(0)
    source.fail_next(1)
    await cache.get("hot", source.load)
    # 不存在
    await cache.get("ghost", source.load)

    snap = metrics.snapshot()
    g = snap["global"]
    hot = metrics.per_key("hot")
    ghost = metrics.per_key("ghost")
    print(f"[assert] global={g}")
    print(f"[assert] hot={hot}")
    print(f"[assert] ghost={ghost}")

    assert g["requests"] == 8
    assert g["computes"] == 1, "全局实际计算次数必须等于数据源真实成功计算次数"
    assert g["coalesced"] == 4
    assert g["hits"] == 2          # 1 新鲜命中 + 1 回退命中
    assert g["errors"] == 1        # 回退背后的数据源失败必须可见
    assert g["not_found"] == 1
    # 口径恒等式：数据源真实调用次数 = 成功计算 + 失败 + 不存在查询
    assert source.call_count == g["computes"] + g["errors"] + g["not_found"]
    assert hot["requests"] == 7 and ghost["requests"] == 1
    # 分 key 之和必须等于全局
    assert hot["requests"] + ghost["requests"] == g["requests"]
    assert hot["coalesced"] + ghost["coalesced"] == g["coalesced"]


async def test_counters_are_race_free_under_concurrency(source, make_cache, metrics):
    cache = make_cache(ttl_seconds=60)
    source.set("k", "v")
    await cache.get("k", source.load)  # 预热，后续全部命中
    n = 1000
    print(f"\n[input] 预热后 {n} 次并发命中读取")

    await asyncio.gather(*[cache.get("k", source.load) for _ in range(n)])
    g = metrics.snapshot()["global"]
    print(f"[assert] global.requests={g['requests']} (期望 {n + 1}), hits={g['hits']}")
    assert g["requests"] == n + 1
    assert g["hits"] == n
    assert metrics.per_key("k")["requests"] == n + 1


def test_counters_are_race_free_across_threads():
    registry = MetricsRegistry()
    threads_n, per_thread = 8, 2000
    print(f"\n[input] {threads_n} 个线程 x {per_thread} 次 record")

    def worker():
        for _ in range(per_thread):
            registry.record("k", ReadStatus.HIT, 1.0)

    threads = [threading.Thread(target=worker) for _ in range(threads_n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    g = registry.snapshot()["global"]
    print(f"[assert] global.requests={g['requests']} (期望 {threads_n * per_thread})")
    assert g["requests"] == threads_n * per_thread, "多线程计数不能因竞态失真"
    assert g["hits"] == threads_n * per_thread


def test_metrics_memory_is_bounded():
    registry = MetricsRegistry(max_keys=50)
    print("\n[input] max_keys=50, 记录 500 个不同 key")
    for i in range(500):
        registry.record(f"key-{i}", ReadStatus.HIT, 1.0)
    snap = registry.snapshot()
    print(f"[assert] tracked_keys={snap['tracked_keys']}, evicted={snap['evicted_keys']}")
    assert snap["tracked_keys"] == 50, "key 维度表必须有界"
    assert snap["evicted_keys"] == 450
    assert snap["global"]["requests"] == 500, "淘汰不影响全局聚合计数"


async def test_cache_entries_are_bounded(source, make_cache):
    cache = make_cache(ttl_seconds=60, max_entries=20)
    print("\n[input] max_entries=20, 读取 100 个不同 key")
    for i in range(100):
        source.set(f"k{i}", i)
        await cache.get(f"k{i}", source.load)
    stats = cache.stats()
    print(f"[assert] cache.stats()={stats}")
    assert stats["entries"] == 20, "缓存条目数必须有界"


async def test_slow_requests_are_locatable_per_key(source, make_cache, metrics):
    cache = make_cache(ttl_seconds=60)
    source.set("slow", "s")
    source.set("fast", "f")
    print("\n[input] slow 延迟 80ms, fast 延迟 0ms")

    source.set_latency(0.08)
    await cache.get("slow", source.load)
    source.set_latency(0)
    await cache.get("fast", source.load)

    slow = metrics.per_key("slow")
    fast = metrics.per_key("fast")
    print(f"[assert] slow.max_latency_ms={slow['max_latency_ms']}, fast.max_latency_ms={fast['max_latency_ms']}")
    assert slow["max_latency_ms"] >= 70, "慢请求的延迟必须按 key 可见"
    assert fast["max_latency_ms"] < slow["max_latency_ms"]
