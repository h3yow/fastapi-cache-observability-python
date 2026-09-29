"""单飞合并：相同热数据的并发读取只计算一次，不同数据互不阻塞，失败一致且不残留。"""

import asyncio
import time

from app.models import ReadStatus


async def test_concurrent_reads_compute_once(source, make_cache, metrics):
    cache = make_cache(ttl_seconds=60)
    source.set("hot", {"v": 1})
    source.set_latency(0.05)  # 制造并发窗口
    n = 20
    print(f"\n[input] 并发数={n}, 数据源延迟=50ms, key='hot'")

    results = await asyncio.gather(*[cache.get("hot", source.load) for _ in range(n)])
    statuses = [r.status for r in results]
    print(f"[assert] 实际计算次数={source.call_count} (期望 1)")
    print(f"[assert] 状态分布={ {s.value: statuses.count(s) for s in set(statuses)} }")

    assert source.call_count == 1, "同一时刻同一份数据只能实际计算一次"
    assert statuses.count(ReadStatus.MISS_COMPUTED) == 1
    assert statuses.count(ReadStatus.COALESCED) == n - 1
    assert all(r.value == {"v": 1} for r in results)
    assert len({r.version for r in results}) == 1, "所有等待方必须拿到同一份结果"
    assert all(r.wait_ms > 0 for r in results if r.status is ReadStatus.COALESCED)


async def test_different_keys_do_not_block_each_other(source, make_cache):
    cache = make_cache(ttl_seconds=60)
    source.set("a", 1)
    source.set("b", 2)
    source.set_latency(0.1)
    print("\n[input] 两个不同 key 各 100ms 延迟，并发读取")

    t0 = time.monotonic()
    ra, rb = await asyncio.gather(cache.get("a", source.load), cache.get("b", source.load))
    elapsed = time.monotonic() - t0
    print(f"[assert] 总耗时={elapsed*1000:.0f}ms (期望远小于串行的 200ms)")

    assert source.call_count == 2
    assert ra.status is ReadStatus.MISS_COMPUTED and rb.status is ReadStatus.MISS_COMPUTED
    assert elapsed < 0.18, "不同数据之间不能互相阻塞"


async def test_failure_is_consistent_and_not_cached(source, make_cache):
    cache = make_cache(ttl_seconds=60)
    source.set("k", "v")
    source.set_latency(0.05)
    source.fail_next(1)  # leader 的这次计算会失败
    n = 10
    print(f"\n[input] 并发数={n}, 数据源首次调用注入异常")

    results = await asyncio.gather(*[cache.get("k", source.load) for _ in range(n)])
    statuses = {r.status for r in results}
    print(f"[assert] 状态集合={ {s.value for s in statuses} }, 计算次数={source.call_count}")
    print(f"[assert] 缓存残留={cache.stats()}")

    assert statuses == {ReadStatus.ERROR}, "所有等待方要拿到一致的失败结果"
    assert source.call_count == 1, "失败也应合并，不放大对数据源的冲击"
    assert cache.stats()["entries"] == 0, "失败结果不能被当成正常数据留下来"

    # 数据源恢复后立即可用（失败未被缓存）
    ok = await cache.get("k", source.load)
    print(f"[assert] 恢复后状态={ok.status.value}, value={ok.value}")
    assert ok.status is ReadStatus.MISS_COMPUTED and ok.value == "v"


async def test_not_found_is_distinguishable_and_negative_cached(source, make_cache):
    cache = make_cache(ttl_seconds=60, negative_ttl_seconds=0.2)
    n = 8
    print(f"\n[input] 并发数={n}, key 不存在于数据源")

    results = await asyncio.gather(*[cache.get("ghost", source.load) for _ in range(n)])
    print(f"[assert] 状态集合={ {r.status.value for r in results} }, 计算次数={source.call_count}")
    assert {r.status for r in results} == {ReadStatus.NOT_FOUND}
    assert source.call_count == 1

    # 负缓存生效：短期内不再访问数据源
    again = await cache.get("ghost", source.load)
    assert again.status is ReadStatus.NOT_FOUND
    assert source.call_count == 1, "负缓存期间不应重复访问数据源"

    # 与数据源异常可区分：注入异常后状态应为 ERROR 而非 NOT_FOUND
    source.fail_next(1)
    await asyncio.sleep(0.25)  # 等负缓存过期
    err = await cache.get("ghost", source.load)
    print(f"[assert] 数据源异常时状态={err.status.value} (期望 error，区别于 not_found)")
    assert err.status is ReadStatus.ERROR
