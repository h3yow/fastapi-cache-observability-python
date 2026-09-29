"""长时间运行：持续混合负载下内存有界、计数恒等、不稳定数据源下不丢数据。"""

import asyncio

from app.models import ReadStatus


async def test_long_running_memory_and_count_consistency(source, make_cache):
    from app.metrics import MetricsRegistry

    metrics = MetricsRegistry(max_keys=50)
    cache = make_cache(ttl_seconds=0.05, retry_after_seconds=0.02, max_entries=50, metrics=metrics)
    for i in range(30):
        source.set(f"k{i}", i)
    rounds, keys_per_round, reads_per_key = 20, 30, 2
    print(f"\n[input] {rounds} 轮 x {keys_per_round} 个 key x {reads_per_key} 次读取, ttl=50ms")

    issued = 0
    for round_no in range(rounds):
        await asyncio.gather(*(
            cache.get(f"k{i}", source.load)
            for i in range(keys_per_round)
            for _ in range(reads_per_key)
        ))
        issued += keys_per_round * reads_per_key
        await asyncio.sleep(0.06)  # 让每轮之间缓存过期，制造持续刷新

    snap = metrics.snapshot()
    g = snap["global"]
    cstats = cache.stats()
    print(f"[assert] global={g}")
    print(f"[assert] cache.stats()={cstats}, tracked_keys={snap['tracked_keys']}")
    print(f"[assert] source.call_count={source.call_count}")

    assert g["requests"] == issued, "请求计数必须与真实调用一致"
    assert g["errors"] == 0 and g["not_found"] == 0
    # 口径恒等式：数据源真实调用次数 = 成功计算 + 失败 + 不存在查询
    assert source.call_count == g["computes"] + g["errors"] + g["not_found"]
    assert cstats["entries"] <= 50, "缓存条目必须有界"
    assert snap["tracked_keys"] <= 50, "key 维度指标必须有界"
    assert cstats["inflight"] == 0, "运行平稳后不应有残留的单飞 future"


async def test_flaky_source_always_serves_last_good_value(source, make_cache):
    cache = make_cache(ttl_seconds=0.05, retry_after_seconds=0.03)
    source.set("k", "stable")
    await cache.get("k", source.load)  # 先预热出一份有效结果
    source.set_flaky(True)  # 数据源交替成功/失败
    rounds = 12
    print(f"\n[input] 数据源交替异常, 连续 {rounds} 轮过期读取")

    statuses = []
    for _ in range(rounds):
        r = await cache.get("k", source.load)
        statuses.append(r.status)
        assert r.value == "stable", "数据源不稳定时也必须返回上一份有效结果，不能丢数据"
        assert r.status is not ReadStatus.ERROR, "有旧值可回退时不应向上抛错"
        await asyncio.sleep(0.06)
    dist = {s.value: statuses.count(s) for s in set(statuses)}
    print(f"[assert] 状态分布={dist}")
    assert ReadStatus.FALLBACK_STALE in statuses, "应观察到失败回退"
    assert ReadStatus.STALE_REFRESHED in statuses, "应观察到恢复后的成功刷新"
