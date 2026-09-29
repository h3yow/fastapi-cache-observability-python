"""过期与并发刷新：边界确定性、刷新期间旧值可用、失败回退、无雪崩、结果不互相覆盖。"""

import asyncio

from app.models import ReadStatus


async def test_ttl_boundary_is_deterministic(source, make_cache):
    cache = make_cache(ttl_seconds=0.15)
    source.set("k", "v1")
    print("\n[input] ttl=150ms, 依次在 t=0 / t=100ms / t=200ms 读取")

    r1 = await cache.get("k", source.load)
    await asyncio.sleep(0.1)
    r2 = await cache.get("k", source.load)
    await asyncio.sleep(0.1)  # 已越过过期边界
    r3 = await cache.get("k", source.load)
    print(f"[assert] 三次状态={[r.status.value for r in (r1, r2, r3)]}, 版本={[r.version for r in (r1, r2, r3)]}")
    print(f"[assert] 计算次数={source.call_count} (期望 2: 冷启动 1 次 + 过期刷新 1 次)")

    assert r1.status is ReadStatus.MISS_COMPUTED and r1.version == 1
    assert r2.status is ReadStatus.HIT and r2.version == 1 and not r2.stale
    assert r3.status is ReadStatus.STALE_REFRESHED and r3.version == 2
    assert source.call_count == 2


async def test_refresh_serves_stale_and_never_avalanches(source, make_cache):
    cache = make_cache(ttl_seconds=0.1)
    source.set("k", "v1")
    await cache.get("k", source.load)  # 预热
    await asyncio.sleep(0.12)          # 过期
    source.set("k", "v2")
    source.set_latency(0.1)            # 刷新窗口
    n = 15
    print(f"\n[input] 缓存已过期, 刷新延迟=100ms, 并发读取数={n}")

    before = source.call_count
    results = await asyncio.gather(*[cache.get("k", source.load) for _ in range(n)])
    statuses = [r.status for r in results]
    print(f"[assert] 状态分布={ {s.value: statuses.count(s) for s in set(statuses)} }")
    print(f"[assert] 刷新期间实际计算次数={source.call_count - before} (期望 1, 无雪崩)")

    assert source.call_count - before == 1, "过期瞬间的并发只能触发一次刷新"
    assert statuses.count(ReadStatus.STALE_REFRESHED) == 1
    assert statuses.count(ReadStatus.STALE_HIT) == n - 1
    stale_hits = [r for r in results if r.status is ReadStatus.STALE_HIT]
    assert all(r.value == "v1" and r.stale and r.version == 1 for r in stale_hits), \
        "刷新期间旧值必须继续可用"

    # 刷新完成后结果不被旧值覆盖：后续读取只能看到新版本
    after = await cache.get("k", source.load)
    print(f"[assert] 刷新后读取: status={after.status.value}, value={after.value}, version={after.version}")
    assert after.status is ReadStatus.HIT and after.value == "v2" and after.version == 2


async def test_source_error_falls_back_to_last_good_value(source, make_cache):
    cache = make_cache(ttl_seconds=0.1, retry_after_seconds=0.2)
    source.set("k", "good")
    await cache.get("k", source.load)
    await asyncio.sleep(0.12)  # 过期
    source.fail_next(1)        # 刷新时数据源短暂异常
    print("\n[input] 缓存过期 + 数据源注入 1 次异常")

    r = await cache.get("k", source.load)
    print(f"[assert] 回退结果: status={r.status.value}, value={r.value}, stale={r.stale}")
    assert r.status is ReadStatus.FALLBACK_STALE
    assert r.value == "good" and r.stale, "必须回退到上一份有效结果"

    # 回退后短暂退避窗口内不再打数据源（避免异常期间雪崩）
    before = source.call_count
    r2 = await cache.get("k", source.load)
    print(f"[assert] 退避窗口内读取: status={r2.status.value}, 新增计算={source.call_count - before} (期望 0)")
    assert source.call_count == before
    assert r2.value == "good"

    # 数据源恢复后，退避窗口结束即可刷新成功
    await asyncio.sleep(0.22)
    r3 = await cache.get("k", source.load)
    print(f"[assert] 恢复后读取: status={r3.status.value}, version={r3.version}")
    assert r3.status is ReadStatus.STALE_REFRESHED and r3.version == 2


async def test_refresh_detects_data_deleted_from_source(source, make_cache):
    cache = make_cache(ttl_seconds=0.1, negative_ttl_seconds=0.3)
    source.set("k", "v1")
    await cache.get("k", source.load)
    await asyncio.sleep(0.12)
    source.remove("k")  # 刷新时发现数据已不存在
    print("\n[input] 缓存过期 + 数据源中 key 已被删除")

    r = await cache.get("k", source.load)
    print(f"[assert] 状态={r.status.value}, 缓存统计={cache.stats()}")
    assert r.status is ReadStatus.NOT_FOUND, "数据不存在必须与数据源异常区分"
    assert cache.stats()["entries"] == 0, "旧值必须一并清除，不能继续当正常数据"

    before = source.call_count
    r2 = await cache.get("k", source.load)
    assert r2.status is ReadStatus.NOT_FOUND
    assert source.call_count == before, "负缓存期间不重复访问数据源"


async def test_versions_never_regress_under_mixed_concurrency(source, make_cache):
    cache = make_cache(ttl_seconds=0.08)
    source.set("k", 0)
    await cache.get("k", source.load)
    print("\n[input] 多轮过期-刷新循环, 每轮 10 并发, 数据源值单调递增")

    seen_after_refresh: list[int] = []
    for round_no in range(3):
        await asyncio.sleep(0.1)  # 等过期
        source.set("k", round_no + 1)
        source.set_latency(0.03)
        results = await asyncio.gather(*[cache.get("k", source.load) for _ in range(10)])
        refreshed = [r for r in results if r.status is ReadStatus.STALE_REFRESHED]
        assert len(refreshed) == 1, f"第 {round_no} 轮只能有一个刷新者"
        new_version = refreshed[0].version
        # 刷新完成后再读，版本不得回退
        follow = await cache.get("k", source.load)
        print(f"[assert] 第{round_no}轮: 刷新版本={new_version}, 后续读取版本={follow.version}")
        assert follow.version >= new_version, "前后结果不能互相覆盖导致版本回退"
        seen_after_refresh.append(follow.version)
    assert seen_after_refresh == sorted(seen_after_refresh)
