"""MetricsRegistry 行为测试：并发计数不失真、慢请求可定位、内存有界。"""
import logging
from concurrent.futures import ThreadPoolExecutor

from app.metrics import MetricsRegistry

log = logging.getLogger("test.metrics")


def test_concurrent_record_no_race():
    reg = MetricsRegistry()
    threads, per_thread = 16, 500
    log.info("输入: %d 线程 x %d 次 record(key=hot); 判定: hits 总数精确等于 %d",
             threads, per_thread, threads * per_thread)

    def work():
        for _ in range(per_thread):
            reg.record("req", "hot", 1.0, "hit")

    with ThreadPoolExecutor(max_workers=threads) as pool:
        list(pool.map(lambda _: work(), range(threads)))
    stats = reg.key_stats("hot")
    assert stats.hits == threads * per_thread, f"计数失真: {stats.hits}"
    assert stats.total_latency_ms == threads * per_thread * 1.0
    assert stats.max_latency_ms == 1.0


def test_slow_requests_logged_with_key_and_bounded():
    reg = MetricsRegistry(slow_log_capacity=10)
    for i in range(25):
        reg.record(f"r{i}", key=f"k{i % 3}", latency_ms=150.0, outcome="miss_compute",
                   slow_threshold_ms=100.0)
    reg.record("fast", key="k0", latency_ms=1.0, outcome="hit")
    slow = reg.slow_requests()
    log.info("输入: 25 条慢请求(容量10) + 1 条快请求; 判定: 慢日志保留最近 10 条且含 key, 实际 %d 条", len(slow))
    assert len(slow) == 10, "慢请求日志必须有界"
    assert slow[-1].request_id == "r24"
    assert all(r.key for r in slow), "慢请求记录必须保留 key 维度以便定位"
    assert all(r.latency_ms >= 100.0 for r in slow)


def test_max_keys_bounds_memory_with_overflow_bucket():
    reg = MetricsRegistry(max_keys=5)
    for i in range(100):
        reg.record("r", key=f"key{i}", latency_ms=1.0, outcome="hit")
    snap = reg.snapshot()
    log.info("输入: 100 个不同 key, max_keys=5; 判定: 统计表 <= 6 项(含 __overflow__), 实际 %d", len(snap))
    assert len(snap) <= 6
    assert "__overflow__" in snap
    assert snap["__overflow__"].hits == 95


def test_compute_count_matches_real_loader_calls():
    reg = MetricsRegistry()
    reg.record_compute("a")
    reg.record_compute("a")
    reg.record_compute("a", failed=True)
    stats = reg.key_stats("a")
    log.info("输入: 2 次成功计算 + 1 次失败计算; 判定: computes=3, compute_failures=1")
    assert stats.computes == 3
    assert stats.compute_failures == 1
