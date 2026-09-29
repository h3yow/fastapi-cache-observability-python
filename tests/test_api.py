"""HTTP 层验证：/data 读取、/metrics 观测口径与真实调用一致。"""
import asyncio
import logging

import httpx
import pytest

from app.cache import HotDataCache
from app.datasource import SimulatedDataSource
from app.metrics import MetricsRegistry
from main import app

log = logging.getLogger("test.api")


@pytest.fixture
async def client():
    import main
    # 每个用例重置全局状态，避免相互污染
    main.source = SimulatedDataSource()
    main.metrics = MetricsRegistry()
    main.source.put("hot", "hello")

    async def loader(key: str):
        try:
            value = await main.source.fetch(key)
        except Exception:
            main.metrics.record_compute(key, failed=True)
            raise
        main.metrics.record_compute(key)
        return value

    main.cache = HotDataCache(loader=loader, ttl=30.0)
    # main.py 的端点引用的是模块级对象，需要同步替换
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        yield c


async def test_read_and_metrics_endpoints(client):
    import main
    log.info("输入: 并发 20 次 GET /data/hot; 判定: 全部 200, /metrics 中 computes == 真实调用 == 1")
    responses = await asyncio.gather(*[client.get("/data/hot") for _ in range(20)])
    assert all(r.status_code == 200 for r in responses)
    assert all(r.json()["value"] == "hello" for r in responses)
    await client.get("/data/hot")  # 第 21 次必为 hit
    m = (await client.get("/metrics")).json()
    log.info("判定依据: /metrics -> %s, 数据源真实调用 %d", m["hot"], main.source.call_count("hot"))
    assert m["hot"]["computes"] == main.source.call_count("hot") == 1
    assert m["hot"]["misses"] == 20, "冷启动并发等待方都应计为 miss"
    assert m["hot"]["hits"] == 1, "第 21 次读取应计为 hit"


async def test_not_found_returns_404_and_source_error_503(client):
    import main
    r = await client.get("/data/ghost")
    log.info("输入: GET /data/ghost(不存在); 判定: 404")
    assert r.status_code == 404
    main.source.fail_next(Exception("down"))
    main.source.put("x", "v")
    r = await client.get("/data/x")
    log.info("输入: GET /data/x 且数据源异常; 判定: 503")
    assert r.status_code == 503
