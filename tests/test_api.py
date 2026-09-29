"""HTTP 层：数据读取、并发合并与观测导出端到端验证。"""

import asyncio

import httpx
import pytest

from app.api import create_app
from app.cache import HotDataCache
from app.datasource import SimulatedDataSource
from app.metrics import MetricsRegistry


@pytest.fixture
def client():
    metrics = MetricsRegistry()
    source = SimulatedDataSource({"hot": "v1"}, latency_seconds=0.05)
    cache = HotDataCache(ttl_seconds=60, metrics=metrics)
    app = create_app(source=source, cache=cache, metrics=metrics)
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test"), source, metrics


async def test_read_and_metrics_export(client):
    http, source, metrics = client
    print("\n[input] 通过 HTTP 并发读取 /data/hot x10")

    responses = await asyncio.gather(*[http.get("/data/hot") for _ in range(10)])
    bodies = [r.json() for r in responses]
    statuses = [b["status"] for b in bodies]
    print(f"[assert] 状态分布={ {s: statuses.count(s) for s in set(statuses)} }, call_count={source.call_count}")
    assert all(r.status_code == 200 for r in responses)
    assert statuses.count("miss_computed") == 1
    assert statuses.count("coalesced") == 9
    assert source.call_count == 1

    snap = (await http.get("/metrics")).json()
    print(f"[assert] /metrics global={snap['global']}")
    assert snap["global"]["requests"] == 10
    assert snap["global"]["coalesced"] == 9

    per_key = (await http.get("/metrics/hot")).json()
    assert per_key["requests"] == 10 and per_key["max_latency_ms"] > 0


async def test_source_admin_and_not_found(client):
    http, source, metrics = client
    print("\n[input] 写入 key 后读取; 删除后读取")

    await http.put("/source/data/new", json={"value": 42})
    r = (await http.get("/data/new")).json()
    assert r["status"] == "miss_computed" and r["value"] == 42

    await http.delete("/source/data/new")
    await http.post("/source/config", json={"fail_next": 0})
    # 清掉缓存里的 new：直接等 ttl 不现实，换一个新 key 验证 not_found
    r2 = (await http.get("/data/ghost")).json()
    print(f"[assert] 不存在 key 的响应={r2}")
    assert r2["status"] == "not_found" and r2["value"] is None

    stats = (await http.get("/cache/stats")).json()
    print(f"[assert] /cache/stats={stats}")
    assert stats["negative_entries"] >= 1
