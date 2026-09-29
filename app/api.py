"""FastAPI 应用：数据读取、观测导出与模拟数据源管理端点。"""

from __future__ import annotations

from fastapi import FastAPI
from pydantic import BaseModel

from .cache import HotDataCache
from .datasource import SimulatedDataSource
from .metrics import MetricsRegistry
from .models import ReadStatus


class SourceConfig(BaseModel):
    latency_seconds: float | None = None
    fail_next: int | None = None
    flaky: bool | None = None


class SetValue(BaseModel):
    value: object


def create_app(
    source: SimulatedDataSource | None = None,
    cache: HotDataCache | None = None,
    metrics: MetricsRegistry | None = None,
) -> FastAPI:
    metrics = metrics or MetricsRegistry()
    source = source or SimulatedDataSource()
    cache = cache or HotDataCache(metrics=metrics)

    app = FastAPI(title="hot-data-cache-observability")
    app.state.source = source
    app.state.cache = cache
    app.state.metrics = metrics

    @app.get("/data/{key}")
    async def get_data(key: str) -> dict:
        result = await cache.get(key, source.load)
        return {
            "key": result.key,
            "value": result.value,
            "status": result.status.value,
            "stale": result.stale,
            "version": result.version,
            "compute_ms": round(result.compute_ms, 3),
            "wait_ms": round(result.wait_ms, 3),
        }

    @app.get("/metrics")
    async def get_metrics() -> dict:
        return metrics.snapshot()

    @app.get("/metrics/{key}")
    async def get_key_metrics(key: str) -> dict:
        return metrics.per_key(key) or {"key": key, "requests": 0}

    @app.get("/cache/stats")
    async def get_cache_stats() -> dict:
        return cache.stats()

    @app.put("/source/data/{key}")
    async def put_source_value(key: str, body: SetValue) -> dict:
        source.set(key, body.value)
        return {"key": key, "value": body.value}

    @app.delete("/source/data/{key}")
    async def delete_source_value(key: str) -> dict:
        source.remove(key)
        return {"key": key, "removed": True}

    @app.post("/source/config")
    async def configure_source(cfg: SourceConfig) -> dict:
        if cfg.latency_seconds is not None:
            source.set_latency(cfg.latency_seconds)
        if cfg.fail_next is not None:
            source.fail_next(cfg.fail_next)
        if cfg.flaky is not None:
            source.set_flaky(cfg.flaky)
        return {"ok": True}

    @app.post("/metrics/reset")
    async def reset_metrics() -> dict:
        metrics.reset()
        source.reset_count()
        return {"ok": True}

    return app
