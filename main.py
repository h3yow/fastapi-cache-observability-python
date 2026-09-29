import time
import uuid
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, HTTPException, Request

from app.cache import DataNotFoundError, HotDataCache
from app.datasource import SimulatedDataSource
from app.metrics import MetricsRegistry

source = SimulatedDataSource()
metrics = MetricsRegistry()


async def _loader(key: str):
    try:
        value = await source.fetch(key)
    except Exception:
        metrics.record_compute(key, failed=True)
        raise
    metrics.record_compute(key)
    return value


cache = HotDataCache(loader=_loader, ttl=30.0)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 演示用：预置一份热数据
    source.put("hot", "hello-hot-data")
    yield


app = FastAPI(lifespan=lifespan)


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/data/{key}")
async def read_data(key: str, request: Request):
    request_id = request.headers.get("x-request-id", uuid.uuid4().hex[:12])
    start = time.perf_counter()
    outcome = "hit"
    try:
        value, outcome = await cache.fetch(key)
        return {"key": key, "value": value, "request_id": request_id}
    except DataNotFoundError:
        outcome = "negative"
        raise HTTPException(status_code=404, detail=f"data not found: {key}")
    except Exception as exc:
        outcome = "error"
        raise HTTPException(status_code=503, detail=f"source unavailable: {exc!r}")
    finally:
        metrics.record(
            request_id=request_id,
            key=key,
            latency_ms=(time.perf_counter() - start) * 1000,
            outcome=outcome,
        )


@app.get("/metrics")
def read_metrics():
    return {
        key: {
            "hits": s.hits,
            "misses": s.misses,
            "computes": s.computes,
            "compute_failures": s.compute_failures,
            "stale_serves": s.stale_serves,
            "avg_latency_ms": round(s.avg_latency_ms, 3),
            "max_latency_ms": round(s.max_latency_ms, 3),
        }
        for key, s in metrics.snapshot().items()
    }


@app.get("/metrics/slow")
def read_slow():
    return [vars(r) for r in metrics.slow_requests()]


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
