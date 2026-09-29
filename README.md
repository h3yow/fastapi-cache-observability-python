# fastapi-cache-observability-python

在 FastAPI 服务上为热点数据读取提供性能优化（单飞合并、过期刷新、失败回退）与运行时观测能力。

## 结构

- `app/cache.py` — `HotDataCache`：单飞合并 + TTL 过期 + 并发刷新 + 失败回退
- `app/metrics.py` — `MetricsRegistry`：按请求/按 key 的延迟、命中与计算次数统计
- `app/datasource.py` — `SimulatedDataSource`：本地模拟数据源（可控延迟/失败、调用计数）
- `main.py` — HTTP 层：`GET /data/{key}`、`GET /metrics`、`GET /metrics/slow`

## 并发读取语义

- **单飞合并**：同一 key 同一时刻最多只有一次真实计算；并发等待方共享同一个结果。
- **失败一致性**：计算失败时，所有等待方拿到**同一个异常实例**；失败结果绝不写入缓存，后续读取会重新计算。
- **key 间隔离**：不同 key 的计算互不干阻塞（仅共享一把极短临界区的锁用于登记）。
- **结果口径**：`HotDataCache.fetch(key)` 返回 `(value, outcome)`，`outcome ∈ {hit, miss_compute, stale}`，HTTP 层据此记录每次请求的真实命中情况；`get(key)` 是只返回值便捷包装。

## 过期与回退规则

- 命中未过期数据：直接返回（hit）。
- 过期边界：以 `clock() - stored_at < ttl` 判定，到达边界即视为过期。
- **stale-while-revalidate**：过期后第一个到达的读请求触发**后台**刷新，刷新期间所有读取继续拿到旧值；同一 key 同一时刻只存在一次刷新，不会雪崩。
- **失败回退**：刷新失败时旧值继续可用（不会被失败结果覆盖）；没有旧值（冷启动）时异常直接抛给所有等待方。
- **负缓存**：数据不存在（`DataNotFoundError`）会缓存 `negative_ttl` 秒，防止穿透；数据源短暂异常（其他异常）**不缓存**，恢复后立即可读。两者借此明确区分。

## 观测指标口径

`GET /metrics` 按 key 输出：

| 字段 | 含义 |
|---|---|
| `hits` | 未过期命中次数 |
| `misses` | 触发计算的读请求数 |
| `computes` | 实际计算次数（单飞合并后 `computes <= misses`），与数据源真实调用次数一致 |
| `compute_failures` | 计算失败次数（含数据不存在与数据源异常） |
| `stale_serves` | 提供过期旧值的次数 |
| `avg_latency_ms` / `max_latency_ms` | 按 key 的延迟统计 |

`GET /metrics/slow` 输出慢请求环形日志（默认容量 1000，阈值 100ms），每条含 `request_id`、`key`、`latency_ms`、`outcome`，可按 key 定位慢请求。

**内存控制**：key 统计表上限 `max_keys`（溢出计入 `__overflow__`），慢日志为定长 `deque`，缓存条目上限 `max_entries`（逐出最旧），长时间运行内存有界。所有计数在单把锁内更新，高并发下不失真。

## 本地验证

```bash
uv run pytest tests/ -q          # 全部测试
uv run pytest tests/ -q -s --log-cli-level=INFO   # 查看各用例打印的关键输入与判定依据
```

覆盖场景：冷启动、并发读取合并、key 间不阻塞、失败一致传播且不缓存、过期单飞刷新、
刷新失败回退旧值、负缓存、长时间运行内存有界、观测计数与真实调用一致、HTTP 层 404/503 语义。

手动体验：

```bash
uv run uvicorn main:app --port 8000
curl localhost:8000/data/hot
curl localhost:8000/metrics
```
