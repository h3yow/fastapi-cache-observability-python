# fastapi-cache-observability-python

在 FastAPI 服务之上提供热点数据读取的性能优化（单飞合并 + 过期并发刷新 + 失败回退）
与运行时观测能力（按请求 / 按 key 的延迟、命中与计算次数）。

## 结构

```
app/
  models.py      读取结果、缓存条目、状态枚举、可区分异常（DataNotFoundError / DataSourceUnavailableError）
  cache.py       HotDataCache：单飞合并、过期刷新、负缓存、失败回退
  metrics.py     MetricsRegistry：线程安全、有界的按 key 与全局观测
  datasource.py  SimulatedDataSource：本地模拟数据源（延迟 / 异常注入 / 调用计数）
  api.py         FastAPI 端点：数据读取、观测导出、数据源管理
tests/           冷启动、并发读取、过期刷新、数据源异常、长时间运行等场景
```

## 并发读取语义（单飞合并）

- **冷启动（无任何缓存值）**：同一 key 的第一个请求成为 leader 实际调用数据源，
  其余并发请求挂到同一个 flight 上等待，全部拿到**同一份结果**（状态 `coalesced`）。
  实际计算只发生一次；不同 key 之间无任何共享锁，互不阻塞。
- **失败一致性**：leader 计算失败时，所有等待方拿到一致的失败结果
  （数据源异常 → `error`；数据不存在 → `not_found`）。失败结果**不写入任何缓存**，
  数据源恢复后下一次读取立即重新计算。
- **过期刷新（有旧值）**：持锁者同步刷新（`stale_refreshed`），刷新期间到达的请求
  **立即返回旧值**（`stale_hit`，`stale=true`），不阻塞、不重复刷新，杜绝雪崩。
- **结果不互相覆盖**：每个条目带单调递增版本号，只有持锁者能写入，
  刷新完成后任何读取都不会再看到旧版本。

## 过期与回退规则

| 场景 | 行为 | 状态 |
| --- | --- | --- |
| 未过期 | 直接返回 | `hit` |
| 过期、无刷新进行中 | 本请求同步刷新，版本 +1 | `stale_refreshed` |
| 过期、刷新进行中 | 旧值继续可用，立即返回 | `stale_hit` |
| 刷新时数据源异常 | 回退上一份有效结果，过期时间延长 `retry_after_seconds`（退避，避免异常期雪崩） | `fallback_stale` |
| 刷新时发现数据已删除 | 旧值一并清除，写入短期负缓存 | `not_found` |
| 冷启动数据源异常 | 不写缓存，直接报错 | `error` |
| 数据不存在 | 写入 `negative_ttl_seconds` 负缓存，期间不再访问数据源 | `not_found` |

`not_found`（数据本身不存在，确定性）与 `error`（数据源短暂异常，非确定性）始终可区分。

## 观测指标口径

`GET /metrics`（全局 + 各 key）、`GET /metrics/{key}`（单 key）、`GET /cache/stats`（缓存内部状态）。

- `requests`：读取请求总数；`hits`：命中数（含 `hit` / `stale_hit` / `fallback_stale`）；
- `computes`：**实际调用数据源成功的次数**（`miss_computed` + `stale_refreshed`）；
- `coalesced`：因单飞合并而等待的次数；`errors`：数据源失败次数（含 `fallback_stale` 背后的失败）；
- `not_found`：判定数据不存在的次数；
- `total/avg/max_latency_ms`：按 key 的延迟分布，用于定位慢请求。
- **恒等式**：数据源真实调用次数 = `computes` + `errors` + `not_found` 中实际触达数据源的部分
  （负缓存命中不触达）。测试中以 `source.call_count == computes + errors + not_found`（无负缓存命中时）断言。
- 所有计数在单把锁内累加，多线程 / 高并发下不失真；key 维度表与缓存条目均有上限
  （`max_keys` / `max_entries`），超出按最近访问淘汰，长时间运行内存有界，淘汰不影响全局聚合。

## 本地验证

```bash
uv run pytest            # 全部场景：冷启动/并发/过期/异常/长运行，日志打印关键输入与判定依据
uv run python main.py    # 启动服务（随机端口）
```

HTTP 端点：

```
GET  /data/{key}          读取（返回 status/stale/version/compute_ms/wait_ms）
GET  /metrics             全局 + 各 key 观测快照
GET  /metrics/{key}       单 key 观测
GET  /cache/stats         缓存条目 / 负缓存 / 单飞 / 刷新锁数量
PUT  /source/data/{key}   写入模拟数据源
DELETE /source/data/{key} 从模拟数据源删除
POST /source/config       {"latency_seconds": 0.1, "fail_next": 2, "flaky": true}
POST /metrics/reset       重置观测与调用计数
```
