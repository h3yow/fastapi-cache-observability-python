"""共享类型定义：读取结果、缓存条目、状态枚举与可区分的异常类型。"""

from __future__ import annotations

import enum
from dataclasses import dataclass, field


class DataNotFoundError(KeyError):
    """数据本身不存在（确定性结果，可被短暂负缓存）。"""


class DataSourceUnavailableError(RuntimeError):
    """数据源短暂异常（非确定性，不允许写入缓存）。"""


class ReadStatus(str, enum.Enum):
    """一次读取的最终状态，用于结果判定与观测口径。"""

    MISS_COMPUTED = "miss_computed"        # 冷启动/未命中，本次实际计算
    HIT = "hit"                            # 命中未过期缓存
    COALESCED = "coalesced"                # 并发合并：等待了他人的计算结果
    STALE_REFRESHED = "stale_refreshed"    # 过期后由本请求完成刷新
    STALE_HIT = "stale_hit"                # 过期但已有刷新在进行，直接返回旧值
    FALLBACK_STALE = "fallback_stale"      # 刷新失败，回退到上一份有效结果
    NOT_FOUND = "not_found"                # 数据不存在（负缓存或即时判定）
    ERROR = "error"                        # 数据源异常且无旧值可回退


@dataclass(frozen=True)
class ReadResult:
    key: str
    value: object | None
    status: ReadStatus
    stale: bool
    compute_ms: float          # 本请求实际承担的数据源耗时（等待他人时为 0）
    wait_ms: float             # 因并发合并而等待的耗时
    version: int               # 数据来源条目的版本号，用于断言结果一致性


@dataclass
class CacheEntry:
    value: object
    version: int
    stored_at: float
    expires_at: float
    last_ok_at: float

    def is_fresh(self, now: float) -> bool:
        return now < self.expires_at
