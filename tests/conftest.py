import pytest

from app.cache import HotDataCache
from app.datasource import SimulatedDataSource
from app.metrics import MetricsRegistry


@pytest.fixture
def metrics() -> MetricsRegistry:
    return MetricsRegistry()


@pytest.fixture
def source() -> SimulatedDataSource:
    return SimulatedDataSource()


@pytest.fixture
def make_cache(metrics):
    def _make(**kwargs) -> HotDataCache:
        kwargs.setdefault("metrics", metrics)
        return HotDataCache(**kwargs)

    return _make
