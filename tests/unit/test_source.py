from collections.abc import Iterator
from itertools import batched

import pandas as pd
import pyarrow as pa
import pytest

from viadot.sources.base import Source


class FakeSource(Source):
    def __init__(self, *, native: bool):
        super().__init__()
        self.supports_batch_size = native
        self.requested_batch_sizes: list[int | None] = []

    def to_json(self) -> dict:
        return {}

    def query(self) -> list[tuple]:
        return []

    def _to_records(
        self, batch_size: int | None = None
    ) -> Iterator[tuple[dict[str, int], ...]]:
        self.requested_batch_sizes.append(batch_size)
        records = [{"value": value} for value in range(5)]
        if batch_size is None:
            yield records
            return
        yield from batched(records, batch_size)


class SourceWithLargeDefault(FakeSource):
    supports_batch_size = True

    def _to_records(
        self, batch_size: int | None = 100_000
    ) -> Iterator[tuple[dict[str, int], ...]]:
        return super()._to_records(batch_size=batch_size)


class EmptySource(FakeSource):
    def _to_records(self, batch_size: int | None = None):
        return None


@pytest.mark.parametrize("native", [True, False])
def test_to_arrow_explicit_none_is_materialized(native):
    source = FakeSource(native=native)

    result = source.to_arrow(batch_size=None)

    assert isinstance(result, pa.Table)
    assert result.num_rows == 5
    assert source.requested_batch_sizes == [None]


def test_to_records_explicit_none_is_materialized():
    source = FakeSource(native=False)

    result = source.to_records(batch_size=None)

    assert result == [{"value": value} for value in range(5)]
    assert source.requested_batch_sizes == [None]


def test_to_records_handles_empty_source():
    assert EmptySource(native=False).to_records(batch_size=None) == []


def test_native_to_arrow_omitted_batch_size_is_lazy():
    source = FakeSource(native=True)

    result = source.to_arrow()

    assert not isinstance(result, pa.Table)
    assert [batch.num_rows for batch in result] == [5]
    assert source.requested_batch_sizes == [20_000]


def test_non_native_to_arrow_omitted_batch_size_uses_global_default():
    source = FakeSource(native=False)

    result = source.to_arrow()

    assert [batch.num_rows for batch in result] == [5]
    assert source.requested_batch_sizes == [20_000]


def test_non_native_integer_batch_size_is_generically_rebatched():
    source = FakeSource(native=False)

    result = source.to_arrow(batch_size=2)

    assert [batch.num_rows for batch in result] == [2, 2, 1]
    assert source.requested_batch_sizes == [2]


def test_connector_default_is_capped_by_global_default():
    source = SourceWithLargeDefault(native=True)

    result = source.to_arrow()

    assert [batch.num_rows for batch in result] == [5]
    assert source.requested_batch_sizes == [20_000]


def test_to_df_preserves_batch_form():
    native = FakeSource(native=True)
    non_native = FakeSource(native=False)

    native_result = native.to_df(batch_size=2)
    non_native_result = non_native.to_df(batch_size=None)

    assert [len(batch) for batch in native_result] == [2, 2, 1]
    assert isinstance(non_native_result, pd.DataFrame)
    assert len(non_native_result) == 5
