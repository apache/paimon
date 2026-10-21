# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Ray datasource facades preserve hooks and own retained worker readers."""

from pathlib import Path
import pickle
import subprocess
import sys
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.read.datasource.ray_datasource import RayDatasource
from pypaimon.read.datasource.split_provider import CatalogSplitProvider, PreResolvedSplitProvider


def _provider():
    fields = Schema.from_pyarrow_schema(pa.schema([('id', pa.int64())])).fields
    split = SimpleNamespace(file_size=10, file_paths=['file.parquet'],
                            merged_row_count=lambda: 2)
    table = SimpleNamespace(is_primary_key_table=False, identifier='default.data')
    return PreResolvedSplitProvider(table, [split], fields)


class _RetainedReader:

    def __init__(self, batches, failure=None, close_failure=None):
        self.batches = iter(batches)
        self.failure = failure
        self.close_failure = close_failure
        self.closes = 0

    def read_next_batch(self):
        try:
            return next(self.batches)
        except StopIteration:
            if self.failure is not None:
                raise self.failure
            raise

    def close(self):
        self.closes += 1
        if self.close_failure is not None:
            raise self.close_failure


def _task(reader):
    task = RayDatasource(_provider()).get_read_tasks(1)[0]
    factory = Mock()
    factory.return_value.to_arrow_batch_reader.return_value = reader
    return task, factory


@pytest.mark.parametrize('empty', [False, True])
def test_ray_worker_closes_retained_reader_on_complete(empty):
    batch = pa.record_batch([pa.array([] if empty else [1, 2], pa.int64())], names=['id'])
    reader = _RetainedReader([batch])
    task, factory = _task(reader)
    with patch('pypaimon.read.table_read.TableRead', factory):
        blocks = list(task())
    assert len(blocks) == 1
    assert blocks[0].to_pydict() == {'id': [] if empty else [1, 2]}
    assert blocks[0].schema == batch.schema
    assert reader.closes == 1
    factory.return_value.to_arrow_batch_reader.assert_called_once()


@pytest.mark.parametrize('close_fails', [False, True])
def test_ray_worker_cancel_closes_retained_reader(close_fails):
    batch = pa.record_batch([pa.array([1, 2], pa.int64())], names=['id'])
    close_error = RuntimeError('reader close failed') if close_fails else None
    reader = _RetainedReader([batch, batch], close_failure=close_error)
    task, factory = _task(reader)
    with patch('pypaimon.read.table_read.TableRead', factory):
        output = iter(task())
        assert next(output).to_pydict() == {'id': [1, 2]}
        if close_fails:
            with pytest.raises(RuntimeError) as caught:
                output.close()
            assert caught.value is close_error
        else:
            output.close()
    assert reader.closes == 1


def test_ray_worker_read_error_survives_retained_reader_close_error():
    batch = pa.record_batch([pa.array([1, 2], pa.int64())], names=['id'])
    error = RuntimeError('retained stream failed')
    reader = _RetainedReader([batch], failure=error,
                             close_failure=RuntimeError('reader close failed'))
    task, factory = _task(reader)
    with patch('pypaimon.read.table_read.TableRead', factory):
        output = iter(task())
        assert next(output).to_pydict() == {'id': [1, 2]}
        with pytest.raises(RuntimeError) as caught:
            next(output)
    assert caught.value is error
    assert reader.closes == 1


def test_ray_worker_cast_error_survives_retained_reader_close_error():
    batch = pa.record_batch([['not an integer']], names=['id'])
    reader = _RetainedReader([batch], close_failure=RuntimeError('reader close failed'))
    task, factory = _task(reader)
    with patch('pypaimon.read.table_read.TableRead', factory):
        with pytest.raises(pa.ArrowInvalid, match='not an integer'):
            list(task())
    assert reader.closes == 1


def test_catalog_provider_plan_cache_preserves_overridden_table_hook():
    class HookedProvider(CatalogSplitProvider):
        def _ensure_table(self):
            self.table_calls += 1
            return self.overridden_table

    provider = HookedProvider('default.data', {}, predicate=object(), projection=['id'], limit=4)
    provider.table_calls = 0
    provider.overridden_table = object()
    builder = Mock()
    builder.with_filter.return_value = builder
    builder.with_projection.return_value = builder
    builder.with_limit.return_value = builder
    builder._output_projection = object()
    fields, splits = [object()], [object()]
    builder.read_type.return_value = fields
    builder.new_scan.return_value.plan.return_value.splits.return_value = splits

    with patch('pypaimon.read.read_builder.ReadBuilder', return_value=builder) as construct:
        assert provider.splits() is splits
        assert provider.read_type() is fields
        assert provider.output_projection() is builder._output_projection
        assert provider.splits() is splits

    assert provider.table_calls == 1
    construct.assert_called_once_with(provider.overridden_table)
    builder.with_filter.assert_called_once_with(provider._predicate)
    builder.with_projection.assert_called_once_with(['id'])
    builder.with_limit.assert_called_once_with(4)
    builder.new_scan.return_value.plan.assert_called_once_with()


def test_ray_read_tasks_preserve_overridden_split_grouping_and_metadata():
    class HookedDatasource(RayDatasource):
        def _distribute_splits_into_equal_chunks(self, splits, n_chunks):
            self.chunks_calls += 1
            return [list(splits)]

    datasource = HookedDatasource(_provider())
    datasource.chunks_calls = 0
    tasks = datasource.get_read_tasks(1)
    assert datasource.chunks_calls == 1
    assert len(tasks) == 1
    assert tasks[0].metadata.num_rows == 2
    assert tasks[0].metadata.size_bytes == 10
    assert tasks[0].metadata.input_files == ['file.parquet']


def test_public_datasource_configuration_is_lazy_and_pickles_at_original_paths():
    source = r'''
import importlib.abc
import pickle
import sys
from types import SimpleNamespace

class BlockExecution(importlib.abc.MetaPathFinder):
    allow_shared = False

    def find_spec(self, fullname, path=None, target=None):
        if (fullname.startswith(('pypaimon.execution.python', 'pypaimon.execution.native', 'pypaimon_rust'))
                or (not self.allow_shared and fullname.startswith('pypaimon.execution.datasource'))):
            raise AssertionError('Datasource configuration loaded execution: ' + fullname)

guard = BlockExecution()
sys.meta_path.insert(0, guard)
from pypaimon.read.datasource.ray_datasource import RayDatasource
from pypaimon.read.datasource.split_provider import CatalogSplitProvider, PreResolvedSplitProvider

provider = CatalogSplitProvider('default.data', {}, projection=['id'], limit=7)
datasource = RayDatasource(provider)
assert datasource.get_name() == 'PaimonTable(default.data)'
assert provider.predicate() is None
assert provider.limit() == 7
assert provider._table_cached is None
assert provider._splits_cached is None
restored = pickle.loads(pickle.dumps(datasource))
assert type(restored) is RayDatasource
assert type(restored._split_provider) is CatalogSplitProvider
assert restored._split_provider._projection == ['id']
assert restored._split_provider._table_cached is None
try:
    datasource.get_read_tasks(0)
except ValueError:
    pass
else:
    raise AssertionError('Invalid parallelism was accepted')
resolved = PreResolvedSplitProvider(SimpleNamespace(identifier='default.data'), [], [])
assert type(pickle.loads(pickle.dumps(resolved))) is PreResolvedSplitProvider
assert RayDatasource.__module__ == 'pypaimon.read.datasource.ray_datasource'
assert CatalogSplitProvider.__module__ == 'pypaimon.read.datasource.split_provider'

guard.allow_shared = True
import pyarrow as pa
from unittest.mock import Mock, patch
from pypaimon import Schema

fields = Schema.from_pyarrow_schema(pa.schema([('id', pa.int64())])).fields
split = SimpleNamespace(file_size=10, file_paths=['data.parquet'], merged_row_count=lambda: 2)
table = SimpleNamespace(is_primary_key_table=False, identifier='default.data')
datasource = RayDatasource(PreResolvedSplitProvider(table, [split], fields))
tasks = datasource.get_read_tasks(1)
reader = pa.RecordBatchReader.from_batches(pa.schema([('id', pa.int64())]),
                                         [pa.record_batch([[1, 2]], names=['id'])])
factory = Mock()
factory.return_value.to_arrow_batch_reader.return_value = reader
with patch('pypaimon.read.table_read.TableRead', factory):
    assert list(tasks[0]())[0].to_pydict() == {'id': [1, 2]}
assert not any(name.startswith(('pypaimon.execution.python', 'pypaimon.execution.native', 'pypaimon_rust'))
               for name in sys.modules)
'''
    result = subprocess.run(
        [sys.executable, '-c', source],
        cwd=str(Path(__file__).resolve().parents[2]),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr


def test_catalog_provider_pickle_retains_cached_plan_without_selecting_an_executor():
    provider = CatalogSplitProvider('default.data', {}, projection=['id'])
    table = SimpleNamespace(identifier='default.data')
    provider._table_cached = table
    provider._splits_cached = []
    provider._read_type_cached = []
    restored = pickle.loads(pickle.dumps(provider))
    assert type(restored) is CatalogSplitProvider
    assert restored.table().identifier == 'default.data'
    assert restored.splits() == []
    assert restored.read_type() == []
