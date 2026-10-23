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

"""Public factories retain captured inputs and enter their selected dispatch boundary."""

import subprocess
import sys
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.table.source.batch_vector_search_builder import BatchVectorSearchBuilderImpl
from pypaimon.table.source.full_text_search_builder import FullTextSearchBuilderImpl
from pypaimon.table.source.vector_search_builder import VectorSearchBuilderImpl


def _search_table(path, native):
    catalog = CatalogFactory.create({'warehouse': str(path)})
    catalog.create_database('default', True)
    catalog.create_table('default.search', Schema.from_pyarrow_schema(
        pa.schema([('embedding', pa.list_(pa.float32())), ('body', pa.string())]),
        options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
                 'read.native.enabled': str(native).lower()}), False)
    return catalog.get_table('default.search')


@pytest.mark.parametrize('kind', ['vector', 'batch-vector', 'full-text'])
@pytest.mark.parametrize('phase', ['scan', 'read'])
@pytest.mark.parametrize('native', [False, True])
def test_search_factories_keep_captured_configuration(tmp_path, kind, phase, native):
    from pypaimon.table.source.search_dispatch import python_search_reader

    table = _search_table(tmp_path, native)
    query = [1.0, 2.0]
    if kind == 'vector':
        builder = (VectorSearchBuilderImpl(table).with_vector_column('embedding')
                   .with_query_vector(query).with_limit(2).with_option('ivf.nprobe', '1'))
    elif kind == 'batch-vector':
        builder = (BatchVectorSearchBuilderImpl(table).with_vector_column('embedding')
                   .with_query_vectors([query]).with_limit(2).with_option('ivf.nprobe', '1'))
    else:
        builder = FullTextSearchBuilderImpl(table).with_query('body', 'term').with_limit(2)
    family = 'full_text' if kind == 'full-text' else 'vector'
    python_name = ('new_batch_vector_read' if kind == 'batch-vector' and phase == 'read'
                   else 'new_' + family + '_' + phase)
    native_name = 'native_' + family + '_' + phase
    python_execution, native_execution = Mock(), Mock()
    with patch('pypaimon.execution.python.read.search.factories.' + python_name,
               return_value=python_execution) as python_factory, \
            patch('pypaimon.execution.native.read.native_search_plan.' + native_name,
                  return_value=native_execution) as native_factory:
        if phase == 'scan':
            selected = (builder.new_full_text_scan() if kind == 'full-text'
                        else builder.new_vector_search_scan())
        else:
            selected = (builder.new_full_text_read() if kind == 'full-text'
                        else builder.new_batch_vector_search_read() if kind == 'batch-vector'
                        else builder.new_vector_search_read())
        if native:
            python_factory.assert_not_called()
            native_factory.assert_called_once()
        else:
            native_factory.assert_not_called()
        builder.with_limit(9)
        if kind == 'full-text':
            builder.with_query('body', 'later')
        else:
            query[0] = 99.0
            builder.with_option('ivf.nprobe', '9')
        if phase == 'scan':
            native_execution.available.return_value = False
            assert selected.scan() is python_execution.scan.return_value
        else:
            assert python_search_reader(selected) is python_execution
        python_factory.assert_called_once()
    captured = python_factory.call_args[0][0]
    assert captured is not builder and captured._limit == 2
    if kind == 'full-text':
        assert captured._query == 'term'
    else:
        assert captured._options == {'ivf.nprobe': '1'}
        if phase == 'read':
            assert (captured._query_vector if kind == 'vector' else captured._query_vectors[0]) == [1.0, 2.0]


def test_native_search_factories_do_not_import_python_execution(tmp_path):
    source = r"""
import importlib.abc
import sys
from unittest.mock import Mock, patch
import pyarrow as pa

class RejectPythonExecution(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.startswith('pypaimon.execution.python'):
            raise AssertionError('Native factory imported Python execution: ' + fullname)

sys.meta_path.insert(0, RejectPythonExecution())
from pypaimon import CatalogFactory, Schema
from pypaimon.table.source.vector_search_builder import VectorSearchBuilderImpl
from pypaimon.table.source.batch_vector_search_builder import BatchVectorSearchBuilderImpl
from pypaimon.table.source.full_text_search_builder import FullTextSearchBuilderImpl
from pypaimon.execution.native.read import native_search_plan

catalog = CatalogFactory.create({'warehouse': sys.argv[1]})
catalog.create_database('default', True)
catalog.create_table('default.search', Schema.from_pyarrow_schema(
    pa.schema([('embedding', pa.list_(pa.float32())), ('body', pa.string())]),
    options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
             'read.native.enabled': 'true'}), False)
table = catalog.get_table('default.search')
vector = (VectorSearchBuilderImpl(table).with_vector_column('embedding')
          .with_query_vector([1., 2.]).with_limit(2))
batch = (BatchVectorSearchBuilderImpl(table).with_vector_column('embedding')
         .with_query_vectors([[1., 2.]]).with_limit(2))
text = FullTextSearchBuilderImpl(table).with_query('body', 'term').with_limit(2)
native = Mock()
native.available.return_value = True
expected = object()
for name in ['scan', 'read_plan', 'read_batch_plan']:
    getattr(native, name).return_value = expected
constructors = {name: Mock(return_value=native) for name in [
    'native_vector_scan', 'native_vector_read', 'native_full_text_scan', 'native_full_text_read']}
with patch.multiple(native_search_plan, **constructors):
    operations = [
        lambda: vector.new_vector_search_scan().scan(),
        lambda: vector.new_vector_search_read().read_plan(object()),
        lambda: batch.new_vector_search_scan().scan(),
        lambda: batch.new_batch_vector_search_read().read_batch_plan(object()),
        lambda: text.new_full_text_scan().scan(),
        lambda: text.new_full_text_read().read_plan(object()),
    ]
    for operation in operations:
        assert operation() is expected
    failure = OSError('selected native executor failed')
    for name in ['scan', 'read_plan', 'read_batch_plan']:
        getattr(native, name).side_effect = failure
    for operation in operations:
        try:
            operation()
        except OSError as error:
            assert error is failure
        else:
            raise AssertionError('Native failure was hidden')
assert not any(name.startswith('pypaimon.execution.python') for name in sys.modules)
"""
    result = subprocess.run(
        [sys.executable, '-c', source, str(tmp_path)], stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, universal_newlines=True, timeout=45)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize('bucket,primary_keys,expected', [
    ('2', ['id', 'p'], 'FixedBucketRowKeyExtractor'),
    ('-1', [], 'UnawareBucketRowKeyExtractor'),
    ('-2', ['id', 'p'], 'PostponeBucketRowKeyExtractor'),
    ('-1', ['id', 'p'], 'DynamicBucketRowKeyExtractor'),
])
def test_public_row_key_factory_preserves_per_row_contract(tmp_path, bucket, primary_keys, expected):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    arrow_schema = pa.schema([('id', pa.int32()), ('p', pa.string())])
    catalog.create_table('default.keys', Schema.from_pyarrow_schema(
        arrow_schema, primary_keys=primary_keys, partition_keys=['p'], options={
            'bucket': bucket, 'write.native.enabled': 'true', 'read.native.enabled': 'false',
            'scan.native-plan.enabled': 'false', 'commit.native.enabled': 'false'}), False)
    table = catalog.get_table('default.keys')
    rows = pa.record_batch([pa.array([1, 2], pa.int32()), pa.array(['a', 'a'])], schema=arrow_schema)
    with patch('pypaimon.execution.native.runtime.native_runtime_available',
               side_effect=AssertionError('Per-row helper rediscovered the Native writer')):
        extractor = table.create_row_key_extractor(ignore_existing=True)
        partitions, buckets = extractor.extract_partition_bucket_batch(rows)
    assert type(extractor).__name__ == expected
    assert partitions == [('a',), ('a',)]
    if bucket == '2':
        assert all(0 <= value < 2 for value in buckets)
    else:
        assert buckets == [-2 if bucket == '-2' else 0] * 2


def test_ray_condition_compatibility_helpers_keep_python_execution():
    if sys.version_info < (3, 10):
        pytest.skip('Python MERGE conditions require Python 3.10')
    pytest.importorskip('datafusion')
    from pypaimon.ray.merge_condition import apply_condition, filter_batch, rewrite_condition

    rows = pa.table({'s.id': pa.chunked_array([[1, 2], [3]]), 't.id': [2, 1, 2]})
    with patch('pypaimon.execution.native.runtime.native_runtime_available',
               side_effect=AssertionError('Python condition helper discovered Native execution')):
        assert filter_batch(rows, 's.id > t.id').column('s.id').to_pylist() == [2, 3]
        schema = pa.schema([('s.id', pa.int64())])
        empty = apply_condition(rows, rewrite_condition('s.id < 0'), schema)
    assert empty.num_rows == 0 and empty.schema == schema
