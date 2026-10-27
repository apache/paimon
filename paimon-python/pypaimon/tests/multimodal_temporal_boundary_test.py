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

"""Temporal schema and cache preparation keep reader execution independent."""

import pickle
import subprocess
import sys

import pyarrow as pa
import pytest


def test_importing_multimodal_does_not_load_python_execution():
    result = subprocess.run([sys.executable, '-B', '-c', '''
import sys
import pypaimon.multimodal
loaded = [name for name in sys.modules if name.startswith('pypaimon.execution.python.')]
assert not loaded, loaded
'''], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_selected_native_batches_do_not_initialize_the_python_cache():
    result = subprocess.run([sys.executable, '-B', '-c', '''
import sys
from types import SimpleNamespace
from unittest.mock import patch
import pyarrow as pa
from pypaimon.read.read_dispatch import to_arrow
from pypaimon.read.read_support import _ParquetRowGroupCacheRequest
schema = pa.schema([('value', pa.int32())])
batch = pa.record_batch([pa.array([7], type=pa.int32())], schema=schema)
request = _ParquetRowGroupCacheRequest(1024)
read = SimpleNamespace(
    include_row_kind=False,
    _parquet_row_group_cache=request,
    _output_arrow_schema=lambda splits: schema,
    _resolve_parallelism=lambda value, count: 1,
    _resolve_blob_parallelism=lambda value: 1,
    _batches_to_arrow=lambda batches, output_schema: pa.Table.from_batches(batches, schema=output_schema))
with patch('pypaimon.read.read_dispatch._native_read_execution',
           return_value=SimpleNamespace(read_batches=lambda *args, **kwargs: [batch])):
    assert to_arrow(read, [object()]).to_pydict() == {'value': [7]}
assert request._cache is None
loaded = [name for name in sys.modules if name.startswith('pypaimon.execution.python.')]
assert not loaded, loaded
'''], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize('data_type,transform', [
    (pa.int32(), {'name': 'NULL'}),
    (pa.int32(), {'name': 'FIELD_REF', 'fieldRef': {'name': 'value'}}),
    (pa.int32(), {'name': 'CAST', 'fieldRef': {'name': 'value', 'type': 'INT'}, 'type': 'STRING'}),
    (pa.string(), {'name': 'UPPER', 'inputs': [{'name': 'value', 'type': 'STRING'}]}),
    (pa.string(), {'name': 'UPPER', 'inputs': [42]}),
    (pa.large_string(), {'name': 'LOWER', 'inputs': [{'name': 'value', 'type': 'STRING'}]}),
    (pa.string(), {'name': 'CONCAT', 'inputs': [{'name': 'value'}, 'tail']}),
    (pa.large_string(), {'name': 'CONCAT_WS', 'inputs': ['-', {'name': 'value'}]}),
    (pa.large_string(), {'name': 'SUBSTRING', 'inputs': [{'name': 'value'}, 1, 2]}),
    (pa.large_string(), {'name': 'SUBSTRING', 'inputs': [{'name': 'value'}, -1, 2]}),
    (pa.large_string(), {'name': 'TRIM', 'trimFlag': 'BOTH', 'inputs': [{'name': 'value', 'type': 'STRING'}]}),
    (pa.large_string(), {'name': 'LENGTH', 'inputs': [{'name': 'value', 'type': 'STRING'}]}),
    (pa.large_string(), {'name': 'BIT_LENGTH', 'inputs': [{'name': 'value', 'type': 'STRING'}]}),
    (pa.large_string(), {'name': 'PAD', 'direction': 'LEFT', 'inputs': [{'name': 'value', 'type': 'STRING'}, 4, 'x']}),
    (pa.large_string(), {'name': 'TRANSLATE', 'inputs': [{'name': 'value', 'type': 'STRING'}, 'a', 'b']}),
    (pa.large_string(), {'name': 'OVERLAY', 'inputs': [{'name': 'value', 'type': 'STRING'}, 'x', 1]}),
    (pa.date32(), {'name': 'YEAR', 'fieldRef': {'name': 'value', 'type': 'DATE'}}),
])
def test_temporal_mask_schema_matches_actual_python_mask(data_type, transform):
    from pypaimon.common.transform_schema import effective_masked_schema
    from pypaimon.common.arrow_predicate import _apply_predicate_transform

    schema = pa.schema([pa.field('value', data_type, nullable=False, metadata={b'field': b'value'})],
                       metadata={b'schema': b'value'})
    batch = pa.RecordBatch.from_arrays([pa.array([], type=data_type)], schema=schema)
    actual = _apply_predicate_transform(transform, batch, null_type=data_type)
    inferred = effective_masked_schema(schema, {'value': transform})
    assert inferred.field('value').type == actual.type
    assert inferred.field('value').nullable
    assert inferred.field('value').metadata == schema.field('value').metadata
    assert inferred.metadata == schema.metadata


@pytest.mark.parametrize('transform', [
    {'name': 'unknown'},
    {'name': 'FIELD_REF', 'fieldRef': {'name': 'missing'}},
    {'name': 'CAST', 'fieldRef': {'name': 'value', 'type': 'STRING'}, 'type': 'INVALID'},
    {'name': 'SUBSTRING', 'inputs': ['value']},
    {'name': 'TRIM', 'trimFlag': 'INVALID', 'inputs': ['value']},
    {'name': 'PAD', 'direction': 'INVALID', 'inputs': ['value', 4, 'x']},
    {'name': 'LENGTH', 'inputs': [{'name': 'value', 'type': 'INT'}]},
])
def test_temporal_mask_schema_rejects_invalid_rules(transform):
    from pypaimon.common.transform_schema import effective_masked_schema

    with pytest.raises((ValueError, KeyError, TypeError)):
        effective_masked_schema(pa.schema([('value', pa.string())]), {'value': transform})


def test_temporal_cache_is_created_only_for_python_and_recreated_after_transport():
    from pypaimon.read.read_support import _ParquetRowGroupCacheRequest
    from pypaimon.execution.python.read.table_read import PythonTableRead

    request = _ParquetRowGroupCacheRequest(1024)
    assert request._cache is None
    cached = PythonTableRead._resolve_row_group_cache(request)
    assert PythonTableRead._resolve_row_group_cache(request) is cached
    restored = pickle.loads(pickle.dumps(request))
    assert restored._cache is None
    assert PythonTableRead._resolve_row_group_cache(restored) is not cached


def test_python_split_readers_share_decoded_cache_without_public_dispatch():
    from types import SimpleNamespace
    from unittest.mock import Mock, patch
    from pypaimon.read.read_support import _ParquetRowGroupCacheRequest
    from pypaimon.execution.python.read.table_read import PythonTableRead

    request = _ParquetRowGroupCacheRequest(1024)
    execution = PythonTableRead(SimpleNamespace(_parquet_row_group_cache=request))
    execution._build_split_read = Mock(side_effect=lambda *args: SimpleNamespace())
    with patch.dict(sys.modules, {'pypaimon.read.read_dispatch': None}):
        first = execution._create_split_read(object())
        second = execution._create_split_read(object())
    assert first._parquet_row_group_cache is second._parquet_row_group_cache
    assert first._parquet_row_group_cache is request._cache
