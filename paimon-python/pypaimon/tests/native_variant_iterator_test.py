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

"""Native Variant projections retain the public InternalRow iterator contract."""

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.data.generic_variant import GenericVariant
from pypaimon.execution.native.read.table_read import native_read
from pypaimon.execution.native.runtime import native_runtime_available
from pypaimon.execution.python.read.table_read import PythonTableRead
from pypaimon.schema.arrow_schema import read_arrow_schema
from pypaimon.table.row.row_kind import RowKind


pytestmark = pytest.mark.skipif(not native_runtime_available(), reason='pypaimon-rust not installed')


@pytest.fixture(params=['append', 'pk', 'de'])
def variant_table(tmp_path, request):
    mode = request.param
    payload = GenericVariant.to_arrow_array([
        GenericVariant.from_python({'score': 1.25, 'good': 2.5}),
        GenericVariant.from_python({'score': 'bad', 'good': 3.5}),
        GenericVariant.from_python({}), None,
        GenericVariant.from_python({'score': None}), None,
    ])
    options = {
        'bucket': '1' if mode == 'pk' else '-1',
        'file.format': 'parquet', 'read.batch-size': '2',
        'source.split.target-size': '1b',
        'read.native.enabled': 'true', 'scan.native-plan.enabled': 'false',
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
    }
    if mode == 'de':
        options.update({'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true'})
    data = pa.Table.from_arrays([
        pa.array(range(1, 7), type=pa.int64()), payload,
        pa.StructArray.from_arrays([payload], names=['payload'], mask=pa.array([False] * 5 + [True])),
    ], names=['id', 'payload', 'profile'])
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', True)
    catalog.create_table('db.t', Schema.from_pyarrow_schema(
        data.schema, primary_keys=['id'] if mode == 'pk' else None, options=options), False)
    table = catalog.get_table('db.t')
    for offset in range(0, data.num_rows, 2):
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(data.slice(offset, 2))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
    with patch.object(PythonTableRead, 'read_rows', side_effect=AssertionError('Python fallback')), \
            patch.object(PythonTableRead, 'read_batches', side_effect=AssertionError('Python fallback')):
        yield table


@pytest.fixture(params=['payload', 'profile.payload'])
def variant_builder(variant_table, request):
    source = request.param
    builder = variant_table.new_read_builder().with_projection({
        'score': "try_variant_get(%s, '$.score', 'float')" % source,
        'identifier': 'id',
        'score_copy': "try_variant_get(%s, '$.score', 'float')" % source,
        'good': "variant_get(%s, '$.good', 'float')" % source,
        'identifier_copy': 'id',
    })
    return builder, source


def _rows(iterator):
    try:
        result = []
        for row in iterator:
            assert row.get_row_kind() == RowKind.INSERT
            result.append(tuple(row.get_field(index) for index in range(len(row))))
        return result
    finally:
        iterator.close()


@pytest.mark.parametrize('limit', [None, 0, 1, 3])
def test_variant_iterator_matches_arrow(variant_builder, limit):
    builder, _ = variant_builder
    splits = builder.new_scan().plan().splits()
    assert len(splits) >= 2
    if limit is not None:
        builder.with_limit(limit)
    read = builder.new_read()
    expected = [(1.25, 1, 1.25, 2.5, 1), (None, 2, None, 3.5, 2)]
    expected += [(None, value, None, None, value) for value in range(3, 7)]
    if limit is not None:
        expected = expected[:limit]
    assert _rows(read.to_iterator(splits)) == expected
    arrow = read.to_arrow(splits, parallelism=1)
    assert arrow.column_names == [alias for alias, _ in read.output_projection.columns]
    assert arrow.schema.field('score').type == pa.float32()
    assert list(zip(*(column.to_pylist() for column in arrow.columns))) == expected
    with read.to_arrow_batch_reader(splits, parallelism=1) as batches:
        assert [tuple(row.values()) for batch in batches for row in batch.to_pylist()] == expected
    assert _rows(read.to_iterator([])) == []
    assert read.include_row_kind is False


def test_variant_iterator_propagates_strict_cast_errors(variant_builder):
    builder, source = variant_builder
    builder.with_projection({'score': "variant_get(%s, '$.score', 'float')" % source})
    splits = builder.new_scan().plan().splits()
    read = builder.new_read()
    with pytest.raises(Exception, match='Cannot cast Variant'):
        read.to_arrow(splits, parallelism=1)
    with pytest.raises(Exception, match='Cannot cast Variant'):
        _rows(read.to_iterator(splits))


@pytest.mark.parametrize('failure', ['setup', 'unsupported_split'])
def test_variant_iterator_never_falls_back_to_python(variant_builder, failure):
    builder, _ = variant_builder
    splits = builder.new_scan().plan().splits()
    target, options = (
        ('native_read', {'side_effect': RuntimeError('Native setup failed')}) if failure == 'setup' else
        ('NativeTableRead._split_files_supported', {'return_value': False}))
    with patch('pypaimon.execution.native.read.table_read.' + target, **options):
        with pytest.raises(RuntimeError, match='Variant extraction requires a compatible native reader'):
            _rows(builder.new_read().to_iterator(splits))


@pytest.mark.parametrize('stop', ['unstarted', 'close', 'failure', 'exhaust'])
def test_variant_iterator_batch_lifetime(variant_builder, stop):
    fetched, closed = [], []
    failure = OSError('Native stream failed')

    def tracked_native(*args, **kwargs):
        batches = native_read(*args, **kwargs)

        def tracked_batches():
            try:
                for batch in batches:
                    fetched.append(batch.num_rows)
                    yield batch
                    if stop == 'failure':
                        raise failure
            finally:
                batches.close()
                closed.append(True)

        return tracked_batches()

    builder, _ = variant_builder
    splits = builder.new_scan().plan().splits()
    with patch('pypaimon.execution.native.read.table_read.native_read', side_effect=tracked_native) as native:
        iterator = builder.new_read().to_iterator(splits)
        native.assert_not_called()
        try:
            if stop != 'unstarted':
                assert next(iterator).get_field(0) == 1.25
                assert len(fetched) == 1
                assert closed == []
                if stop == 'failure':
                    with pytest.raises(OSError) as raised:
                        list(iterator)
                    assert raised.value is failure
                elif stop == 'exhaust':
                    assert len(list(iterator)) == 5
        finally:
            iterator.close()
    assert native.call_count == int(stop != 'unstarted')
    assert closed == ([] if stop == 'unstarted' else [True])
    if stop in ('close', 'failure'):
        assert len(fetched) == 1
    if stop == 'exhaust':
        assert sum(fetched) == 6


def test_variant_iterator_preserves_row_kinds(variant_builder):
    builder, path = variant_builder
    read = builder.new_read()
    source = {'0': 1.25, '1': 1.25, '2': 2.5}
    field = path.split('.')[0]
    payload = {'payload': source} if field == 'profile' else source
    kinds = ['+I', '-U', '+U', '-D']
    batch = pa.RecordBatch.from_pylist([
        {'rowkind': kind, field: payload, 'id': index + 1} for index, kind in enumerate(kinds)
    ], schema=pa.schema([pa.field('rowkind', pa.string())] + list(read_arrow_schema(read.read_type))))
    splits = builder.new_scan().plan().splits()
    with patch('pypaimon.execution.native.read.table_read.native_read', return_value=iter([batch])):
        actual = [(tuple(row.get_field(index) for index in range(len(row))), row.get_row_kind())
                  for row in read.to_iterator(splits)]
    assert actual == [((1.25, index + 1, 1.25, 2.5, index + 1), RowKind.from_string(kind))
                      for index, kind in enumerate(kinds)]
