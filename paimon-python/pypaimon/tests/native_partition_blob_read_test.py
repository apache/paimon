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

"""Partition columns omitted by a write type come from the split when reading."""

from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.execution.native.runtime import (native_runtime_available)
from pypaimon.schema.data_types import AtomicType, DataField, PyarrowFieldParser
from pypaimon.schema.schema_change import SchemaChange


pytestmark = [pytest.mark.python_plan, pytest.mark.python_write, pytest.mark.python_commit]
_ENGINES = [pytest.param(False, marks=pytest.mark.python_read), pytest.param(
    True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
        not native_runtime_available(), reason='pypaimon-rust not installed')])]


def _append(table, start):
    data = {'id': list(range(start, start + 4)), 'p': ['a', 'b', 'a', 'b'],
            'payload': [b'one', b'two', b'three', b'four']}
    data.update({field.name: [None] * 4 for field in table.fields if field.name not in data})
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.with_write_type(['id', 'payload'])
        writer.write_arrow(pa.table(data, schema=PyarrowFieldParser.from_paimon_schema(table.fields)))
        messages = writer.prepare_commit()
        assert all('p' not in file.write_cols for message in messages for file in message.new_files)
        commit.commit(messages)
    finally:
        writer.close()
        commit.close()


def _table(catalog, nullable, historical):
    fields = [DataField(0, 'id', AtomicType('INT')), DataField(1, 'p', AtomicType('STRING', nullable)),
              DataField(2, 'payload', AtomicType('BLOB'))]
    catalog.create_table('default.partition_blobs', Schema(fields=fields, partition_keys=['p'], options={
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'deletion-vectors.enabled': 'true',
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false', 'read.batch-size': '2',
    }), False)
    table = catalog.get_table('default.partition_blobs')
    _append(table, 1)
    if historical:
        catalog.alter_table(table.identifier, [SchemaChange.add_column('extra', AtomicType('INT'))])
        table = catalog.get_table(table.identifier)
        _append(table, 11)
        table = table.copy({'scan.snapshot-id': '1'})
        assert table.table_schema.id == 0
    return table


def _read(table, splits, native, projection, predicate=None, limit=None):
    builder = table.copy({'read.native.enabled': str(native).lower()}).new_read_builder()
    builder.with_projection(projection)
    if predicate is not None:
        builder.with_filter(predicate)
    if limit is not None:
        builder.with_limit(limit)
    reader = builder.new_read()
    with ExitStack() as guard:
        if native:
            guard.enter_context(patch(
                'pypaimon.execution.python.read.table_read.PythonTableRead._create_split_read',
                side_effect=AssertionError('Python partition-BLOB read fallback')
            ))
        return reader.to_arrow(splits, parallelism=1)


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('nullable', [True, False], ids=['nullable', 'not-null'])
@pytest.mark.parametrize('historical', [False, True], ids=['current', 'historical'])
def test_partition_columns_without_physical_providers_filter_before_blob_limit(
        native_rest_catalog, native, nullable, historical):
    table = _table(native_rest_catalog, nullable, historical)
    plan = table.new_read_builder().new_scan().plan()
    pb = table.new_read_builder().new_predicate_builder()
    predicate = pb.or_predicates([pb.equal('p', 'a'), pb.equal('id', 999)])
    expected = [{'id': 1, 'payload': b'one'}, {'id': 3, 'payload': b'three'}]
    for limit in [None, 1]:
        result = _read(table, plan.splits(), native, ['id', 'payload'], predicate, limit)
        assert result.sort_by('id').to_pylist() == expected[:limit]
        assert _read(table, plan.splits(), native, [], predicate, limit).num_rows == len(expected[:limit])
    partitions = _read(table, plan.splits(), native, ['p'])
    assert partitions.column('p').to_pylist().count('a') == 2
    assert partitions.column('p').to_pylist().count('b') == 2


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
def test_omitted_partitions_survive_partial_updates_and_deletion_vectors(native_rest_catalog, native):
    table = _table(native_rest_catalog, False, False)
    read = table.new_read_builder().with_projection(['id', '_ROW_ID'])
    data = read.new_read().to_arrow(read.new_scan().plan().splits())
    row_ids = dict(zip(data.column('id').to_pylist(), data.column('_ROW_ID').to_pylist()))
    builder = table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['id'])
    messages = update.update_by_arrow_with_row_id(pa.table({
        'id': pa.array([11], type=pa.int32()), '_ROW_ID': pa.array([row_ids[1]], type=pa.int64()),
    }))
    assert all(file.write_cols == ['id'] for message in messages for file in message.new_files)
    builder.new_commit().commit(messages)
    builder.new_commit().commit(builder.new_update().delete_by_row_id([row_ids[3]]))
    splits = table.new_read_builder().new_scan().plan().splits()
    pb = table.new_read_builder().new_predicate_builder()
    predicate = pb.or_predicates([pb.equal('p', 'a'), pb.equal('id', 999)])
    assert _read(table, splits, native, ['id', 'payload'], predicate, 1).to_pylist() == [
        {'id': 11, 'payload': b'one'}]
    assert _read(table, splits, native, [], predicate, 1).num_rows == 1
    assert _read(table, splits, native, ['p'], predicate, 1).to_pylist() == [{'p': 'a'}]
    assert sorted(_read(table, splits, native, ['p']).column('p').to_pylist()) == ['a', 'b', 'b']
