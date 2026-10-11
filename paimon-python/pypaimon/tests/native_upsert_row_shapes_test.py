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

"""Keep named-row presence through Native upsert matching and appending."""

from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.native_write import native_write_available


@pytest.fixture(params=[False, pytest.param(True, marks=[
    pytest.mark.native_plan,
    pytest.mark.skipif(not native_write_available(), reason='native writer required'),
])], ids=['python', 'native'])
def native(request):
    return request.param


@pytest.fixture(params=[False, True], ids=['batch', 'stream'])
def stream(request):
    return request.param


@pytest.fixture
def table_factory(tmp_path, native):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)

    def create(partitioned=False):
        fields = [('id', pa.int32()), ('value', pa.int32()), ('score', pa.int32())]
        if partitioned:
            fields.append(('p', pa.int32()))
        schema = pa.schema(fields)
        catalog.create_table('default.t', Schema.from_pyarrow_schema(
            schema, partition_keys=['p'] if partitioned else None, options={
                'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
                'deletion-vectors.enabled': 'true',
                'write.native.enabled': str(native).lower(),
                'scan.native-plan.enabled': str(native).lower(),
                'read.native.enabled': str(native).lower(),
            }), False)
        return catalog.get_table('default.t'), schema

    return create


def _seed(table, schema, rows):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _upsert(table, native, stream, rows, columns=('value',)):
    by_name = {field.name: field for field in table.fields}
    rows = [GenericRow(list(row.values()), [by_name[name] for name in row]) for row in rows]
    builder = (table.new_stream_write_builder() if stream else table.new_batch_write_builder())
    update = builder.new_update().with_update_type(list(columns))
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch(
                'pypaimon.write.table_upsert_by_key.TableUpsertByKey._upsert_row_partition',
                side_effect=AssertionError('named-row upsert fell back to Python')))
            stack.enter_context(patch(
                'pypaimon.write.table_upsert_by_key.TableUpsertByKey._build_key_to_row_ids_map',
                side_effect=AssertionError('upsert keys were matched in Python')))
        messages = (update.upsert_by_key(rows, ['id'], 101) if stream
                    else update.upsert_by_key(rows, ['id']))
    commit = builder.new_commit()
    try:
        if stream:
            commit.commit(messages, 101)
        else:
            commit.commit(messages)
    finally:
        commit.close()
    return messages


def _rows(table):
    builder = table.new_read_builder()
    plan = builder.new_scan().plan()
    return sorted(builder.new_read().to_arrow(plan.splits()).to_pylist(),
                  key=lambda row: (row['id'], row.get('p', 0)))


def _append_shapes(messages):
    return {tuple(file.write_cols) for message in messages for file in message.new_files
            if file.write_cols and 'id' in file.write_cols}


def test_upsert_deduplicates_across_shapes_before_validating_columns(
        native, stream, table_factory):
    table, schema = table_factory()
    _seed(table, schema, [
        {'id': 1, 'value': 10, 'score': 100},
        {'id': 1, 'value': 11, 'score': 101},
        {'id': 2, 'value': 20, 'score': 200},
    ])
    messages = _upsert(table, native, stream, [
        {'id': 1, 'score': 999}, {'id': 4, 'value': 40},
        {'value': 111, 'id': 1}, {'id': 4, 'score': 400},
        {'id': 2, 'value': 222, 'score': 999},
    ])
    assert _rows(table) == [
        {'id': 1, 'value': 111, 'score': 100},
        {'id': 1, 'value': 111, 'score': 101},
        {'id': 2, 'value': 222, 'score': 200},
        {'id': 4, 'value': None, 'score': 400},
    ]
    assert _append_shapes(messages) == {('id', 'score')}


def test_upsert_distinguishes_explicit_null_from_an_absent_field(native, stream, table_factory):
    table, schema = table_factory()
    _seed(table, schema, [{'id': 1, 'value': 10, 'score': 100}])
    messages = _upsert(table, native, stream, [
        {'id': 1, 'value': None}, {'id': 2, 'score': 200},
    ])
    assert _rows(table) == [
        {'id': 1, 'value': None, 'score': 100},
        {'id': 2, 'value': None, 'score': 200},
    ]
    assert _append_shapes(messages) == {('id', 'score')}


def test_upsert_allows_different_append_shapes_in_different_partitions(native, stream, table_factory):
    table, schema = table_factory(partitioned=True)
    _seed(table, schema, [
        {'id': 1, 'value': 10, 'score': 100, 'p': 1},
        {'id': 1, 'value': 20, 'score': 200, 'p': 2},
    ])
    messages = _upsert(table, native, stream, [
        {'id': 1, 'value': 111, 'p': 1}, {'id': 4, 'value': 40, 'p': 1},
        {'id': 4, 'score': 400, 'p': 2}, {'id': 1, 'value': 222, 'score': 999, 'p': 2},
        {'id': 4, 'value': 41, 'p': 1},
    ])
    assert _rows(table) == [
        {'id': 1, 'value': 111, 'score': 100, 'p': 1},
        {'id': 1, 'value': 222, 'score': 200, 'p': 2},
        {'id': 4, 'value': 41, 'score': None, 'p': 1},
        {'id': 4, 'value': None, 'score': 400, 'p': 2},
    ]
    assert _append_shapes(messages) == {('id', 'value', 'p'), ('id', 'score', 'p')}


@pytest.mark.parametrize('input_rows,message', [
    ([{'id': 1, 'value': 111}, {'id': 2, 'score': 222}], 'value'),
    ([{'id': 4, 'value': 40}, {'id': 5, 'score': 500}], 'same field set'),
])
def test_upsert_rejects_missing_winner_columns_and_inconsistent_appends(
        native, stream, table_factory, input_rows, message):
    table, schema = table_factory()
    original = [{'id': 1, 'value': 10, 'score': 100}, {'id': 2, 'value': 20, 'score': 200}]
    _seed(table, schema, original)
    before = table.snapshot_manager().get_latest_snapshot().id
    with pytest.raises(ValueError, match=message):
        _upsert(table, native, stream, input_rows)
    assert table.snapshot_manager().get_latest_snapshot().id == before
    assert _rows(table) == original
