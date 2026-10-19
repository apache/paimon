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

"""An unused vector format must not disable Parquet/Blob Native mutations."""

from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pyarrow.compute as pc
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.table.data_evolution_merge_into import WhenMatched, WhenNotMatched, source_col
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.tests import native_plan_rest_test
from pypaimon.write.native_update import _native_update_formats_supported
from pypaimon.write.table_delete import TableDeleteByRowId
from pypaimon.write.table_update import TableUpdate
from pypaimon.write.table_update_by_row_id import TableUpdateByRowId
from pypaimon.write.table_upsert_by_key import TableUpsertByKey


rest_catalog = native_plan_rest_test.rest_catalog
pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
_NATIVE = pytest.mark.skipif(not native_runtime_available(), reason='pypaimon-rust not installed')
_ENGINES = [False, pytest.param(True, marks=_NATIVE)]
_SCHEMA = pa.schema([('id', pa.int32()), ('pt', pa.string()), ('value', pa.int32()),
                     ('payload', pa.large_binary())])
_ROWS = [dict(id=i, pt='a', value=i * 10, payload=('blob-%d' % i).encode()) for i in range(1, 4)]


@pytest.mark.parametrize('vector,format_,expected', [
    (False, 'orc', True), (False, 'parquet', True),
    (True, 'orc', False), (True, 'parquet', True),
])
def test_update_format_gate_checks_only_active_vector_fields(tmp_path, vector, format_, expected):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = (_SCHEMA.append(pa.field('embedding', pa.list_(pa.float32(), 2))) if vector else _SCHEMA)
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'file.format': 'parquet', 'vector.file.format': format_,
    }), False)
    assert _native_update_formats_supported(catalog.get_table('default.t')) is expected


def _write(table, rows):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=_SCHEMA))
        messages = writer.prepare_commit()
        assert all(file.file_name.endswith(('.parquet', '.blob'))
                   and '.vector.' not in file.file_name for message in messages for file in message.new_files)
        commit.commit(messages)
    finally:
        writer.close()
        commit.close()


def _read(table):
    table = table.copy({'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    builder = table.new_read_builder().with_projection(table.field_names + ['_ROW_ID'])
    return builder.new_read().to_arrow(builder.new_scan().plan().splits()).sort_by('id').to_pylist()


def _native_only(native):
    stack = ExitStack()
    if native:
        for owner, method in [(TableUpdateByRowId, '_load_existing_files_info'),
                              (TableUpsertByKey, '_upsert_partition'),
                              (TableUpsertByKey, '_upsert_row_partition'),
                              (TableUpdate, '_build_predicate_update_table'),
                              (TableDeleteByRowId, '_scan_anchor_ranges')]:
            stack.enter_context(patch.object(owner, method, side_effect=AssertionError('Python mutation fallback')))
        stack.enter_context(patch('pypaimon.table.data_evolution_merge_into._build_tables',
                                  side_effect=AssertionError('Python MERGE fallback')))
    return stack


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('stream', [False, True], ids=['batch', 'stream'])
@pytest.mark.parametrize('operation', ['row-id', 'predicate', 'callable', 'upsert', 'row-upsert',
                                       'merge', 'merge-table', 'row-delete', 'predicate-delete'])
def test_inactive_vector_option_preserves_parquet_blob_mutations(rest_catalog, native, stream, operation):
    catalog, _ = rest_catalog
    options = {
        'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        'deletion-vectors.enabled': 'true', 'file.format': 'parquet', 'vector.file.format': 'orc',
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
    }
    catalog.create_table('default.t', Schema.from_pyarrow_schema(
        _SCHEMA, partition_keys=['pt'], options=options), False)
    table = catalog.get_table('default.t')
    _write(table, _ROWS)
    before = _read(table)
    row_id = before[0]['_ROW_ID']
    table = table.copy({'write.native.enabled': str(native).lower(), 'commit.native.enabled': str(native).lower()})
    source_rows = [dict(_ROWS[0], value=99), dict(id=4, pt='a', value=40, payload=b'blob-4')]
    source = pa.Table.from_pylist(source_rows, schema=_SCHEMA)
    if operation == 'merge-table':
        catalog.create_table('default.s', Schema.from_pyarrow_schema(_SCHEMA, options=options), False)
        source = catalog.get_table('default.s')
        _write(source, source_rows)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    update = builder.new_update().with_update_type(['value'])
    predicate = update.new_predicate_builder().equal('id', 1)

    def invoke(method, *args, **kwargs):
        if stream:
            kwargs['commit_identifier'] = 7
        return method(*args, **kwargs)

    with _native_only(native):
        if operation == 'row-id':
            messages = invoke(update.update_by_arrow_with_row_id,
                              pa.table({'_ROW_ID': [row_id], 'value': pa.array([99], type=pa.int32())}))
        elif operation in ('predicate', 'callable'):
            assignments = {'value': 99 if operation == 'predicate' else lambda batch: pc.add(batch['value'], 89)}
            messages = invoke(update.update_by_predicate, predicate, assignments,
                              read_columns=['value'] if operation == 'callable' else None)
        elif operation == 'upsert':
            messages = invoke(update.upsert_by_arrow_with_key, source, ['id'])
        elif operation == 'row-upsert':
            rows = [GenericRow([row[field.name] for field in table.fields], table.fields) for row in source_rows]
            messages = invoke(update.upsert_by_key, rows, ['id'])
        elif operation in ('merge', 'merge-table'):
            messages = invoke(update.merge_into, source, on=['id', 'pt'],
                              when_matched=[WhenMatched.update({'value': source_col('value')})],
                              when_not_matched=[WhenNotMatched('*')])
        elif operation == 'row-delete':
            messages = invoke(update.delete_by_row_id, [row_id])
        else:
            messages = invoke(update.delete_by_predicate, predicate)
    files = [file for message in messages for file in message.new_files]
    assert messages
    assert all(file.file_name.endswith(('.parquet', '.blob')) and '.vector.' not in file.file_name for file in files)
    commit = builder.new_commit()
    try:
        with ExitStack() as stack:
            if native:
                stack.enter_context(patch.object(
                    commit.file_store_commit, 'commit', side_effect=AssertionError('Python commit fallback')))
            commit.commit(messages, 7) if stream else commit.commit(messages)
    finally:
        commit.close()
    actual = [{key: value for key, value in row.items() if key != '_ROW_ID'} for row in _read(table)]
    expected = [dict(row) for row in _ROWS]
    if operation.endswith('delete'):
        expected = expected[1:]
    else:
        expected[0]['value'] = 99
        if operation in ('upsert', 'row-upsert', 'merge', 'merge-table'):
            expected.append(source_rows[1])
    assert actual == expected
