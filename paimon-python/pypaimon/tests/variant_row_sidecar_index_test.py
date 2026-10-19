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

"""Global indexes must select ROW sidecars without losing Variant column versions."""

import os

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.data.generic_variant import GenericVariant
from pypaimon.globalindex.data_evolution_global_index_scanner import DataEvolutionGlobalIndexScanner
from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.utils.range import Range

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]


def _commit(builder, messages):
    commit = builder.new_commit()
    try:
        commit.commit(messages)
    finally:
        commit.close()


def _append(table, data):
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    try:
        writer.write_arrow(data)
        messages = writer.prepare_commit()
        _commit(builder, messages)
        return messages
    finally:
        writer.close()


def _data(schema, ids):
    roots, nested = [], []
    for row in ids:
        value = ({'score': row} if row not in [6, 7, 8, 9]
                 else {6: None, 7: None, 8: 'scalar', 9: [9, None]}[row])
        roots.append(None if row == 6 else GenericVariant.from_python(value))
        child = (None if row == 7 else GenericVariant.from_python(
            None if row == 6 else {'score': row * 10}))
        nested.append(None if row == 8 else {
            'detail': None if child is None else {'value': child.value(), 'metadata': child.metadata()},
            'tag': 'nested-%d' % row})
    return pa.table({
        'id': list(ids),
        'category': ['match' if row in range(5, 10) else 'other' for row in ids],
        'payload': GenericVariant.to_arrow_array(roots),
        'nested': nested,
    }, schema=schema)


def _indexed_read(table):
    predicate = table.new_read_builder().new_predicate_builder().equal('category', 'match')
    builder = table.new_read_builder().with_filter(predicate).with_projection([
        'renamed_payload', 'id', 'nested', '_ROW_ID'])
    splits = builder.new_scan().plan().splits()
    assert splits and all(isinstance(split, IndexedSplit) for split in splits)
    assert [r for split in splits for r in split.row_ranges()] == [Range(25, 29)]
    return builder.new_read().to_arrow(splits)


@pytest.mark.parametrize('index_type', ['bitmap', 'btree'])
@pytest.mark.parametrize('column_update', [False, True])
def test_global_index_reads_variant_sidecars_after_historical_rename(tmp_path, index_type, column_update):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    variant = GenericVariant.to_arrow_array([]).type
    schema = pa.schema([
        ('id', pa.int32()), ('category', pa.string()), ('payload', variant),
        ('nested', pa.struct([('detail', variant), ('tag', pa.string())])),
    ])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'file.format': 'parquet', 'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true', 'data-evolution.row-sidecar.enabled': 'true',
        'global-index.enabled': 'true', 'scan.native-plan.enabled': 'false',
        'read.native.enabled': 'false', 'write.native.enabled': 'false',
        'commit.native.enabled': 'false',
    }), False)
    table = catalog.get_table('default.t')
    # The second physical file starts at global row ID 20, so global index
    # positions 25..29 must be converted into local ROW positions 5..9.
    messages = _append(table, _data(schema, range(-20, 0)))
    data = _data(schema, range(100))
    messages += _append(table, data)
    assert table.create_global_index('category', index_type=index_type) > 0
    predicate = table.new_read_builder().new_predicate_builder().equal('category', 'match')
    with DataEvolutionGlobalIndexScanner.create(table, predicate=predicate) as scanner:
        result = scanner.scan(predicate)
    assert result.results().to_range_list() == [Range(25, 29)]
    expected = data.slice(5, 5).to_pylist()
    if column_update:
        values = GenericVariant.to_arrow_array([
            GenericVariant.from_python({'score': 500}), GenericVariant.from_python(None),
            None, GenericVariant.from_python('updated'), GenericVariant.from_python([99, None]),
        ])
        builder = table.new_batch_write_builder()
        updates = builder.new_update().new_update_by_row_id().update_columns(
            pa.table({'_ROW_ID': list(range(25, 30)), 'payload': values}), ['payload'])
        _commit(builder, updates)
        updated_files = [file for message in updates for file in message.new_files]
        assert len(updated_files) == 1
        assert updated_files[0].write_cols == ['payload']
        assert updated_files[0].row_count == 100
        assert updated_files[0].first_row_id == 20
        messages += updates
        for row, value in zip(expected, values.to_pylist()):
            row['payload'] = value

    payload_id = table.field_dict['payload'].id
    catalog.alter_table('default.t', [
        SchemaChange.rename_column('payload', 'renamed_payload'),
        SchemaChange.rename_column(['nested', 'detail'], 'renamed_detail'),
    ], False)
    table = catalog.get_table('default.t')
    assert table.field_dict['renamed_payload'].id == payload_id
    expected = [{
        'renamed_payload': row['payload'], 'id': row['id'],
        'nested': (None if row['nested'] is None else {
            'renamed_detail': row['nested']['detail'], 'tag': row['nested']['tag']}),
        '_ROW_ID': row['id'] + 20,
    } for row in expected]

    # This assertion also guards against silently choosing the old full file
    # instead of the latest column-only file after its write_cols was renamed.
    assert _indexed_read(table).to_pylist() == expected
    for message in messages:
        for file in message.new_files:
            sidecars = [path for path in file.collect_files() if path.endswith('.row')]
            assert len(sidecars) == 1 and os.path.exists(sidecars[0])
            os.remove(file.external_path or file.file_path)
    # A correct result now requires every contributing column bunch to use ROW.
    assert _indexed_read(table).to_pylist() == expected

    # Without the real global index there is no row selection and the deleted
    # primary must be opened. This rules out an accidental full-scan success.
    unindexed = table.copy({'global-index.enabled': 'false'})
    builder = unindexed.new_read_builder().with_filter(predicate)
    with pytest.raises(OSError):
        builder.new_read().to_arrow(builder.new_scan().plan().splits())
