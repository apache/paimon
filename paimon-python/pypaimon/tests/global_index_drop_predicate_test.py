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

"""Index selection accepts immutable table and partition layout predicates."""

from contextlib import ExitStack
from copy import deepcopy
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.execution.native.runtime import native_runtime_available
from pypaimon.globalindex.create_global_index import GlobalIndexBuilder
from pypaimon.globalindex.drop_global_index import GlobalIndexDropper
from pypaimon.index.index_file_handler import IndexFileHandler

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]
_ENGINES = [False, pytest.param(True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed')])]


def _table(catalog, native=False, multipartition=False):
    schema = pa.schema([('id', pa.int32()), ('first', pa.string()), ('second', pa.string())])
    catalog.create_table('default.index_predicates', Schema.from_pyarrow_schema(
        schema, partition_keys=['second', 'first'] if multipartition else ['second'], options={
            'bucket': '-1', 'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
            'write.native.enabled': str(native).lower(), 'commit.native.enabled': 'false',
            'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
        }), False)
    table = catalog.get_table('default.index_predicates')
    builder = table.copy({'write.native.enabled': 'false'}).new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pydict({
            'id': [1, 2, 3], 'first': ['A', 'B', 'A'], 'second': ['x', 'x', 'y'],
        }, schema=schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return table


def _partition_matches(table, entry, multipartition):
    values = dict(zip(table.partition_keys, entry.partition.values))
    return values['second'] == 'x' and (not multipartition or values['first'] == 'A')


@pytest.mark.parametrize('native', _ENGINES, ids=['python', 'native'])
@pytest.mark.parametrize('multipartition', [False, True], ids=['single', 'reordered'])
@pytest.mark.parametrize('layout', ['table', 'partition'])
@pytest.mark.parametrize('kind', ['btree', 'bitmap'])
def test_drop_rebinds_partition_selection_without_mutating_the_users_predicate(
        native_rest_catalog, native, multipartition, layout, kind):
    table = _table(native_rest_catalog, native, multipartition)
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch(
                'pypaimon.execution.python.globalindex.create_global_index.PythonGlobalIndexBuilder.build',
                side_effect=AssertionError('Native index build used Python execution')))
        assert table.create_global_index('id', index_type=kind) > 0
        assert table.create_global_index('first', index_type=kind) > 0
    entries = IndexFileHandler(table).scan(None)
    target_id = table.field_dict['id'].id
    expected = {entry.index_file.file_name for entry in entries
                if entry.index_file.global_index_meta.index_field_id == target_id
                and _partition_matches(table, entry, multipartition)}
    assert expected
    builder = (table.new_read_builder().new_predicate_builder() if layout == 'table' else
               PredicateBuilder(table.partition_keys_fields))
    leaves = [builder.equal('second', 'x')]
    if multipartition:
        leaves.append(builder.equal('first', 'A'))
    selection = builder.and_predicates(leaves)
    original = deepcopy(selection)
    snapshot_id = table.snapshot_manager().get_latest_snapshot().id
    with ExitStack() as stack:
        if native:
            stack.enter_context(patch(
                'pypaimon.execution.python.globalindex.drop_global_index.build_global_index_drop',
                side_effect=AssertionError('Native index drop used Python execution')))
        assert table.drop_global_index('id', index_type=kind, partition_filter=selection, dry_run=True) == len(expected)
        assert table.snapshot_manager().get_latest_snapshot().id == snapshot_id
        assert {entry.index_file.file_name for entry in IndexFileHandler(table).scan(None)} == {
            entry.index_file.file_name for entry in entries}
        assert table.drop_global_index('id', index_type=kind, partition_filter=selection) == len(expected)
    assert table.snapshot_manager().get_latest_snapshot().id == snapshot_id + 1
    assert selection == original
    assert {entry.index_file.file_name for entry in IndexFileHandler(table).scan(None)} == {
        entry.index_file.file_name for entry in entries} - expected
    # Snapshots may still refer to these files; dropping only changes metadata.
    paths = table.path_factory().global_index_path_factory()
    assert all(table.file_io.exists(paths.to_path(entry.index_file.file_name)) for entry in entries)


@pytest.mark.parametrize('native', [False, True])
@pytest.mark.parametrize('invalid', ['non-partition', 'mixed', 'unknown', 'nested-alias'])
def test_invalid_index_partition_filter_is_rejected_before_selecting_execution(tmp_path, native, invalid):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    table = _table(catalog)
    assert table.create_global_index('id') > 0
    table = table.copy({'write.native.enabled': str(native).lower()})
    builder = table.new_read_builder().new_predicate_builder()
    selection = {
        'non-partition': builder.equal('id', 1),
        'mixed': builder.and_predicates([builder.equal('second', 'x'), builder.equal('id', 1)]),
        'unknown': Predicate('equal', 99, 'absent', ['x']),
        'nested-alias': Predicate('equal', 0, 'second', ['x'], ['id', 'child']),
    }[invalid]
    original = deepcopy(selection)
    before = table.snapshot_manager().get_latest_snapshot().id
    files = {entry.index_file.file_name for entry in IndexFileHandler(table).scan(None)}
    for request in (GlobalIndexBuilder(table, 'id', partition_filter=selection),
                    GlobalIndexDropper(table, 'id', partition_filter=selection)):
        with patch('pypaimon.globalindex.index_dispatch.build_global_index',
                   side_effect=AssertionError('Invalid selection reached index build')) as build, \
                patch('pypaimon.globalindex.index_dispatch.build_global_index_drop',
                      side_effect=AssertionError('Invalid selection reached index drop')) as drop:
            with pytest.raises(ValueError, match='only partition keys'):
                request.build()
            build.assert_not_called()
            drop.assert_not_called()
    assert selection == original
    assert table.snapshot_manager().get_latest_snapshot().id == before
    assert {entry.index_file.file_name for entry in IndexFileHandler(table).scan(None)} == files


def test_build_uses_the_same_partition_layout_normalization(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
    catalog.create_database('default', True)
    table = _table(catalog, multipartition=True)
    builder = table.new_read_builder().new_predicate_builder()
    selection = builder.and_predicates([builder.equal('second', 'x'), builder.equal('first', 'A')])
    original = deepcopy(selection)
    count = table.create_global_index('id', partition_filter=selection)
    entries = IndexFileHandler(table).scan(None)
    assert count == len(entries) > 0
    assert all(_partition_matches(table, entry, True) for entry in entries)
    assert selection == original
