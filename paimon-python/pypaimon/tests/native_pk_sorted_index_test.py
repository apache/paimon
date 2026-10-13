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

"""Java source-backed scalar indexes through the REST Native planner."""

import shutil
from pathlib import Path
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.globalindex.indexed_split import IndexedSplit
from pypaimon.read.native_plan import native_plan, native_runtime_available
from pypaimon.tests import native_plan_rest_test, primary_key_global_index_golden_test

rest_catalog = native_plan_rest_test.rest_catalog
golden_catalog = primary_key_global_index_golden_test.catalog

pytestmark = [pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='Rust main required')]


@pytest.fixture
def indexed_table(rest_catalog, golden_catalog):
    catalog, server = rest_catalog
    golden = golden_catalog.get_table('default.test_pk_global_index_golden')
    schema = golden.table_schema
    catalog.create_table('default.indexed', Schema(
        schema.fields, schema.partition_keys, schema.primary_keys, schema.options), False)
    source = Path(golden.table_path)
    destination = Path(server.data_path) / server.warehouse / 'default' / 'indexed'
    for directory in ('bucket-0', 'index', 'manifest', 'snapshot'):
        shutil.copytree(str(source / directory), str(destination / directory))
    return catalog.get_table('default.indexed')


@pytest.mark.parametrize('column,value,ids', [
    ('name', 'name-4', [4]), ('category', 'odd', [1, 3, 5]),
    ('name', 'absent', []),
])
def test_java_scalar_indexes_are_planned_in_rust(indexed_table, column, value, ids):
    table = indexed_table.copy({'scan.native-plan.enabled': 'true', 'read.native.enabled': 'true'})
    predicate = table.new_read_builder().new_predicate_builder().equal(column, value)
    plan = native_plan(table, predicate=predicate)
    assert plan.snapshot_id == 6
    # Java's fixture also contains an uncovered APPEND file. Keep it as an
    # ordinary split; only the compacted source can use index positions.
    indexed = [split for split in plan.splits() if isinstance(split, IndexedSplit)]
    assert len(indexed) == (1 if ids else 0)
    assert all(not split.raw_convertible for split in indexed)
    assert all(file.file_source != 1 for split in plan.splits()
               if not isinstance(split, IndexedSplit) for file in split.files)
    with patch('pypaimon.index.index_file_handler.IndexFileHandler.scan',
               side_effect=AssertionError('Python index planning must not run')):
        builder = table.new_read_builder().with_filter(predicate)
        scan_plan = builder.new_scan().plan()
        result = builder.new_read().to_arrow(scan_plan.splits())
    assert sorted(result.column('id').to_pylist()) == ids


@pytest.mark.parametrize('mode,ids', [('and', [3]), ('or', [1, 3, 4, 5]), ('unsupported-or', [1, 2])])
def test_native_compound_predicates_keep_residuals(indexed_table, mode, ids):
    from pypaimon.common.predicate_builder import PredicateBuilder
    table = indexed_table.copy({'scan.native-plan.enabled': 'true', 'read.native.enabled': 'true'})
    pb = table.new_read_builder().new_predicate_builder()
    if mode == 'and':
        predicate = PredicateBuilder.and_predicates([pb.equal('name', 'name-3'), pb.equal('category', 'odd')])
    elif mode == 'or':
        predicate = PredicateBuilder.or_predicates([pb.equal('name', 'name-4'), pb.equal('category', 'odd')])
    else:
        predicate = PredicateBuilder.or_predicates([pb.equal('name', 'name-1'), pb.equal('id', 2)])
    with patch('pypaimon.index.index_file_handler.IndexFileHandler.scan',
               side_effect=AssertionError('Python index planning must not run')):
        builder = table.new_read_builder().with_filter(predicate)
        result = builder.new_read().to_arrow(builder.new_scan().plan().splits())
    assert sorted(result.column('id').to_pylist()) == ids


@pytest.mark.parametrize('version', [2, 4, 6])
def test_native_index_uses_selected_snapshot(indexed_table, version):
    base = indexed_table.copy({'scan.version': str(version), 'read.native.enabled': 'true'})
    predicate = base.new_read_builder().new_predicate_builder().equal('category', 'odd')
    classic = base.copy({'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false'})
    classic = classic.new_read_builder().with_filter(predicate)
    expected = classic.new_read().to_arrow(classic.new_scan().plan().splits()).to_pylist()
    with patch('pypaimon.index.index_file_handler.IndexFileHandler.scan',
               side_effect=AssertionError('Python index planning must not run')):
        builder = base.copy({'scan.native-plan.enabled': 'true'}).new_read_builder().with_filter(predicate)
        plan = builder.new_scan().plan()
        rows = builder.new_read().to_arrow(plan.splits()).to_pylist()
    assert plan.snapshot_id == version
    assert sorted(rows, key=lambda row: row['id']) == sorted(expected, key=lambda row: row['id'])


@pytest.mark.parametrize('index_type,column,value,ids', [
    ('btree', 'name', 'name-4', [4]), ('bitmap', 'category', 'odd', [1, 3, 5]),
])
def test_native_index_read_failure_keeps_data(indexed_table, index_type, column, value, ids):
    from pypaimon.index.index_file_handler import IndexFileHandler
    table = indexed_table.copy({'scan.native-plan.enabled': 'true', 'read.native.enabled': 'true'})
    entries = IndexFileHandler(table).scan(table.snapshot_manager().get_latest_snapshot())
    entry = next(entry for entry in entries if entry.index_file.index_type == index_type)
    table.file_io.delete(table.table_path + '/index/' + entry.index_file.file_name)
    predicate = table.new_read_builder().new_predicate_builder().equal(column, value)
    with patch('pypaimon.index.index_file_handler.IndexFileHandler.scan',
               side_effect=AssertionError('Python index planning must not run')):
        builder = table.new_read_builder().with_filter(predicate)
        plan = builder.new_scan().plan()
        assert all(not isinstance(split, IndexedSplit) for split in plan.splits())
        rows = builder.new_read().to_arrow(plan.splits())
    assert sorted(rows.column('id').to_pylist()) == ids


def test_native_pk_index_positions_are_file_local_with_row_tracking(indexed_table):
    from copy import deepcopy
    table = indexed_table.copy({'read.native.enabled': 'true'})
    predicate = table.new_read_builder().new_predicate_builder().equal('name', 'name-4')
    plan = native_plan(table, predicate=predicate)
    splits = deepcopy(plan.splits())
    for split in splits:
        for file in split.files:
            file.first_row_id = 100
    builder = table.new_read_builder().with_filter(predicate)
    assert builder.new_read().to_arrow(splits).column('id').to_pylist() == [4]


@pytest.mark.parametrize('disabled', [False, True], ids=['indexed', 'index-disabled'])
def test_native_sorted_index_limit_and_shards(indexed_table, disabled):
    table = indexed_table.copy({'scan.native-plan.enabled': 'true', 'read.native.enabled': 'true',
                                'global-index.enabled': str(not disabled).lower()})
    predicate = table.new_read_builder().new_predicate_builder().equal('category', 'odd')
    ids = []
    with patch('pypaimon.index.index_file_handler.IndexFileHandler.scan',
               side_effect=AssertionError('Python index planning must not run')):
        for shard in range(2):
            builder = table.new_read_builder().with_filter(predicate).with_limit(1)
            plan = builder.new_scan().with_shard(shard, 2).plan()
            if disabled:
                assert all(not isinstance(split, IndexedSplit) for split in plan.splits())
            rows = builder.new_read().to_arrow(plan.splits())
            assert rows.num_rows <= 1
            ids.extend(rows.column('id').to_pylist())
    assert len(ids) == 1
    assert ids[0] in [1, 3, 5]


def test_native_sorted_index_does_not_resurrect_an_old_version(indexed_table):
    from pypaimon.tests.native_plan_expanded_test import write
    table = indexed_table.copy({
        'deletion-vectors.enabled': 'false', 'write-only': 'true',
        'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'scan.native-plan.enabled': 'true', 'read.native.enabled': 'true',
    })
    write(table, [{'id': 4, 'name': 'changed', 'category': 'even'}],
          pa.schema([('id', pa.int32()), ('name', pa.string()), ('category', pa.string())]))
    predicate = table.new_read_builder().new_predicate_builder().equal('name', 'name-4')
    with patch('pypaimon.index.index_file_handler.IndexFileHandler.scan',
               side_effect=AssertionError('Python index planning must not run')):
        builder = table.new_read_builder().with_filter(predicate)
        plan = builder.new_scan().plan()
        assert any(not split.raw_convertible and len(split.files) > 1 for split in plan.splits())
        assert builder.new_read().to_arrow(plan.splits()).num_rows == 0
