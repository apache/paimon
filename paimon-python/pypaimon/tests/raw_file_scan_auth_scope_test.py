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

"""Raw file planning uses its own authorization scope on cached scanners."""

import json
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.catalog.catalog_exception import TableNoPermissionException
from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.execution.native.read.plan import NativeTableScan


@pytest.fixture
def table(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    arrow_schema = pa.schema([('id', pa.int32()), ('pt', pa.string())])
    catalog.create_table('default.files', Schema.from_pyarrow_schema(
        arrow_schema, partition_keys=['pt'], options={'file.format': 'parquet'}), False)
    table = catalog.get_table('default.files')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(
            [{'id': 1, 'pt': 'a'}, {'id': 2, 'pt': 'b'}], schema=arrow_schema))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return table


def _partition_filter():
    return json.dumps({
        'kind': 'LEAF',
        'transform': {'name': 'FIELD_REF',
                      'fieldRef': {'index': 1, 'name': 'pt', 'type': 'STRING'}},
        'function': 'EQUAL', 'literals': ['a'],
    })


@pytest.mark.parametrize('native_fallback', [False, True])
@pytest.mark.parametrize('restriction', ['row-filter', 'mask'])
def test_raw_scope_clears_cached_query_rules(table, restriction, native_fallback):
    builder = table.new_read_builder()
    if restriction == 'mask':
        predicate = builder.new_predicate_builder().equal('pt', 'a')
        builder.with_filter(predicate)
        rules = TableQueryAuthResult(None, {'pt': json.dumps({'name': 'NULL'})})
    else:
        rules = TableQueryAuthResult([_partition_filter()], None)
    scan = builder.new_scan()
    state = {'rules': rules}
    selects = []

    def authorize(select):
        selects.append(select)
        return state['rules']

    scan._query_auth_fn = authorize
    scan.plan()
    source = scan._python_file_scanner
    state['rules'] = None
    with patch.object(scan, '_native_requested', return_value=native_fallback), \
            patch.object(NativeTableScan, 'plan_files', return_value=None) as native:
        entries = scan.file_scanner.plan_files()
    assert {tuple(entry.partition.values) for entry in entries} == (
        {('a',)} if restriction == 'mask' else {('a',), ('b',)})
    assert scan._python_file_scanner is source
    assert source.auth_partition_predicate is None
    assert source.predicate == scan.predicate
    assert selects[-1] is None, 'Raw files require whole-table authorization'
    assert native.call_count == int(native_fallback)


def test_restricted_raw_scope_does_not_clear_cached_rules(table):
    scan = table.new_read_builder().new_scan()
    scan._query_auth_fn = lambda select: TableQueryAuthResult([_partition_filter()], None)
    scan.plan()
    source = scan._python_file_scanner
    prior_partition_filter = source.auth_partition_predicate
    assert prior_partition_filter is not None
    with patch.object(source, 'plan_files', side_effect=AssertionError('Unauthorized file access')):
        with pytest.raises(TableNoPermissionException):
            scan.file_scanner.plan_files()
    assert source.auth_partition_predicate is prior_partition_filter
