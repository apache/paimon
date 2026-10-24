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

"""Bound scan/read executors share an explicit protocol and live configuration."""

from inspect import signature
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.native.read.plan import NativeTableScan
from pypaimon.execution.native.read.table_read import NativeTableRead
from pypaimon.execution.python.read.table_read import PythonTableRead
from pypaimon.execution.python.read.table_scan import PythonTableScan
from pypaimon.read.plan import Plan
from pypaimon.read.scan_request import ScanRequest

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read,
              pytest.mark.python_write, pytest.mark.python_commit]


@pytest.mark.parametrize('method', ['read_rows', 'read_batches'])
def test_bound_read_executors_have_the_same_call_contract(method):
    assert signature(getattr(PythonTableRead, method)) == signature(getattr(NativeTableRead, method))


@pytest.mark.parametrize('method', ['plan', 'plan_all', 'scan_with_stats'])
def test_bound_scan_executors_have_the_same_call_contract(method):
    assert signature(getattr(PythonTableScan, method)) == signature(getattr(NativeTableScan, method))


@pytest.fixture
def table(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.protocol', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int32())]), options={
            'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
            'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        }), False)
    table = catalog.get_table('default.protocol')
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'id': pa.array([1, 2, 3], type=pa.int32())}))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    return table


@pytest.mark.parametrize('backend', ['python', 'native'])
@pytest.mark.parametrize('method', ['read_rows', 'read_batches'])
def test_bound_read_uses_live_configuration_and_selected_executor(table, backend, method):
    splits = table.new_read_builder().new_scan().plan().splits()
    if backend == 'native':
        table = table.copy({'read.native.enabled': 'true'})
        for split in splits:
            split._native_split = object()
    read = table.new_read_builder().new_read()
    execution = (NativeTableRead if backend == 'native' else PythonTableRead)(read)
    read.limit = 2
    schema = read._output_arrow_schema(splits)
    closed = []

    def native_batches(*args, **kwargs):
        values = [1, 2, 3][:kwargs['limit']]
        batch = pa.record_batch([pa.array(values, type=pa.int32())], names=['id'])
        if kwargs.get('include_row_kind'):
            batch = pa.record_batch(
                [pa.array(['+I'] * len(values)), batch.column(0)], names=['rowkind', 'id'])

        def batches():
            try:
                yield batch
            finally:
                closed.append(True)
        return batches()

    with patch('pypaimon.execution.native.read.table_read.native_read', side_effect=native_batches) as native, \
            patch.object(PythonTableRead, '_create_reader_for_split', autospec=True,
                         side_effect=PythonTableRead._create_reader_for_split) as python:
        if method == 'read_rows':
            values = [row.get_field(0) for row in execution.read_rows(splits)]
        else:
            values = [value for batch in execution.read_batches(splits, schema, parallelism=1)
                      for value in batch.column('id').to_pylist()]
    assert values == [1, 2]
    assert read.limit == 2 and not read.include_row_kind and read._row_kind_column == '_row_kind'
    if backend == 'native':
        native.assert_called_once()
        assert native.call_args[1]['limit'] == 2
        python.assert_not_called()
        assert closed == [True]
    else:
        native.assert_not_called()
        python.assert_called_once()


@pytest.mark.parametrize('method', ['read_rows', 'read_batches'])
def test_native_read_preparation_distinguishes_empty_results_from_unsupported(table, method):
    read = table.copy({'read.native.enabled': 'true'}).new_read_builder().new_read()
    execution = NativeTableRead(read)
    schema = read._output_arrow_schema([])
    if method == 'read_rows':
        assert list(execution.read_rows([])) == []
    else:
        assert list(execution.read_batches([], schema)) == []
    read.table = table
    if method == 'read_rows':
        assert execution.read_rows([]) is None
    else:
        assert execution.read_batches([], schema) is None


@pytest.mark.parametrize('failure', [False, True])
def test_python_bound_all_files_restores_scanner_configuration_on_success_or_failure(failure):
    scanner = SimpleNamespace(skip_level0=True)
    expected = Plan([], 9)

    def scan():
        assert scanner.skip_level0 is False
        if failure:
            raise OSError('planning failed')
        return expected

    scanner.scan = scan
    execution = PythonTableScan(SimpleNamespace(_python_file_scanner=scanner, _query_auth_fn=None))
    if failure:
        with pytest.raises(OSError, match='planning failed'):
            execution.plan_all()
    else:
        assert execution.plan_all() is expected
    assert scanner.skip_level0 is True


@pytest.mark.parametrize('method', ['plan', 'plan_all', 'scan_with_stats'])
def test_native_bound_scan_uses_live_configuration_without_creating_a_python_scanner(table, method):
    scan = table.new_read_builder().new_scan()
    execution = NativeTableScan(scan)
    scan.limit = 1
    expected = Plan([], 9)
    with patch('pypaimon.execution.native.read.plan.native_plan', return_value=expected) as native, \
            patch.object(scan, '_create_file_scanner', side_effect=AssertionError('Python scanner created')):
        result = getattr(execution, method)()
    assert (result[0] if method == 'scan_with_stats' else result).snapshot_id == 9
    assert native.call_args[1]['limit'] == 1
    assert bool(native.call_args[1].get('all_files')) is (method == 'plan_all')
    assert scan._file_scanner is None


@pytest.mark.parametrize('method', ['plan', 'plan_all', 'scan_with_stats'])
def test_native_bound_scan_does_not_turn_preparation_failure_into_an_empty_plan(table, method):
    scan = table.new_read_builder().new_scan()
    execution = NativeTableScan(scan)
    with patch('pypaimon.execution.native.read.plan.native_plan', side_effect=OSError('prepare failed')), \
            patch.object(scan, '_create_file_scanner', side_effect=AssertionError('Python scanner created')):
        assert getattr(execution, method)() is None
    assert scan._file_scanner is None


@pytest.mark.parametrize('ordered', [False, True])
def test_native_bound_batches_preserve_the_requested_order_mode(table, ordered):
    read = table.copy({'read.native.enabled': 'true'}).new_read_builder().new_read()
    execution = NativeTableRead(read)
    split = SimpleNamespace(files=[SimpleNamespace(file_name='data.parquet')], _native_split=object())
    schema = read._output_arrow_schema([split])
    with patch('pypaimon.execution.native.read.table_read.native_read', return_value=[]) as native:
        assert list(execution.read_batches([split], schema, parallelism=3, ordered=ordered)) == []
    assert native.call_args[1]['parallelism'] == 3
    assert native.call_args[1]['ordered'] is ordered


def test_python_scanner_uses_private_cache_and_captured_configuration(table):
    request = ScanRequest(idx_of_this_subtask=1, number_of_para_subtasks=3,
                          row_ranges=((7, 9),))
    configure = Mock(side_effect=lambda scanner: setattr(scanner, 'skip_level0', False))

    class PublicScanner:
        _is_dispatch_file_scanner = True
        _configure_python_scanner = staticmethod(configure)

    class Scan:
        _python_file_scanner = None
        _file_scanner = PublicScanner()
        _query_auth_fn = None

        @property
        def file_scanner(self):
            raise AssertionError('Python execution entered public scanner dispatch')

        def _scan_request(self):
            return request

    scan = Scan()
    scan.table = table
    scanner = Mock(skip_level0=True)
    with patch.object(PythonTableScan, '_create_file_scanner', return_value=scanner) as create:
        assert PythonTableScan(scan)._file_scanner() is scanner
        assert PythonTableScan(scan)._scanner(None) is scanner
    create.assert_called_once()
    configure.assert_called_once_with(scanner)
    scanner.with_shard.assert_called_once_with(1, 3)
    ranges, = scanner.with_row_ranges.call_args.args
    assert [(value.from_, value.to) for value in ranges] == [(7, 9)]
    assert scan._python_file_scanner is scanner and not scanner.skip_level0
