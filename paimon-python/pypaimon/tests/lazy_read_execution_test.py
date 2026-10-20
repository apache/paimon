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

"""A selected Native backend never constructs Python execution resources."""

from contextlib import ExitStack
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.python.read.scanner.file_scanner import FileScanner
from pypaimon.read.plan import Plan
from pypaimon.utils.range import Range


@pytest.fixture
def table(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.lazy_execution', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int32())]), options={
            'scan.native-plan.enabled': 'true', 'read.native.enabled': 'true',
            'data-evolution.enabled': 'true', 'row-tracking.enabled': 'true',
        }), False)
    return catalog.get_table('default.lazy_execution')


@pytest.mark.parametrize('method', ['plan', 'plan_all_files', 'scan_with_stats'])
@pytest.mark.parametrize('unrestricted_auth', [False, True])
def test_native_scan_does_not_construct_python_executors(table, method, unrestricted_auth):
    with ExitStack() as stack:
        for target in (
                'pypaimon.execution.python.read.table_scan.FileScanner',
                'pypaimon.execution.python.read.table_scan.ManifestListManager',
                'pypaimon.execution.python.read.scanner.file_scanner.ManifestFileManager'):
            stack.enter_context(patch(target, side_effect=AssertionError(target)))
        stack.enter_context(patch(
            'pypaimon.execution.native.runtime.native_runtime_available', return_value=True))
        native = stack.enter_context(patch(
            'pypaimon.execution.native.read.plan.native_plan', return_value=Plan([], 7)))
        scan = table.new_read_builder().new_scan()
        if unrestricted_auth:
            scan._query_auth_fn = object()
            stack.enter_context(patch('pypaimon.read.table_scan.authorize', return_value=None))
        scan.with_shard(1, 3).with_row_ranges([Range(1, 4), Range(3, 8)])
        result = getattr(scan, method)()
    plan = result[0] if method == 'scan_with_stats' else result
    assert plan.snapshot_id == 7
    assert native.call_args.kwargs['row_ranges'] == [(1, 8)]
    assert native.call_args.kwargs['row_position_shard'] == (1, 3)


def test_python_scanner_is_constructed_only_after_native_preparation_fails(table):
    with patch('pypaimon.execution.python.read.table_scan.FileScanner', wraps=FileScanner) as create, \
            patch('pypaimon.read.table_scan.TableScan._native_plan_supported', return_value=True), \
            patch('pypaimon.execution.native.read.plan.native_plan', side_effect=RuntimeError('prepare')):
        scan = table.new_read_builder().new_scan().with_shard(1, 3).with_row_ranges([Range(1, 4)])
        create.assert_not_called()
        assert scan.plan().splits() == []
        create.assert_called_once()
        assert scan.file_scanner.idx_of_this_subtask == 1
        assert scan.file_scanner.number_of_para_subtasks == 3
        assert [(r.from_, r.to) for r in scan.file_scanner._row_ranges] == [(1, 4)]


def test_explicit_scanner_access_and_mutation_remain_visible_to_native(table):
    scan = table.new_read_builder().new_scan()
    scanner = scan.file_scanner
    assert scanner is scan.file_scanner
    scanner.with_slice(2, 9).with_row_ranges([Range(20, 30)])
    with patch('pypaimon.execution.native.read.plan.native_plan', return_value=Plan([], 7)) as native:
        scan._try_native_plan()
    assert native.call_args.kwargs['row_position_slice'] == (2, 9)
    assert native.call_args.kwargs['row_ranges'] == [(20, 30)]


@pytest.mark.parametrize('end', ['close', 'error'])
def test_native_read_never_initializes_or_replays_python_read(table, end):
    events = []
    batch = pa.record_batch([pa.array([1], type=pa.int32())], names=['id'])

    def batches():
        try:
            yield batch
            raise RuntimeError('native stream failed')
        finally:
            events.append('closed')

    split = SimpleNamespace(files=[SimpleNamespace(file_name='data.parquet')], _native_split=object())
    with ExitStack() as stack:
        for target in (
                'pypaimon.execution.python.read.merge_engine_support.check_supported',
                'pypaimon.execution.python.read.table_read.RawFileSplitRead',
                'pypaimon.execution.python.read.table_read.MergeFileSplitRead',
                'pypaimon.execution.python.read.table_read.DataEvolutionSplitRead'):
            stack.enter_context(patch(target, side_effect=AssertionError(target)))
        stack.enter_context(patch('pypaimon.execution.native.read.table_read.native_read', return_value=batches()))
        reader = table.new_read_builder().new_read().to_arrow_batch_reader([split], parallelism=1)
        assert reader.read_next_batch().to_pydict() == {'id': [1]}
        if end == 'close':
            reader.close()
        else:
            with pytest.raises(RuntimeError, match='native stream failed'):
                reader.read_next_batch()
    assert events == ['closed']


def test_native_stream_scan_does_not_construct_python_executors(table):
    with ExitStack() as stack:
        for target in (
                'pypaimon.execution.python.read.streaming_table_scan.FileScanner',
                'pypaimon.execution.python.read.streaming_table_scan.ManifestListManager',
                'pypaimon.execution.python.read.streaming_table_scan.ManifestFileManager'):
            stack.enter_context(patch(target, side_effect=AssertionError(target)))
        core = stack.enter_context(patch(
            'pypaimon.execution.native.read.streaming_table_scan.native_stream_scan')).return_value
        expected = Plan([], 7)
        stack.enter_context(patch(
            'pypaimon.execution.native.read.streaming_table_scan._from_native_plan', return_value=expected))
        scan = table.new_stream_read_builder().new_streaming_scan()
        assert scan.plan() is expected
        core.plan.assert_called_once()


def test_python_read_resources_are_created_after_native_setup_failure(table):
    from pypaimon.execution.python.read.merge_engine_support import check_supported
    from pypaimon.execution.python.read.split_read import RawFileSplitRead

    table = table.copy({
        'data-evolution.enabled': 'false', 'row-tracking.enabled': 'false',
        'write.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'id': pa.array([1, 2], type=pa.int32())}))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    read_builder = table.new_read_builder()
    splits = read_builder.new_scan().plan().splits()
    assert splits
    for split in splits:
        split._native_split = object()
    with patch('pypaimon.execution.python.read.merge_engine_support.check_supported', wraps=check_supported) as check, \
            patch('pypaimon.execution.python.read.table_read.RawFileSplitRead', wraps=RawFileSplitRead) as create, \
            patch('pypaimon.execution.native.read.table_read.native_read', side_effect=RuntimeError('prepare')):
        read = read_builder.new_read()
        check.assert_not_called()
        create.assert_not_called()
        assert read.to_arrow(splits, parallelism=1).to_pydict() == {'id': [1, 2]}
        check.assert_called_once_with(table)
        create.assert_called_once()


@pytest.mark.parametrize('configure', [
    lambda scan: scan.with_shard(1, 1),
    lambda scan: scan.with_slice(-1, 3),
    lambda scan: scan.with_shard(0, 2).with_slice(0, 2),
    lambda scan: scan.with_slice(0, 2).with_shard(0, 2),
    lambda scan: scan.with_chunk_shuffle('seed', 2),
    lambda scan: scan.with_chunk_shuffle(1, 0),
    lambda scan: scan.with_row_ranges(None),
    lambda scan: scan.with_row_ranges([]).with_global_index_result(object()),
    lambda scan: scan.with_global_index_result(object()).with_row_ranges([]),
])
def test_public_scan_input_validation_does_not_need_a_python_scanner(table, configure):
    with patch('pypaimon.execution.python.read.table_scan.FileScanner',
               side_effect=AssertionError('Python scanner constructed')):
        scan = table.new_read_builder().new_scan()
        with pytest.raises(ValueError):
            configure(scan)
