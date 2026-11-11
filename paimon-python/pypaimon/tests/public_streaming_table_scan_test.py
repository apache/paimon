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

"""Direct streaming constructors and builders share backend selection."""

import asyncio
from contextlib import ExitStack
from inspect import Parameter, signature
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.native.read.streaming_table_scan import StreamTableScan as NativeStreamScan
from pypaimon.execution.python.read.streaming_table_scan import AsyncStreamingTableScan as PythonStreamScan
from pypaimon.read.plan import Plan
from pypaimon.read.streaming_table_scan import AsyncStreamingTableScan, StreamTableScan


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]


@pytest.fixture
def table(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.stream_facade', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int32())]), options={
            'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
            'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        }), False)
    return catalog.get_table('default.stream_facade')


def _create(table, entrypoint):
    if entrypoint == 'builder':
        return table.new_stream_read_builder().new_streaming_scan()
    return entrypoint(table)


@pytest.mark.parametrize('public,execution', [
    (AsyncStreamingTableScan, PythonStreamScan), (StreamTableScan, NativeStreamScan),
])
def test_standard_constructor_arguments_and_defaults_remain_stable(public, execution):
    def parameters(cls):
        return [parameter.replace(annotation=Parameter.empty)
                for parameter in signature(cls).parameters.values()
                if parameter.name != 'follow_up_scanner']
    assert parameters(public) == parameters(execution)


@pytest.mark.parametrize('entrypoint', [AsyncStreamingTableScan, StreamTableScan, 'builder'])
@pytest.mark.parametrize('native_plan,native_read', [(True, False), (False, True), (True, True)])
def test_all_streaming_entrypoints_select_native_without_python_resources(
        table, entrypoint, native_plan, native_read):
    selected = table.copy({'scan.native-plan.enabled': str(native_plan).lower(),
                           'read.native.enabled': str(native_read).lower()})
    core = Mock()
    expected = Plan([], 7)
    with ExitStack() as stack:
        stack.enter_context(patch(
            'pypaimon.execution.native.runtime.native_runtime_available', return_value=True))
        for resource in ('FileScanner', 'ManifestListManager', 'ManifestFileManager'):
            target = 'pypaimon.execution.python.read.streaming_table_scan.' + resource
            stack.enter_context(patch(target, side_effect=AssertionError(target)))
        stack.enter_context(patch(
            'pypaimon.execution.native.read.streaming_table_scan.native_stream_scan', return_value=core))
        stack.enter_context(patch(
            'pypaimon.execution.native.read.streaming_table_scan._from_native_plan', return_value=expected))
        scan = _create(selected, entrypoint)
        try:
            assert isinstance(scan._execution, NativeStreamScan)
            assert scan.plan() is expected
        finally:
            scan.close()
    core.plan.assert_called_once()


@pytest.mark.parametrize('entrypoint', [AsyncStreamingTableScan, StreamTableScan, 'builder'])
@pytest.mark.parametrize('failure', ['disabled', 'unavailable', 'preparation'])
def test_streaming_fallback_is_selected_before_planning(table, entrypoint, failure):
    selected = table.copy({'read.native.enabled': str(failure != 'disabled').lower()})
    with patch('pypaimon.execution.native.runtime.native_runtime_available',
               return_value=failure == 'preparation') as runtime, patch(
            'pypaimon.execution.native.read.streaming_table_scan.native_stream_scan',
            side_effect=ImportError('optional runtime unavailable')) as native:
        scan = _create(selected, entrypoint)
        try:
            assert isinstance(scan._execution, PythonStreamScan)
            assert scan.plan() is None
        finally:
            scan.close()
    assert native.call_count == int(failure == 'preparation')
    assert runtime.call_count == int(failure != 'disabled')


@pytest.mark.parametrize('entrypoint', [AsyncStreamingTableScan, StreamTableScan, 'builder'])
def test_planning_errors_never_switch_to_python(table, entrypoint):
    failure = OSError('native frame unavailable')
    core = Mock()
    core.plan.side_effect = failure
    with patch('pypaimon.execution.native.read.streaming_table_scan.native_stream_scan', return_value=core), \
            patch('pypaimon.execution.python.read.streaming_table_scan.AsyncStreamingTableScan',
                  side_effect=AssertionError('late Python fallback')):
        scan = _create(table.copy({'scan.native-plan.enabled': 'true'}), entrypoint)
        try:
            with pytest.raises(OSError) as caught:
                scan.plan()
            assert caught.value is failure
        finally:
            scan.close()
    core.plan.assert_called_once()


def test_public_streaming_does_not_expose_scanner_injection(table):
    with pytest.raises(TypeError, match='follow_up_scanner'):
        AsyncStreamingTableScan(table, follow_up_scanner=object())


def test_python_constructor_controls_do_not_force_python_execution(table):
    core = Mock()
    with patch('pypaimon.execution.native.read.streaming_table_scan.native_stream_scan', return_value=core):
        scan = AsyncStreamingTableScan(
            table.copy({'scan.native-plan.enabled': 'true'}), prefetch_enabled=False, diff_threshold=3)
    try:
        assert isinstance(scan._execution, NativeStreamScan)
    finally:
        scan.close()


@pytest.mark.parametrize('public', [AsyncStreamingTableScan, StreamTableScan])
@pytest.mark.parametrize('synchronous', [False, True])
def test_polling_honors_public_control_overrides_and_cancellation(public, synchronous):
    acknowledgements = []

    class CustomScan(public):
        position = 0

        def plan(self):
            self.position += 1
            return Plan([], self.position)

        def checkpoint(self):
            return self.position

        def notify_checkpoint_complete(self, next_snapshot):
            acknowledgements.append(next_snapshot)

    execution = Mock()
    with patch('pypaimon.read.read_dispatch.create_streaming_scan', return_value=execution):
        scan = CustomScan(object())

    async def consume():
        iterator = scan.stream()
        try:
            assert (await iterator.__anext__()).snapshot_id == 1
            assert acknowledgements == []
            assert (await iterator.__anext__()).snapshot_id == 2
        finally:
            await iterator.aclose()

    try:
        if synchronous:
            iterator = scan.stream_sync()
            try:
                assert next(iterator).snapshot_id == 1
                assert acknowledgements == []
                assert next(iterator).snapshot_id == 2
            finally:
                iterator.close()
        else:
            asyncio.run(consume())
        assert acknowledgements == [1]
        assert scan._pending_consumer_snapshot is None
        execution.plan.assert_not_called()
        execution.checkpoint.assert_not_called()
        execution.notify_checkpoint_complete.assert_not_called()
        execution.cancel_prefetch.assert_called_once()
    finally:
        scan.close()
    execution.close.assert_called_once()
