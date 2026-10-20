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

"""Streaming consumer checkpoints belong to the selected table branch."""

import asyncio
from contextlib import ExitStack
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.common.identifier import Identifier
from pypaimon.consumer.consumer import Consumer
from pypaimon.execution.native.runtime import (native_runtime_available)
from pypaimon.read.streaming_table_scan import AsyncStreamingTableScan, StreamTableScan
from pypaimon.tests import native_plan_rest_test


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
rest_catalog = native_plan_rest_test.rest_catalog
_ENGINES = [False, pytest.param(True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed')])]


def _append(table, value):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'id': [value]}, schema=pa.schema([('id', pa.int32())])))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()


def _tables(catalog, native, branch_snapshot):
    catalog.create_table('default.t', Schema.from_pyarrow_schema(pa.schema([('id', pa.int32())]), options={
        'file.format': 'parquet', 'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
    }), False)
    main = catalog.get_table('default.t')
    _append(main, 1)
    _append(main, 2)
    catalog.create_tag(main.identifier, 'base', branch_snapshot)
    for branch in ('dev', 'other'):
        catalog.create_branch(main.identifier, branch, tag_name='base')
    view = catalog.get_table(Identifier('default', 't', branch='dev')).copy({
        'scan.native-plan.enabled': str(native).lower(), 'read.native.enabled': str(native).lower()})
    sibling = catalog.get_table(Identifier('default', 't', branch='other'))
    return main, view, sibling


def _read_and_acknowledge(table, native):
    builder = table.new_stream_read_builder().with_consumer_id('job').with_poll_interval_ms(1)
    scan = builder.new_streaming_scan()
    assert isinstance(scan, StreamTableScan if native else AsyncStreamingTableScan)
    if not native:
        # Keep this regression independent of the optional prefetch worker.
        scan._prefetch_executor.shutdown(wait=True)
        scan._prefetch_executor = None
        scan._prefetch_enabled = False

    async def consume():
        iterator = scan.stream()
        try:
            plan = await asyncio.wait_for(iterator.__anext__(), timeout=5)
            reader = builder.new_read()
            with ExitStack() as guards:
                if native:
                    guards.enter_context(patch.object(reader, '_create_split_read',
                                                      side_effect=AssertionError('Python read fallback')))
                rows = reader.to_arrow(plan.splits()).to_pylist()
            # Resuming acknowledges the preceding plan before polling for data.
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(iterator.__anext__(), timeout=0.02)
            return plan.snapshot_id, rows, scan.next_snapshot_id
        finally:
            await iterator.aclose()

    with ExitStack() as guards:
        if native:
            guards.enter_context(patch('pypaimon.execution.python.read.streaming_table_scan.FileScanner',
                                       side_effect=AssertionError('Python initial plan fallback')))
            guards.enter_context(patch('pypaimon.execution.python.read.streaming_table_scan.ManifestFileManager',
                                       side_effect=AssertionError('Python incremental plan fallback')))
        return asyncio.run(consume())


@pytest.mark.parametrize('native', _ENGINES)
def test_branch_initial_stream_ignores_another_branches_consumer(rest_catalog, native):
    catalog, _ = rest_catalog
    main, branch, sibling = _tables(catalog, native, 1)
    main.consumer_manager().reset_consumer('job', Consumer(3))
    sibling.consumer_manager().reset_consumer('job', Consumer(4))
    assert branch.consumer_manager().consumer('job') is None

    assert _read_and_acknowledge(branch, native) == (1, [{'id': 1}], 2)
    assert branch.consumer_manager().consumer('job').next_snapshot == 2
    assert main.consumer_manager().consumer('job').next_snapshot == 3
    assert sibling.consumer_manager().consumer('job').next_snapshot == 4


@pytest.mark.parametrize('native', _ENGINES)
def test_branch_stream_restores_only_its_own_saved_checkpoint(rest_catalog, native):
    catalog, _ = rest_catalog
    main, branch, sibling = _tables(catalog, native, 2)
    main.consumer_manager().reset_consumer('job', Consumer(1))
    branch.consumer_manager().reset_consumer('job', Consumer(2))
    sibling.consumer_manager().reset_consumer('job', Consumer(4))

    assert _read_and_acknowledge(branch, native) == (2, [{'id': 2}], 3)
    assert branch.consumer_manager().consumer('job').next_snapshot == 3
    assert main.consumer_manager().consumer('job').next_snapshot == 1
    assert sibling.consumer_manager().consumer('job').next_snapshot == 4
