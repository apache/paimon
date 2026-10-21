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

"""The two streaming executors implement the same state and lifecycle API."""

import asyncio
from concurrent.futures import Future, ThreadPoolExecutor
import json
import subprocess
import sys
import threading
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.native.runtime import native_runtime_available
from pypaimon.read.plan import Plan


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
_ENGINES = [False, pytest.param(True, marks=[
    pytest.mark.native_plan,
    pytest.mark.skipif(not native_runtime_available(), reason='pypaimon-rust not installed'),
])]


def test_streaming_executors_declare_the_same_control_protocol():
    # CI instruments Native plan() with a variadic wrapper. Inspect the real
    # declarations in a fresh interpreter rather than that test-only wrapper.
    script = """
import inspect
from pypaimon.execution.native.read.streaming_table_scan import StreamTableScan
from pypaimon.execution.python.read.streaming_table_scan import AsyncStreamingTableScan
for name in ('plan', 'checkpoint', 'restore', 'watermark',
             'notify_checkpoint_complete', 'close', 'stream', 'stream_sync'):
    assert inspect.signature(getattr(AsyncStreamingTableScan, name)) == inspect.signature(
        getattr(StreamTableScan, name)), name
"""
    result = subprocess.run([sys.executable, '-c', script], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, universal_newlines=True)
    assert result.returncode == 0, result.stdout


def _table(tmp_path, native):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', True)
    catalog.create_table('db.t', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int32()), ('value', pa.int32())]), primary_keys=['id'], options={
            'bucket': '1', 'changelog-producer': 'input',
            'write.native.enabled': 'false', 'commit.native.enabled': 'false',
            'read.native.enabled': 'false', 'scan.native-plan.enabled': str(native).lower(),
        }), False)
    return catalog.get_table('db.t')


def _write(table, value, watermark=None):
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'id': pa.array([1], pa.int32()),
                                    'value': pa.array([value], pa.int32())}))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    manager = table.snapshot_manager()
    snapshot = manager.get_latest_snapshot()
    if watermark is not None:
        path = manager.get_snapshot_path(snapshot.id)
        metadata = json.loads(table.file_io.read_file_utf8(path))
        metadata['watermark'] = watermark
        table.file_io.overwrite_file_utf8(path, json.dumps(metadata))
    return snapshot.id


@pytest.mark.parametrize('native', _ENGINES)
def test_control_methods_share_checkpoints_watermarks_and_explicit_ack(tmp_path, native):
    table = _table(tmp_path, native)
    _write(table, 10, watermark=100)
    _write(table, 20, watermark=200)
    scan = table.new_stream_read_builder().with_consumer_id('job').new_streaming_scan()
    try:
        assert scan.checkpoint() is None
        assert scan.watermark() is None
        assert scan.plan().snapshot_id == 2
        assert scan.checkpoint() == scan.next_snapshot_id == 3
        assert scan.watermark() == 200
        assert table.consumer_manager().consumer('job') is None
        scan.notify_checkpoint_complete(scan.checkpoint())
        assert table.consumer_manager().consumer('job').next_snapshot == 3
        scan.restore(1)
        assert scan.plan().snapshot_id == 1
        assert scan.checkpoint() == 2
        assert scan.watermark() == 100
        scan.next_snapshot_id = 2
        assert scan.plan().snapshot_id == 2
        assert scan.plan() is None
        scan.restore()
        assert scan.plan() is None  # Restores the acknowledged consumer, not the initial full state.
        assert scan.checkpoint() == 3
        with pytest.raises(ValueError, match='positive'):
            scan.restore(0)
        with pytest.raises(ValueError, match='positive'):
            scan.notify_checkpoint_complete(0)
        with pytest.raises(OverflowError):
            scan.restore(1 << 63)
        with pytest.raises(TypeError):
            scan.restore(1.5)
    finally:
        scan.close()
    scan.close()
    with pytest.raises(RuntimeError, match='closed'):
        scan.plan()
    with pytest.raises(RuntimeError, match='closed'):
        scan.restore(1)


@pytest.mark.parametrize('native', _ENGINES)
def test_large_backlog_keeps_each_physical_changelog_frame(tmp_path, native):
    table = _table(tmp_path, native)
    for value in range(15):
        _write(table, value)
    scan = table.new_stream_read_builder().new_streaming_scan()
    try:
        scan.restore(1)
        assert [scan.plan().snapshot_id for _ in range(15)] == list(range(1, 16))
        assert scan.checkpoint() == 16
        assert scan.plan() is None
    finally:
        scan.close()


@pytest.mark.parametrize('native', _ENGINES)
def test_empty_follow_up_is_not_emitted_and_retains_retryable_progress(tmp_path, native):
    table = _table(tmp_path, native)
    for value in range(3):
        _write(table, value, watermark=value * 10)
    scan = (table.new_stream_read_builder().with_consumer_id('empty')
            .with_bucket_filter(lambda _: False).new_streaming_scan())
    try:
        scan.restore(1)
        assert scan.plan() is None
        assert scan.checkpoint() == 4
        assert scan.watermark() == 20
        assert table.consumer_manager().consumer('empty') is None
        scan.notify_checkpoint_complete(4)
        assert table.consumer_manager().consumer('empty').next_snapshot == 4
    finally:
        scan.close()


@pytest.mark.parametrize('native', _ENGINES)
def test_cancelled_yield_is_not_acknowledged_by_a_new_iterator(tmp_path, native):
    table = _table(tmp_path, native)
    _write(table, 10)
    scan = (table.new_stream_read_builder().with_consumer_id('cancelled')
            .with_poll_interval_ms(1).new_streaming_scan())

    async def scenario():
        iterator = scan.stream()
        assert (await iterator.__anext__()).snapshot_id == 1
        assert scan._pending_consumer_snapshot == 2
        await iterator.aclose()
        assert scan._pending_consumer_snapshot is None
        resumed = scan.stream()
        try:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(resumed.__anext__(), timeout=0.02)
            assert table.consumer_manager().consumer('cancelled') is None
        finally:
            await resumed.aclose()

    try:
        asyncio.run(scenario())
    finally:
        scan.close()


@pytest.mark.parametrize('native', _ENGINES)
def test_old_iterator_close_does_not_clear_a_new_iterator_pending_frame(tmp_path, native):
    table = _table(tmp_path, native)
    _write(table, 10)
    scan = table.new_stream_read_builder().with_consumer_id('owners').new_streaming_scan()

    async def scenario():
        old = scan.stream()
        current = scan.stream()
        try:
            assert (await old.__anext__()).snapshot_id == 1
            _write(table, 20)
            assert (await current.__anext__()).snapshot_id == 2
            assert table.consumer_manager().consumer('owners').next_snapshot == 2
            await old.aclose()
            assert scan._pending_consumer_snapshot == 3
            await current.aclose()
            assert scan._pending_consumer_snapshot is None
            assert table.consumer_manager().consumer('owners').next_snapshot == 2
        finally:
            await old.aclose()
            await current.aclose()

    try:
        asyncio.run(scenario())
    finally:
        scan.close()


@pytest.mark.parametrize('native', _ENGINES)
def test_sync_iterator_close_discards_unacknowledged_frame(tmp_path, native):
    table = _table(tmp_path, native)
    _write(table, 10)
    scan = table.new_stream_read_builder().with_consumer_id('sync-close').new_streaming_scan()
    iterator = scan.stream_sync()
    try:
        assert next(iterator).snapshot_id == 1
        assert scan._pending_consumer_snapshot == 2
        iterator.close()
        assert scan._pending_consumer_snapshot is None
        assert table.consumer_manager().consumer('sync-close') is None
    finally:
        iterator.close()
        scan.close()


@pytest.mark.parametrize('native', _ENGINES)
def test_ack_failure_keeps_pending_checkpoint_for_retry(tmp_path, native):
    table = _table(tmp_path, native)
    _write(table, 10)
    scan = (table.new_stream_read_builder().with_consumer_id('ack-retry')
            .with_poll_interval_ms(1).new_streaming_scan())

    async def scenario():
        iterator = scan.stream()
        assert (await iterator.__anext__()).snapshot_id == 1
        failure = OSError('consumer storage unavailable')
        with patch.object(scan, 'notify_checkpoint_complete', side_effect=failure):
            with pytest.raises(OSError) as caught:
                await iterator.__anext__()
            assert caught.value is failure
        assert scan._pending_consumer_snapshot == 2
        assert table.consumer_manager().consumer('ack-retry') is None
        retry = scan.stream()
        try:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(retry.__anext__(), timeout=0.02)
            assert table.consumer_manager().consumer('ack-retry').next_snapshot == 2
        finally:
            await retry.aclose()
            await iterator.aclose()

    try:
        asyncio.run(scenario())
    finally:
        scan.close()


def test_prefetch_failure_is_not_hidden_or_advanced(tmp_path):
    table = _table(tmp_path, False)
    _write(table, 10)
    scan = table.new_stream_read_builder().new_streaming_scan()
    scan.restore(1)
    future = Future()
    failure = OSError('prefetch manifest unavailable')
    future.set_exception(failure)
    scan._prefetch_future = future
    try:
        with pytest.raises(OSError) as caught:
            scan.plan()
        assert caught.value is failure
        assert scan.checkpoint() == 1
        assert scan.plan().snapshot_id == 1
        assert scan.checkpoint() == 2
    finally:
        scan.close()


def test_restoring_during_prefetch_does_not_apply_its_stale_result(tmp_path):
    table = _table(tmp_path, False)
    scan = table.new_stream_read_builder().new_streaming_scan()
    scan.restore(1)
    future = Future()
    started = threading.Event()
    original_result = future.result

    def result():
        started.set()
        return original_result(timeout=5)

    future.result = result
    scan._prefetch_future = future
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            planning = executor.submit(scan.plan)
            assert started.wait(5)
            scan.restore(7)
            future.set_result((Plan([object()], snapshot_id=1), 2, 0, 100))
            with pytest.raises(RuntimeError, match='restored during planning'):
                planning.result(timeout=5)
        assert scan.checkpoint() == 7
        assert scan.watermark() is None
        assert scan._prefetch_future is None
    finally:
        scan.close()


def test_native_plan_conversion_failure_retries_the_same_frame():
    from pypaimon.execution.native.read.streaming_table_scan import StreamTableScan

    class CoreScan:
        checkpoint_id = 1
        last_watermark = None

        def checkpoint(self):
            return self.checkpoint_id

        def restore(self, checkpoint):
            self.checkpoint_id = checkpoint

        def watermark(self):
            return self.last_watermark

        def plan(self):
            assert self.checkpoint_id == 1
            self.checkpoint_id = 2
            self.last_watermark = 100
            return object()

    with patch('pypaimon.execution.native.read.streaming_table_scan.native_stream_scan', return_value=CoreScan()):
        scan = StreamTableScan(object())
    plan = Plan([object()], snapshot_id=1)
    try:
        with patch('pypaimon.execution.native.read.streaming_table_scan._from_native_plan',
                   side_effect=[ValueError('bad split'), plan]):
            with pytest.raises(ValueError, match='bad split'):
                scan.plan()
            assert scan.checkpoint() == 1
            assert scan.watermark() is None
            assert scan.plan() is plan
            assert scan.checkpoint() == 2
            assert scan.watermark() == 100
    finally:
        scan.close()


@pytest.mark.parametrize('old_watermark', [None, 50])
@pytest.mark.parametrize('next_poll', ['wait', 'irrelevant', 'empty'])
def test_failed_decode_restores_watermark_before_later_none_or_empty_poll(old_watermark, next_poll):
    from pypaimon.execution.native.read.streaming_table_scan import StreamTableScan

    class CoreScan:
        def __init__(self):
            self.next_id = 1
            self.last_watermark = old_watermark

        def checkpoint(self):
            return self.next_id

        def watermark(self):
            return self.last_watermark

        def restore(self, checkpoint):
            self.next_id = checkpoint

        def plan(self):
            if self.next_id == 1:
                self.next_id = 2
                self.last_watermark = 100
                return object()
            if next_poll != 'wait':
                self.next_id = 3
            if next_poll == 'empty':
                # A legitimate empty scannable frame has exactly the same
                # watermark as the previously failed conversion.
                self.last_watermark = 100
            return None

    with patch('pypaimon.execution.native.read.streaming_table_scan.native_stream_scan', return_value=CoreScan()):
        scan = StreamTableScan(object())
    try:
        with patch('pypaimon.execution.native.read.streaming_table_scan._from_native_plan',
                   side_effect=ValueError('bad split')):
            with pytest.raises(ValueError, match='bad split'):
                scan.plan()
        assert scan.checkpoint() == 1
        assert scan.watermark() == old_watermark
        scan.restore(2)
        assert scan.plan() is None
        assert scan.watermark() == (100 if next_poll == 'empty' else old_watermark)
        assert scan.checkpoint() == (2 if next_poll == 'wait' else 3)
    finally:
        scan.close()


@pytest.mark.parametrize('native', _ENGINES)
@pytest.mark.parametrize('case', ['expired', 'future', 'empty', 'next'])
def test_missing_checkpoint_periodically_distinguishes_publication_from_expiration(tmp_path, native, case):
    table = _table(tmp_path, native)
    if case != 'empty':
        _write(table, 10)
        _write(table, 20)
    manager = table.snapshot_manager()
    if case == 'expired':
        table.file_io.delete(manager.get_snapshot_path(1))
        table.file_io.overwrite_file_utf8(manager.snapshot_dir + '/EARLIEST', '2')
    checkpoint = 1 if case == 'expired' else 3 if case == 'next' else 4
    scan = table.new_stream_read_builder().new_streaming_scan()
    try:
        scan.restore(checkpoint)
        for _ in range(15):
            assert scan.plan() is None
        if case == 'next':
            assert scan.plan() is None
            _write(table, 30)
            assert scan.plan().snapshot_id == 3
            assert scan.checkpoint() == 4
            # A selected frame resets the missing-snapshot counter.
            for _ in range(16):
                assert scan.plan() is None
        else:
            with pytest.raises(ValueError, match='outside the available snapshots'):
                scan.plan()
        scan.restore(4 if case == 'next' else checkpoint)
        assert scan.plan() is None  # Restore also resets the periodic check.
    finally:
        scan.close()


@pytest.mark.parametrize('result', [None, object()])
def test_native_close_during_planning_does_not_return_or_restore_a_frame(result):
    from pypaimon.execution.native.read.streaming_table_scan import StreamTableScan

    entered, release = threading.Event(), threading.Event()

    class CoreScan:
        restored = []

        def checkpoint(self):
            return 1

        def plan(self):
            entered.set()
            assert release.wait(5)
            return result

        def restore(self, checkpoint):
            self.restored.append(checkpoint)

    core = CoreScan()
    with patch('pypaimon.execution.native.read.streaming_table_scan.native_stream_scan', return_value=core):
        scan = StreamTableScan(object())
    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            planning = executor.submit(scan.plan)
            assert entered.wait(5)
            scan.close()
            release.set()
            with pytest.raises(RuntimeError, match='closed'):
                planning.result(timeout=5)
        assert core.restored == []
    finally:
        release.set()
        scan.close()
