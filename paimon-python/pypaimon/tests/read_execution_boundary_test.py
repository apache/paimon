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

"""Read entry points load only the selected execution backend."""

import subprocess
import sys
import textwrap
from unittest.mock import Mock, patch

import pytest


def _run(script):
    result = subprocess.run(
        [sys.executable, '-c', textwrap.dedent(script)],
        capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_native_read_scan_and_stream_preparation_do_not_import_python_execution():
    _run("""
        import importlib.abc
        import sys
        import tempfile
        from types import SimpleNamespace
        from unittest.mock import patch

        class RejectPythonRead(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path=None, target=None):
                if fullname.startswith('pypaimon.execution.python'):
                    raise AssertionError('Python execution imported: ' + fullname)

        sys.meta_path.insert(0, RejectPythonRead())
        import pyarrow as pa
        from pypaimon import CatalogFactory, Schema
        from pypaimon.read.plan import Plan
        from pypaimon.read.table_read import TableRead
        from pypaimon.read.table_scan import TableScan
        from pypaimon.read.stream_read_builder import StreamReadBuilder

        assert TableRead.__bases__ == (object,)
        assert TableScan.__bases__ == (object,)
        with tempfile.TemporaryDirectory() as path:
            catalog = CatalogFactory.create({'warehouse': path})
            catalog.create_database('default', True)
            catalog.create_table('default.lazy', Schema.from_pyarrow_schema(
                pa.schema([('id', pa.int32())]), options={
                    'read.native.enabled': 'true', 'scan.native-plan.enabled': 'true',
                }), False)
            table = catalog.get_table('default.lazy')
            builder = table.new_read_builder()
            with patch('pypaimon.execution.native.runtime.native_runtime_available', return_value=True), \
                    patch('pypaimon.execution.native.read.plan.native_plan', return_value=Plan([], 7)):
                assert builder.new_scan().plan().snapshot_id == 7
                assert builder.new_scan().plan_all_files().snapshot_id == 7
                assert builder.new_scan().scan_with_stats()[0].snapshot_id == 7
            reader = builder.new_read()
            assert reader.to_arrow([]).num_rows == 0
            batches = reader.to_arrow_batch_reader([])
            batches.close()
            assert list(reader.to_iterator([])) == []
            native = SimpleNamespace(checkpoint=lambda: 8)
            with patch('pypaimon.execution.native.read.streaming_table_scan.native_stream_scan', return_value=native):
                stream = StreamReadBuilder(table).new_streaming_scan()
                assert stream.checkpoint() == 8
        assert not any(name.startswith('pypaimon.execution.python') for name in sys.modules)
    """)


@pytest.mark.parametrize('failure', [False, True])
def test_native_stream_failure_does_not_create_a_python_reader(failure):
    from types import SimpleNamespace
    import pyarrow as pa
    from pypaimon.read.table_read import TableRead

    schema = pa.schema([('id', pa.int32())])
    closed = []

    def batches():
        try:
            yield pa.record_batch([[1]], schema=schema)
            if failure:
                raise OSError('native stream failed')
        finally:
            closed.append(True)

    read = TableRead.__new__(TableRead)
    read.table = SimpleNamespace(options=SimpleNamespace(native_read_enabled=lambda: True))
    read._read_parallelism = 1
    read.limit = None
    read.include_row_kind = False
    read.predicate = None
    read._deferred_blob_fields = set()
    read._output_arrow_schema = lambda splits: schema
    read._try_native_batches = Mock(return_value=batches())
    with patch.dict(sys.modules, {'pypaimon.execution.python.read.table_read': None}):
        reader = read.to_arrow_batch_reader([], parallelism=1)
        assert reader.read_next_batch().to_pydict() == {'id': [1]}
        if failure:
            with pytest.raises(OSError, match='native stream failed'):
                reader.read_next_batch()
        reader.close()
    assert closed == [True]


def test_python_vector_executor_never_selects_the_native_vindex_facade():
    from pypaimon.execution.python.read.search.vector_search_read import _create_vector_reader

    expected = object()
    with patch('pypaimon.execution.python.globalindex.vindex.vindex_vector_global_index_reader.'
               'PythonVindexVectorGlobalIndexReader', return_value=expected) as create, \
            patch.dict(sys.modules, {'pypaimon.globalindex.vindex_reader': None}):
        assert _create_vector_reader('ivf-flat', 'io', 'path', ['meta'], {'nprobe': '2'}) is expected
    create.assert_called_once_with('io', 'path', ['meta'], {'nprobe': '2'})


@pytest.mark.parametrize('outcome', ['success', 'schema-error', 'stream-error'])
@pytest.mark.parametrize('close_error', [False, True])
def test_native_materialization_always_closes_a_retained_batch_source(outcome, close_error):
    import pyarrow as pa
    from pypaimon.read.table_read import TableRead

    schema = pa.schema([('id', pa.int32())])
    events = []
    primary = OSError('native stream failed')
    cleanup = RuntimeError('native close failed')

    def batches():
        try:
            batch_type = pa.int64() if outcome == 'schema-error' else pa.int32()
            yield pa.record_batch([pa.array([1], type=batch_type)], names=['id'])
            if outcome == 'stream-error':
                raise primary
        finally:
            events.append('generator-closed')

    class Source:
        def __init__(self):
            self.stream = batches()
            self.close_count = 0

        def __iter__(self):
            return self

        def __next__(self):
            return next(self.stream)

        def close(self):
            self.close_count += 1
            self.stream.close()
            if close_error:
                raise cleanup

    source = Source()
    read = TableRead.__new__(TableRead)
    read._read_parallelism = 1
    read.include_row_kind = False
    read._output_arrow_schema = lambda splits: schema
    read._try_native_batches = Mock(return_value=source)
    with patch.dict(sys.modules, {'pypaimon.execution.python.read.table_read': None}):
        if outcome == 'schema-error':
            with pytest.raises(TypeError, match="has type int64, expected int32"):
                read.to_arrow([], parallelism=1)
        elif outcome == 'stream-error':
            with pytest.raises(OSError) as caught:
                read.to_arrow([], parallelism=1)
            assert caught.value is primary
        elif close_error:
            with pytest.raises(RuntimeError) as caught:
                read.to_arrow([], parallelism=1)
            assert caught.value is cleanup
        else:
            assert read.to_arrow([], parallelism=1).to_pydict() == {'id': [1]}
    # Keep source and its generator reachable: cleanup must not depend on GC.
    assert source.close_count == 1
    assert source.stream.gi_frame is None
    assert events == ['generator-closed']


@pytest.mark.parametrize('failure', ['schema', 'stream'])
def test_native_converter_preserves_primary_failure_when_retained_source_close_fails(failure):
    import pyarrow as pa
    from pypaimon.read.table_read import TableRead

    schema = pa.schema([('id', pa.int32())])
    primary = OSError('primary native stream failure')
    closed = []

    class Source:
        close_count = 0

        def __init__(self):
            self.stream = self.batches()

        def batches(self):
            try:
                if failure == 'stream':
                    raise primary
                yield pa.record_batch([pa.array([1], type=pa.int64())], names=['id'])
            finally:
                closed.append(True)

        def __iter__(self):
            return self

        def __next__(self):
            return next(self.stream)

        def close(self):
            self.close_count += 1
            self.stream.close()
            raise RuntimeError('secondary close failure')

    source = Source()
    read = TableRead.__new__(TableRead)
    read._prepare_native_batch = lambda batch, schema: batch
    read._project_batch_to_output = lambda batch: batch
    converted = read._convert_native_batches(source, schema)
    with patch.dict(sys.modules, {'pypaimon.execution.python.read.table_read': None}):
        if failure == 'schema':
            with pytest.raises(TypeError, match='has type int64, expected int32'):
                read._batches_to_arrow(converted, schema)
        else:
            with pytest.raises(OSError) as caught:
                read._batches_to_arrow(converted, schema)
            assert caught.value is primary
    assert source.close_count == 1
    assert source.stream.gi_frame is None
    assert closed == [True]


def test_native_row_conversion_keeps_its_failure_when_closing_batches_also_fails():
    from types import SimpleNamespace
    import pyarrow as pa
    from pypaimon.read.table_read import TableRead
    from pypaimon.schema.data_types import AtomicType, DataField

    primary = ValueError('invalid native row batch')
    source = Mock(close=Mock(side_effect=OSError('secondary close failure')))
    read = TableRead.__new__(TableRead)
    read.table = SimpleNamespace(options=SimpleNamespace(native_read_enabled=lambda: True))
    read.read_type = [DataField(0, 'id', AtomicType('INT'))]
    read.output_projection = None
    read._output_arrow_schema = lambda splits: pa.schema([('id', pa.int32())])
    read._try_native_batches = Mock(return_value=source)
    with patch('pypaimon.execution.native.read.row_iterator.native_rows', side_effect=primary), \
            patch.dict(sys.modules, {'pypaimon.execution.python.read.table_read': None}):
        rows = read._try_native_rows([])
        with pytest.raises(ValueError) as caught:
            next(rows)
    assert caught.value is primary
    source.close.assert_called_once_with()


def _python_read_source(primary=None, cleanup=None):
    from types import SimpleNamespace
    import pyarrow as pa
    from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
    from pypaimon.schema.data_types import AtomicType, DataField
    from pypaimon.table.row.offset_row import OffsetRow

    schema = pa.schema([('id', pa.int32())])

    class Source(RecordBatchReader):
        close_count = 0
        read_count = 0

        def read_arrow_batch(self):
            self.read_count += 1
            if primary is not None:
                raise primary
            if self.read_count == 1:
                return pa.record_batch([pa.array([1], type=pa.int32())], schema=schema)
            return None

        def read_batch(self):
            self.read_count += 1
            if primary is not None:
                raise primary
            if self.read_count != 1:
                return None
            rows = iter([OffsetRow((1,), 0, 1)])
            return SimpleNamespace(next=lambda: next(rows, None))

        def close(self):
            self.close_count += 1
            if cleanup is not None:
                raise cleanup

    source = Source()
    read = SimpleNamespace(
        _check_python_merge_supported=lambda: None,
        _TableRead__create_reader_for_split=lambda *args, **kwargs: source,
        _begin_auth_read=lambda splits: None,
        _serialize_blob_views_as_descriptors=lambda batch, reader: batch,
        _project_batch_to_output=lambda batch: batch,
        output_projection=None,
        _adapter_read_type=[DataField(0, 'id', AtomicType('INT'))],
        include_row_kind=False,
        limit=None,
    )
    return read, source, schema


def _consume_python_read_path(path, read, schema):
    from concurrent.futures import ThreadPoolExecutor
    from pypaimon.execution.python.read.table_read import PythonTableReadMixin, _RemainingRows

    if path == 'rows':
        return list(PythonTableReadMixin._python_row_iterator(read, [object()], None))
    if path == 'serial':
        return list(PythonTableReadMixin._arrow_batch_generator(read, [object()], schema))
    with ThreadPoolExecutor(max_workers=1) as executor:
        return executor.submit(
            PythonTableReadMixin._read_one_split_to_batches,
            read, object(), schema, _RemainingRows(None),
        ).result(timeout=5)


@pytest.mark.parametrize('path', ['rows', 'serial', 'parallel-worker'])
@pytest.mark.parametrize('read_error', [False, True])
@pytest.mark.parametrize('close_error', [False, True])
def test_python_reader_preserves_primary_failure_and_closes_on_eof(path, read_error, close_error):
    primary = ValueError('primary Python read failure') if read_error else None
    cleanup = OSError('secondary Python close failure') if close_error else None
    read, source, schema = _python_read_source(primary, cleanup)
    expected = primary if primary is not None else cleanup
    if expected is not None:
        with pytest.raises(type(expected)) as caught:
            _consume_python_read_path(path, read, schema)
        assert caught.value is expected
    else:
        result = _consume_python_read_path(path, read, schema)
        assert len(result) == 1
        if path == 'rows':
            assert result[0].get_field(0) == 1
        else:
            assert result[0].column('id').to_pylist() == [1]
    # The source remains reachable, so close must be deterministic, not GC-driven.
    assert source.close_count == 1


@pytest.mark.parametrize('path', ['serial', 'parallel-worker'])
def test_python_batch_conversion_keeps_primary_failure_when_close_also_fails(path):
    primary = ValueError('invalid Python batch conversion')
    read, source, schema = _python_read_source(cleanup=OSError('secondary close failure'))
    read._serialize_blob_views_as_descriptors = Mock(side_effect=primary)
    with pytest.raises(ValueError) as caught:
        _consume_python_read_path(path, read, schema)
    assert caught.value is primary
    assert source.close_count == 1


@pytest.mark.parametrize('path', ['rows', 'serial'])
@pytest.mark.parametrize('close_error', [False, True])
def test_python_read_explicit_generator_close_keeps_cleanup_error_visible(path, close_error):
    from pypaimon.execution.python.read.table_read import PythonTableReadMixin

    cleanup = OSError('explicit close failure') if close_error else None
    read, source, schema = _python_read_source(cleanup=cleanup)
    if path == 'rows':
        iterator = PythonTableReadMixin._python_row_iterator(read, [object()], None)
    else:
        iterator = PythonTableReadMixin._arrow_batch_generator(read, [object()], schema)
    next(iterator)
    if close_error:
        with pytest.raises(OSError) as caught:
            iterator.close()
        assert caught.value is cleanup
    else:
        iterator.close()
    assert source.close_count == 1
    assert iterator.gi_frame is None
