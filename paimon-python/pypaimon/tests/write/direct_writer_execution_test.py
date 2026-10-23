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

"""Direct public writer construction shares the builder execution boundary."""

from contextlib import ExitStack, contextmanager
import inspect
from pathlib import Path
import subprocess
import sys
from unittest.mock import Mock, call, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.bucket_plan import PostponeBucketPlan
from pypaimon.write.postpone_batch_table_write import PostponeFixedBucketBatchTableWrite
from pypaimon.write.table_commit import BatchTableCommit, StreamTableCommit
from pypaimon.write.table_write import BatchTableWrite, StreamTableWrite, TableWrite


WRITERS = [TableWrite, BatchTableWrite, StreamTableWrite, PostponeFixedBucketBatchTableWrite]

pytestmark = [pytest.mark.python_plan, pytest.mark.python_read, pytest.mark.python_commit]


def _table(tmp_path, writer_class):
    postpone = issubclass(writer_class, PostponeFixedBucketBatchTableWrite)
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([('id', pa.int32()), ('value', pa.int32())])
    catalog.create_table('default.direct', Schema.from_pyarrow_schema(
        schema, primary_keys=['id'] if postpone else [], options={
            'bucket': '-2' if postpone else '-1', 'postpone.default-bucket-num': '2',
            'write.native.enabled': 'true', 'commit.native.enabled': 'false',
            'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
        }), False)
    return catalog.get_table('default.direct'), schema


def _construct(writer_class, table):
    if issubclass(writer_class, PostponeFixedBucketBatchTableWrite):
        return writer_class(table, 'direct-writer', bucket_plan=PostponeBucketPlan({(): 2}))
    return writer_class(table, 'direct-writer')


def _prepare(writer):
    if isinstance(writer, StreamTableWrite):
        return writer.prepare_commit(7)
    if isinstance(writer, BatchTableWrite):
        return writer.prepare_commit()
    return writer._prepare_commit(BATCH_COMMIT_IDENTIFIER)


@contextmanager
def _native_backend(forbid_python=True):
    """Use the real thin Native adapter without loading or building the SDK."""
    native_table, builder, core = Mock(), Mock(), Mock()
    for name in ('new_batch_write_builder', 'new_stream_write_builder',
                 'new_postpone_fixed_bucket_write_builder'):
        getattr(native_table, name).return_value = builder
    for name in ('with_commit_user', 'with_bucket_plan', 'with_overwrite'):
        getattr(builder, name).return_value = builder
    builder.new_write.return_value = core
    core.prepare_commit.return_value = ['wire-message']
    with ExitStack() as stack:
        stack.enter_context(patch(
            'pypaimon.execution.native.write.native_write.native_runtime_available', return_value=True))
        create = stack.enter_context(patch(
            'pypaimon.execution.native.write.native_write.create_native_write_table', return_value=native_table))
        stack.enter_context(patch(
            'pypaimon.execution.native.write.native_write.from_native_commit_messages', return_value=['message']))
        if forbid_python:
            stack.enter_context(patch(
                'pypaimon.execution.python.write.table_write.PythonTableWrite.initialize',
                side_effect=AssertionError('Native initialized Python execution')))
        yield core, create


@pytest.mark.parametrize('writer_class', WRITERS)
def test_direct_writer_selects_native_without_python_initialization(tmp_path, writer_class):
    table, schema = _table(tmp_path, writer_class)
    batch = pa.record_batch([[1], [10]], schema=schema)
    with _native_backend() as (core, create):
        writer = _construct(writer_class, table)
        try:
            assert type(writer) is writer_class
            writer.write_arrow_batch(batch)
            assert _prepare(writer) == ['message']
        finally:
            writer.close()
        create.assert_called_once_with(table)
    core.write_arrow.assert_called_once_with(batch, None)
    core.prepare_commit.assert_called_once_with(*((True, 7) if writer_class is StreamTableWrite else ()))
    core.close.assert_called_once_with()


@pytest.mark.parametrize('writer_class', WRITERS)
def test_direct_writer_metadata_inspection_does_not_select_python(tmp_path, writer_class):
    table, _ = _table(tmp_path, writer_class)
    schema = PyarrowFieldParser.from_paimon_schema(table.fields)
    with _native_backend() as (core, create):
        writer = _construct(writer_class, table)
        try:
            assert writer.table is table
            assert writer.commit_user == 'direct-writer'
            assert writer.static_partition is None
            assert writer.table_pyarrow_schema == schema
            assert writer.restore_snapshot_id is None
            assert not hasattr(writer, 'missing_writer_attribute')
            with pytest.raises(AttributeError):
                getattr(writer, 'missing_writer_attribute')
            if isinstance(writer, BatchTableWrite):
                assert writer.batch_committed is False
            create.assert_called_once_with(table)
            core.close.assert_not_called()
        finally:
            writer.close()


@pytest.mark.parametrize('writer_class', WRITERS)
@pytest.mark.parametrize('native', [False, True])
def test_direct_writer_public_api_is_backend_independent(tmp_path, writer_class, native):
    table, _ = _table(tmp_path, writer_class)
    schema = PyarrowFieldParser.from_paimon_schema(table.fields)
    with ExitStack() as stack:
        if native:
            stack.enter_context(_native_backend())
        else:
            stack.enter_context(patch(
                'pypaimon.execution.native.write.native_write.create_native_write', return_value=None))
        writer = _construct(writer_class, table)
        try:
            assert type(writer) is writer_class
            assert writer.table is table
            assert writer.commit_user == 'direct-writer'
            assert writer.static_partition is None
            assert writer.table_pyarrow_schema == schema
            assert writer.restore_snapshot_id is None
            if isinstance(writer, BatchTableWrite):
                assert writer.batch_committed is False
            else:
                assert not hasattr(writer, 'batch_committed')
            if writer_class is TableWrite:
                # The base API intentionally leaves preparation to its subclass.
                # Discovering optional methods must not leak the selected executor.
                assert not hasattr(writer, 'prepare_commit')
            else:
                parameters = list(inspect.signature(writer.prepare_commit).parameters.values())
                if writer_class is StreamTableWrite:
                    assert [parameter.name for parameter in parameters] == ['commit_identifier']
                    assert parameters[0].default is inspect.Parameter.empty
                else:
                    assert not parameters
        finally:
            writer.close()


@pytest.mark.parametrize('writer_class', WRITERS)
def test_direct_writer_native_write_failure_is_not_replayed(tmp_path, writer_class):
    table, schema = _table(tmp_path, writer_class)
    failure = OSError('file write may already have succeeded')
    with _native_backend() as (core, create):
        core.write_arrow.side_effect = failure
        writer = _construct(writer_class, table)
        try:
            with pytest.raises(OSError) as caught:
                writer.write_arrow_batch(pa.record_batch([[1], [10]], schema=schema))
            assert caught.value is failure
            create.assert_called_once_with(table)
        finally:
            writer.close()
    core.write_arrow.assert_called_once()


@pytest.mark.parametrize('writer_class', WRITERS)
def test_direct_native_writer_configuration_keeps_public_receiver(tmp_path, writer_class):
    table, _ = _table(tmp_path, writer_class)
    with _native_backend() as (core, _):
        writer = _construct(writer_class, table)
        try:
            assert writer.with_write_type(['id', 'value']) is writer
            assert writer.with_blob_consumer(None) is writer
            assert writer.with_blob_uri_reader_factory(None) is writer
            core.with_blob_consumer.assert_called_once_with(None)
            core.with_blob_uri_reader_factory.assert_called_once_with(None)
        finally:
            writer.close()


@pytest.mark.parametrize('writer_class', WRITERS)
def test_direct_writer_python_selection_is_not_revisited(tmp_path, writer_class):
    table, schema = _table(tmp_path, writer_class)
    data = pa.table({'id': [1], 'value': [10]}, schema=schema)
    with patch('pypaimon.execution.native.write.native_write.create_native_write', return_value=None) as prepare:
        writer = _construct(writer_class, table)
        try:
            assert type(writer) is writer_class
            writer.write_arrow(data)
            messages = _prepare(writer)
        finally:
            writer.close()
        prepare.assert_called_once()
    commit = (StreamTableCommit(table, 'direct-writer') if writer_class is StreamTableWrite else
              BatchTableCommit(table, 'direct-writer', None))
    try:
        commit.commit(messages, 7) if writer_class is StreamTableWrite else commit.commit(messages)
    finally:
        commit.close()
    read = table.new_read_builder()
    assert read.new_read().to_arrow(read.new_scan().plan().splits()).to_pylist() == data.to_pylist()


@pytest.mark.parametrize('writer_class', WRITERS)
@pytest.mark.parametrize('pandas', [False, True])
def test_direct_native_writer_preserves_public_batch_override(tmp_path, writer_class, pandas):
    table, schema = _table(tmp_path, writer_class)

    class ObservingWriter(writer_class):
        def write_arrow_batch(self, data, bucket=None):
            self.observed.append(data)
            return super().write_arrow_batch(data, bucket)

    with _native_backend() as (core, _):
        writer = _construct(ObservingWriter, table)
        writer.observed = []
        data = pa.table({'id': [1, 2], 'value': [10, 20]}, schema=schema)
        try:
            writer.write_pandas(data.to_pandas()) if pandas else writer.write_arrow(data)
            assert isinstance(writer, ObservingWriter)
            assert len(writer.observed) == 1
            assert writer.observed[0].to_pydict() == data.to_pydict()
            core.write_arrow.assert_called_once()
        finally:
            writer.close()


@pytest.mark.parametrize('writer_class', WRITERS)
@pytest.mark.parametrize('pandas', [False, True])
def test_direct_writer_diagnostic_switch_preserves_outer_batch_override(tmp_path, writer_class, pandas):
    table, schema = _table(tmp_path, writer_class)

    class ObservingWriter(writer_class):
        def write_arrow_batch(self, data, bucket=None):
            self.observed.append(data)
            return super().write_arrow_batch(data, bucket)

    with _native_backend(forbid_python=False) as (core, create):
        writer = _construct(ObservingWriter, table)
        writer.observed = []
        try:
            # Explicit Python diagnostics select that backend before mutation.
            # The outer public subclass must continue to observe batch writes.
            assert writer.file_store_write is writer.file_store_write
            core.close.assert_called_once_with()
            data = pa.table({'id': [1], 'value': [10]}, schema=schema)
            writer.write_pandas(data.to_pandas()) if pandas else writer.write_arrow(data)
            assert len(writer.observed) == 1
            assert writer.observed[0].to_pydict() == data.to_pydict()
            assert _prepare(writer)
            create.assert_called_once_with(table)
            core.write_arrow.assert_not_called()
            core.prepare_commit.assert_not_called()
        finally:
            writer.close()
    core.close.assert_called_once_with()


@pytest.mark.parametrize('overwrite', [False, True])
def test_direct_postpone_diagnostic_switch_reuses_python_distributed_planner(tmp_path, overwrite):
    table, _ = _table(tmp_path, PostponeFixedBucketBatchTableWrite)
    with _native_backend(forbid_python=False):
        writer = _construct(PostponeFixedBucketBatchTableWrite, table)
        try:
            assert writer.file_store_write is writer.file_store_write
            pure = writer._execution._python_writer
            planner = pure._planner
            with patch('pypaimon.write.backend_dispatch.create_postpone_bucket_planner',
                       side_effect=AssertionError('distributed write selected a second planner')):
                options = writer._distributed_write_options(overwrite=overwrite)
            assert options['postpone_bucket_planner'] is planner
        finally:
            writer.close()


@pytest.mark.parametrize('writer_class', [BatchTableWrite, PostponeFixedBucketBatchTableWrite])
def test_direct_native_batch_preparation_failure_consumes_one_attempt(tmp_path, writer_class):
    table, _ = _table(tmp_path, writer_class)
    failure = OSError('preparation may already have succeeded')
    with _native_backend() as (core, _):
        core.prepare_commit.side_effect = failure
        writer = _construct(writer_class, table)
        try:
            with pytest.raises(TypeError):
                writer.prepare_commit(1)
            core.prepare_commit.assert_not_called()
            assert writer.batch_committed is False
            with pytest.raises(OSError) as caught:
                writer.prepare_commit()
            assert caught.value is failure
            assert writer.batch_committed is True
            with pytest.raises(RuntimeError, match='one-time committing'):
                writer.prepare_commit()
            core.prepare_commit.assert_called_once_with()
        finally:
            writer.close()


def test_direct_native_stream_preparation_validates_before_io_and_supports_multiple_rounds(tmp_path):
    table, _ = _table(tmp_path, StreamTableWrite)
    with _native_backend() as (core, _):
        writer = StreamTableWrite(table, 'direct-writer')
        try:
            with pytest.raises(TypeError):
                writer.prepare_commit()
            for identifier in (None, 7.5, '7'):
                with pytest.raises(TypeError):
                    writer.prepare_commit(identifier)
            core.prepare_commit.assert_not_called()
            assert writer.prepare_commit(7) == ['message']
            assert writer.prepare_commit(8) == ['message']
            assert core.prepare_commit.call_args_list == [call(True, 7), call(True, 8)]
        finally:
            writer.close()


@pytest.mark.parametrize('writer_class', WRITERS)
def test_direct_native_writer_never_imports_python_execution(tmp_path, writer_class):
    source = r'''
import importlib.abc
import sys
from unittest.mock import Mock, patch

class BlockPythonExecution(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname == 'pypaimon.execution.python' or fullname.startswith('pypaimon.execution.python.'):
            raise AssertionError('Native imported Python execution: ' + fullname)

sys.meta_path.insert(0, BlockPythonExecution())
import pyarrow as pa
from pypaimon import CatalogFactory, Schema
from pypaimon.snapshot.snapshot import BATCH_COMMIT_IDENTIFIER
from pypaimon.write.bucket_plan import PostponeBucketPlan
from pypaimon.write.postpone_batch_table_write import PostponeFixedBucketBatchTableWrite
from pypaimon.write.table_write import BatchTableWrite, StreamTableWrite, TableWrite

writer_class = globals()[sys.argv[1]]
postpone = writer_class is PostponeFixedBucketBatchTableWrite
catalog = CatalogFactory.create({'warehouse': sys.argv[2]})
catalog.create_database('default', True)
schema = pa.schema([('id', pa.int32()), ('value', pa.int32())])
catalog.create_table('default.direct', Schema.from_pyarrow_schema(
    schema, primary_keys=['id'] if postpone else [], options={
        'bucket': '-2' if postpone else '-1', 'postpone.default-bucket-num': '2',
        'write.native.enabled': 'true', 'commit.native.enabled': 'false',
        'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
    }), False)
table = catalog.get_table('default.direct')
native_table, builder, core = Mock(), Mock(), Mock()
for name in ('new_batch_write_builder', 'new_stream_write_builder', 'new_postpone_fixed_bucket_write_builder'):
    getattr(native_table, name).return_value = builder
for name in ('with_commit_user', 'with_bucket_plan'):
    getattr(builder, name).return_value = builder
builder.new_write.return_value = core
core.prepare_commit.return_value = []
with patch('pypaimon.execution.native.write.native_write.native_runtime_available', return_value=True), \
        patch('pypaimon.execution.native.write.native_write.create_native_write_table', return_value=native_table), \
        patch('pypaimon.execution.native.write.native_write.from_native_commit_messages', return_value=[]):
    kwargs = {'bucket_plan': PostponeBucketPlan({(): 2})} if postpone else {}
    writer = writer_class(table, 'native-direct', **kwargs)
    try:
        assert type(writer) is writer_class
        writer.write_arrow(pa.table({'id': [1], 'value': [10]}, schema=schema))
        if writer_class is StreamTableWrite:
            assert writer.prepare_commit(7) == []
        elif writer_class is TableWrite:
            assert writer._prepare_commit(BATCH_COMMIT_IDENTIFIER) == []
        else:
            assert writer.prepare_commit() == []
    finally:
        writer.close()
core.write_arrow.assert_called_once()
assert core.write_arrow.call_args[0][0].to_pydict() == {'id': [1], 'value': [10]}
core.close.assert_called_once_with()
assert not any(name.startswith('pypaimon.execution.python') for name in sys.modules)
'''
    result = subprocess.run(
        [sys.executable, '-c', source, writer_class.__name__, str(tmp_path / 'warehouse')],
        cwd=str(Path(__file__).resolve().parents[3]),
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=30)
    assert result.returncode == 0, result.stdout + result.stderr
