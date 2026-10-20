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

"""Configuration remains available when RowKind filtering wrote no rows."""

from contextlib import ExitStack
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import Schema
from pypaimon.read.native_plan import native_runtime_available
from pypaimon.schema.data_types import AtomicType, DataField, PyarrowFieldParser
from pypaimon.table.row.blob import BlobDescriptor
from pypaimon.tests import native_plan_rest_test
from pypaimon.tests.native_blob_uri_reader_test import Factory
from pypaimon.tests.native_write_routing_test import _table
from pypaimon.write.native_write import NativeTableWrite


pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]
rest_catalog = native_plan_rest_test.rest_catalog
_ENGINES = [False, pytest.param(True, marks=[pytest.mark.native_plan, pytest.mark.skipif(
    not native_runtime_available(), reason='pypaimon-rust not installed')])]


@pytest.mark.parametrize('setter', ['type', 'consumer', 'consumer-none', 'factory', 'factory-none'])
def test_native_setters_delegate_written_state_without_permitting_fallback(tmp_path, setter):
    table = _table(tmp_path).copy({'rowkind.field': 'value', 'ignore-delete': 'true'})
    core = Mock()
    writer = NativeTableWrite(table, 'job', None, False, core)
    try:
        writer.write_arrow(pa.table({'id': pa.array([1], pa.int64()), 'pt': ['a'], 'value': ['-D']}))
        if setter == 'type':
            assert writer.with_write_type(['id']) is writer
            core.with_write_type.assert_called_once_with(['id'])
        elif setter.startswith('consumer'):
            callback = None if setter.endswith('none') else lambda name, value: True
            assert writer.with_blob_consumer(callback) is writer
            core.with_blob_consumer.assert_called_once()
        else:
            factory = None if setter.endswith('none') else Factory()
            assert writer.with_blob_uri_reader_factory(factory) is writer
            core.with_blob_uri_reader_factory.assert_called_once_with(factory)
        with pytest.raises(RuntimeError, match='Cannot switch'):
            writer._switch_to_python()
        assert writer._python_writer is None
    finally:
        writer.close()


def _blob_table(catalog, native):
    fields = [DataField(0, 'id', AtomicType('INT')), DataField(1, 'op', AtomicType('STRING')),
              DataField(2, 'payload', AtomicType('BLOB'))]
    catalog.create_table('default.t', Schema(fields=fields, options={
        'rowkind.field': 'op', 'ignore-delete': 'true', 'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true', 'write.native.enabled': str(native).lower(),
        'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
        'commit.native.enabled': 'false',
    }), False)
    return catalog.get_table('default.t')


@pytest.mark.parametrize('setter', ['consumer', 'factory'])
def test_core_rejection_does_not_change_live_blob_configuration(rest_catalog, setter):
    catalog, _ = rest_catalog
    table = _blob_table(catalog, False)
    core = Mock()
    writer = NativeTableWrite(table, 'job', None, False, core)
    previous = (lambda name, value: True) if setter == 'consumer' else Factory()
    method = 'with_blob_' + ('consumer' if setter == 'consumer' else 'uri_reader_factory')
    try:
        getattr(writer, method)(previous)
        getattr(core, method).reset_mock()
        getattr(core, method).side_effect = ValueError('must be called before any write operation')
        with pytest.raises(ValueError, match='before any write'):
            getattr(writer, method)(None)
        getattr(core, method).assert_called_once()
        if setter == 'factory':
            getattr(core, method).assert_called_once_with(writer._blob_rows)
            assert writer._blob_rows.fallback is previous
            assert writer._blob_uri_reader_factory is previous
        else:
            assert writer._blob_consumer is previous
    finally:
        writer.close()


@pytest.mark.parametrize('native', _ENGINES)
@pytest.mark.parametrize('kind', [None, '-D', '-U'])
@pytest.mark.parametrize('stream', [False, True])
def test_filtered_blob_batches_allow_configuration_and_real_writes_lock_it(rest_catalog, native, kind, stream):
    catalog, _ = rest_catalog
    table = _blob_table(catalog, native)
    schema = PyarrowFieldParser.from_paimon_schema(table.fields)
    builder = table.new_stream_write_builder() if stream else table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    received = []
    factory = Factory()
    try:
        assert isinstance(writer, NativeTableWrite) == native
        writer.with_blob_consumer(lambda name, value: pytest.fail('replaced consumer called'))
        bad_factory = Factory()
        bad_factory.errors['create'] = AssertionError('replaced factory called')
        writer.with_blob_uri_reader_factory(bad_factory)
        data = [] if kind is None else [dict(id=1, op=kind, payload=b'ignored')]
        writer.write_arrow(pa.Table.from_pylist(data, schema=schema))
        writer.with_write_type(['id', 'payload'])
        writer.with_blob_consumer(None)
        writer.with_blob_uri_reader_factory(None)
        writer.with_blob_consumer(lambda name, value: received.append((name, value)) or True)
        writer.with_blob_uri_reader_factory(factory)
        with ExitStack() as guards:
            if native:
                guards.enter_context(patch.object(writer, '_new_python_writer',
                                                  side_effect=AssertionError('Python writer fallback')))
            writer.write_arrow(pa.Table.from_pylist([
                dict(id=2, op='+I', payload=BlobDescriptor('custom://source', 6, 7).serialize()),
            ], schema=schema))
            for setter in (lambda: writer.with_blob_consumer(None),
                           lambda: writer.with_blob_uri_reader_factory(None)):
                with pytest.raises((ValueError, RuntimeError), match='before.*writ'):
                    setter()
            if native:
                with pytest.raises((ValueError, RuntimeError), match='before.*writ'):
                    writer.with_write_type(['id'])
                assert writer._blob_rows.fallback is factory
            messages = writer.prepare_commit(7) if stream else writer.prepare_commit()
            commit.commit(messages, 7) if stream else commit.commit(messages)
        assert len(received) == 1
        assert received[0][0] == 'payload'
        assert received[0][1].length == 7
        assert factory.opened == factory.closed == ['custom://source']
    finally:
        writer.close()
        commit.close()
    reader = table.new_read_builder()
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits()).to_pylist() == [
        dict(id=2, op=None, payload=b'PAYLOAD')]


@pytest.mark.parametrize('native', _ENGINES)
def test_filtered_primary_key_batch_keeps_complete_write_type_a_noop(rest_catalog, native):
    catalog, _ = rest_catalog
    table = _table(None, 'fixed', catalog, use_native=native).copy({
        'rowkind.field': 'value', 'ignore-delete': 'true'})
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'id': pa.array([1], pa.int64()), 'pt': ['a'], 'value': ['-D']}))
        assert writer.with_write_type(reversed(table.field_names)) is writer
        writer.write_arrow(pa.table({'id': pa.array([2], pa.int64()), 'pt': ['a'], 'value': ['+I']}))
        assert writer.with_write_type(table.field_names) is writer
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    reader = table.new_read_builder()
    assert reader.new_read().to_arrow(reader.new_scan().plan().splits()).to_pylist() == [
        dict(id=2, pt='a', value='+I')]
