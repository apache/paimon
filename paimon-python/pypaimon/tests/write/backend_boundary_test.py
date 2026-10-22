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

"""Execution ownership and public writer hook contracts."""

import builtins
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.native.write.native_write import NativeTableWrite as NativeWrite
from pypaimon.write.table_write import BatchTableWrite


def _table(tmp_path, options=None):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.data', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int32()), ('value', pa.string())]),
        options=options or {}), False)
    return catalog.get_table('default.data')


@pytest.mark.parametrize('input_method', ['write_arrow', 'write_pandas'])
@pytest.mark.parametrize('override', ['subclass', 'instance'])
def test_public_writer_keeps_dynamic_batch_hooks(tmp_path, input_method, override):
    table = _table(tmp_path)
    builder = table.new_batch_write_builder()
    batches = []

    class ObservedWriter(BatchTableWrite):
        def write_arrow_batch(self, data, bucket=None):
            batches.append(data)
            return super().write_arrow_batch(data, bucket)

    if override == 'subclass':
        writer = ObservedWriter(table, builder.commit_user)
    else:
        writer = builder.new_write()
        original = writer.write_arrow_batch

        def observed(data, bucket=None):
            batches.append(data)
            return original(data, bucket)

        writer.write_arrow_batch = observed
    data = pa.table({'id': pa.array([1, 2], pa.int32()), 'value': ['one', 'two']})
    getattr(writer, input_method)(data if input_method == 'write_arrow' else data.to_pandas())
    assert len(batches) == 1
    assert batches[0].to_pydict() == data.to_pydict()
    commit = builder.new_commit()
    try:
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()
    read = table.new_read_builder()
    assert read.new_read().to_arrow(read.new_scan().plan().splits()).to_pylist() == data.to_pylist()


def test_native_adapter_owns_only_one_backend(tmp_path):
    core = Mock()
    writer = NativeWrite(_table(tmp_path), 'job', None, False, core)
    for name in ('_python_writer', '_new_python_writer', '_switch_to_python',
                 'file_store_write', 'row_key_extractor'):
        assert not hasattr(writer, name)
    writer.write_arrow_batch(pa.record_batch(
        [pa.array([1], pa.int32()), ['one']], names=['id', 'value']))
    assert writer._written
    core.write_arrow.assert_called_once()
    writer.close()
    core.close.assert_called_once_with()


def test_invalid_blob_configuration_keeps_the_configured_consumer(tmp_path):
    table = _table(tmp_path)
    writer = table.new_batch_write_builder().new_write()
    consumer = Mock()
    factory = Mock()
    try:
        assert writer.with_blob_consumer(consumer) is writer
        assert writer.with_blob_uri_reader_factory(factory) is writer
        with pytest.raises(TypeError, match='blob_consumer must be callable'):
            writer.with_blob_consumer(42)
        with pytest.raises(TypeError, match='factory must provide callable create'):
            writer.with_blob_uri_reader_factory(42)
        assert writer.file_store_write.blob_consumer is consumer
        assert writer.file_store_write.blob_uri_reader_factory is factory
        assert writer.with_blob_consumer(None) is writer
        assert writer.with_blob_uri_reader_factory(None) is writer
        assert writer.file_store_write.blob_consumer is None
        assert writer.file_store_write.blob_uri_reader_factory is None
    finally:
        writer.close()


@pytest.mark.parametrize('row_bridge', [False, True])
def test_native_uri_reader_factory_validation_precedes_core_configuration(tmp_path, row_bridge):
    core = Mock()
    writer = NativeWrite(_table(tmp_path), 'job', None, False, core)
    factory = Mock()
    if row_bridge:
        writer._blob_rows = Mock()
    try:
        assert writer.with_blob_uri_reader_factory(factory) is writer
        core.with_blob_uri_reader_factory.reset_mock()
        with pytest.raises(TypeError, match='factory must provide callable create'):
            writer.with_blob_uri_reader_factory(42)
        assert writer._blob_uri_reader_factory is factory
        if row_bridge:
            assert writer._blob_rows.fallback is factory
        core.with_blob_uri_reader_factory.assert_not_called()
    finally:
        writer.close()


@pytest.mark.parametrize('method,attribute', [
    ('with_write_type', 'write_cols'),
    ('with_blob_consumer', 'blob_consumer'),
    ('with_blob_uri_reader_factory', 'blob_uri_reader_factory'),
])
def test_native_facade_configuration_keeps_identity_after_python_selection(tmp_path, method, attribute):
    from pypaimon.write.write_dispatch import NativeTableWrite

    table = _table(tmp_path)
    core = Mock()
    writer = NativeTableWrite(table, 'job', None, False, core)
    try:
        store = writer.file_store_write
        value = ['value'] if method == 'with_write_type' else Mock()
        reset = table.field_names if method == 'with_write_type' else None
        configure = getattr(writer, method)
        assert configure(value) is writer
        assert getattr(store, attribute) == value
        assert configure(reset) is writer
        assert getattr(store, attribute) is None
        getattr(core, method).assert_not_called()
    finally:
        writer.close()
    core.close.assert_called_once_with()


def test_python_delete_matching_respects_independent_read_backend(tmp_path):
    table = _table(tmp_path, {'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
                              'deletion-vectors.enabled': 'true'})
    builder = table.new_batch_write_builder()
    writer, commit = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.table({'id': pa.array([1, 2], pa.int32()), 'value': ['one', 'two']}))
        commit.commit(writer.prepare_commit())
    finally:
        writer.close()
        commit.close()

    table = table.copy({'write.native.enabled': 'true', 'read.native.enabled': 'false',
                        'scan.native-plan.enabled': 'false'})
    updater = table.new_batch_write_builder().new_update()
    predicate = table.new_read_builder().new_predicate_builder().equal('id', 2)
    original_import = builtins.__import__

    def no_native(name, *args, **kwargs):
        if name.startswith(('pypaimon.execution.native', 'pypaimon_rust')):
            raise AssertionError('Python delete matching selected Native execution: ' + name)
        return original_import(name, *args, **kwargs)

    with patch('builtins.__import__', side_effect=no_native):
        assert updater._matched_delete_row_ids(predicate) == [1]
