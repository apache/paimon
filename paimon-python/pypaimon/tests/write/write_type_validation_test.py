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

"""Write-column requests are validated before changing either executor."""

from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.native.write.native_write import NativeTableWrite as NativeWrite
from pypaimon.write.write_dispatch import NativeTableWrite

pytestmark = [pytest.mark.python_write, pytest.mark.python_commit]


@pytest.fixture
def table(tmp_path):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    catalog.create_table('default.columns', Schema.from_pyarrow_schema(
        pa.schema([('id', pa.int32()), ('value', pa.int32())]), options={
            'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
            'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false',
            'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        }), False)
    return catalog.get_table('default.columns')


@pytest.mark.parametrize('backend', ['python', 'native-adapter', 'native-coordinator'])
@pytest.mark.parametrize('columns, iterator, error', [
    (['id', 'id'], False, 'Duplicate write column'),
    (['id', 'id'], True, 'Duplicate write column'),
    ([], False, 'write columns must not be empty'),
    ([], True, 'write columns must not be empty'),
])
def test_invalid_write_type_preserves_prior_configuration_without_staging_files(
        table, tmp_path, backend, columns, iterator, error):
    core = Mock()
    native = None
    if backend == 'python':
        writer = table.new_batch_write_builder().new_write()
    else:
        native = NativeWrite(table, 'user', None, False, core)
        writer = NativeTableWrite.from_native(native) if backend == 'native-coordinator' else native
    try:
        writer.with_write_type(['value'])
        before = sorted(tmp_path.rglob('*.parquet'))
        with pytest.raises(ValueError, match=error):
            writer.with_write_type((name for name in columns) if iterator else columns)
        assert sorted(tmp_path.rglob('*.parquet')) == before == []
        batch = pa.record_batch([pa.array([10, 20], pa.int32())], names=['value'])
        writer.write_arrow(batch)
        if backend == 'python':
            assert writer.file_store_write.write_cols == ['value']
            messages = writer.prepare_commit()
            assert [(file.row_count, file.write_cols) for message in messages for file in message.new_files] == [
                (2, ['value'])]
            commit = table.new_batch_write_builder().new_commit()
            try:
                commit.commit(messages)
            finally:
                commit.close()
            read = table.new_read_builder()
            assert read.new_read().to_arrow(read.new_scan().plan().splits()).to_pydict() == {
                'id': [None, None], 'value': [10, 20]}
        else:
            assert native._write_cols == ['value']
            assert native._schema.names == ['value']
            core.with_write_type.assert_called_once_with(['value'])
            core.write_arrow.assert_called_once_with(batch, None)
            if backend == 'native-coordinator':
                assert writer._python_writer is None
    finally:
        writer.close()


def test_duplicate_primary_key_request_does_not_switch_the_native_writer_to_python(table):
    from pypaimon.execution.native.write.native_write import NativeTableWrite as NativeWrite

    table.is_primary_key_table = True
    core = Mock()
    native = NativeWrite(table, 'user', None, False, core)
    writer = NativeTableWrite.from_native(native)
    try:
        with patch.object(writer, '_new_python_writer', side_effect=AssertionError('selected Python')) as pure:
            with pytest.raises(ValueError, match='Duplicate write column'):
                writer.with_write_type(['id', 'id'])
            pure.assert_not_called()
        core.close.assert_not_called()
        core.with_write_type.assert_not_called()
        assert writer._python_writer is None
    finally:
        writer.close()
