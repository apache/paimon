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

"""End-to-end tests for the ``$files`` system table."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

from pypaimon.schema.data_types import AtomicType, DataField, RowType
from pypaimon.table.system.native_system_table import try_plan_native_system_table, read_native_system_splits


def _table(name='files', enabled=True):
    return SimpleNamespace(
        base_table=SimpleNamespace(options=SimpleNamespace(native_read_enabled=lambda: enabled)),
        system_table_name=lambda: name,
        row_type=lambda: RowType(False, [DataField(0, 'id', AtomicType('INT', nullable=False))]))


@pytest.mark.parametrize('reason', ['disabled', 'old-binding', 'custom-io', 'catalog-required'])
def test_unsupported_metadata_reads_do_not_create_native_tables(reason):
    table = _table('partitions' if reason == 'catalog-required' else 'files', enabled=reason != 'disabled')
    with patch('pypaimon.table.system.native_system_table.native_method_available',
               return_value=reason != 'old-binding'):
        with patch('pypaimon.table.system.native_system_table._rest_catalog_supported',
                   return_value=reason != 'custom-io'):
            with patch('pypaimon.table.system.native_system_table._native_table') as create:
                assert try_plan_native_system_table(table) is None
                create.assert_not_called()


def _read_with_fake_reader(reader):
    table = _table()
    import sys
    binding = MagicMock()
    binding.SystemTableRead.return_value.read.return_value = reader
    with patch.dict(sys.modules, {'pypaimon_rust.datafusion': binding}):
        return read_native_system_splits(table, table.row_type().fields, None, None, [])


def test_native_reader_is_closed_after_stream_failure():
    reader = MagicMock()
    reader.__iter__.side_effect = RuntimeError('metadata stream failed')
    with pytest.raises(RuntimeError, match='metadata stream failed'):
        _read_with_fake_reader(reader)
    reader.close.assert_called_once_with()


def test_native_empty_reader_uses_declared_schema():
    reader = MagicMock()
    reader.schema = pa.schema([('id', pa.int32())])
    reader.__iter__.return_value = iter([])
    result = _read_with_fake_reader(reader)
    assert result.num_rows == 0
    assert result.schema == pa.schema([pa.field('id', pa.int32(), nullable=False)])
    reader.close.assert_called_once_with()
