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

"""End-to-end coverage of the optional native batch row-ID update bridge."""

from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.write.table_update import BatchTableUpdate


@pytest.mark.native_plan
def test_batch_row_id_update_uses_rust_and_python_commit(tmp_path):
    from pypaimon_rust.datafusion import BatchWriteBuilder

    if not hasattr(BatchWriteBuilder, 'new_update'):
        pytest.skip('installed Rust binding does not expose batch update yet')
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('default', True)
    schema = pa.schema([
        ('id', pa.int32()), ('name', pa.string()), ('age', pa.int32()),
    ])
    catalog.create_table('default.t', Schema.from_pyarrow_schema(schema, options={
        'row-tracking.enabled': 'true',
        'data-evolution.enabled': 'true',
        'write.native.enabled': 'true',
    }), False)
    table = catalog.get_table('default.t')
    builder = table.new_batch_write_builder()
    writer = builder.new_write()
    writer.write_arrow(pa.Table.from_pydict({
        'id': [1, 2, 3], 'name': ['a', 'b', 'c'], 'age': [10, 20, 30],
    }, schema=schema))
    builder.new_commit().commit(writer.prepare_commit())
    writer.close()

    update_builder = table.new_batch_write_builder()
    update = update_builder.new_update().with_update_type(['name', 'age'])
    changed = pa.Table.from_batches([
        pa.record_batch([
            pa.array([2], type=pa.int64()), pa.array(['C']),
            pa.array([31], type=pa.int32()),
        ], names=['_ROW_ID', 'name', 'age']),
        pa.record_batch([
            pa.array([0], type=pa.int64()), pa.array(['A']),
            pa.array([11], type=pa.int32()),
        ], names=['_ROW_ID', 'name', 'age']),
    ])
    with patch.object(BatchTableUpdate, '_update_by_arrow_with_row_id',
                      side_effect=AssertionError('Python update was selected')):
        messages = update.update_by_arrow_with_row_id(changed)
    assert messages
    assert all(file.file_path and table.file_io.exists(file.file_path)
               for message in messages for file in message.new_files)
    update_builder.new_commit().commit(messages)

    read_builder = table.new_read_builder()
    actual = read_builder.new_read().to_arrow(
        read_builder.new_scan().plan().splits()).sort_by('id')
    assert actual.select(['id', 'name', 'age']).to_pydict() == {
        'id': [1, 2, 3], 'name': ['A', 'b', 'C'], 'age': [11, 20, 31],
    }
