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

from unittest.mock import Mock, patch

import pyarrow as pa

from pypaimon.read.table_read import TableRead
from pypaimon.schema.data_types import AtomicType, DataField


class _Split:
    def __init__(self, file_name='data.parquet'):
        self.files = [Mock(file_name=file_name)]


def _table_read(limit=None):
    read = TableRead.__new__(TableRead)
    read.table = Mock()
    read.table.options.native_read_enabled.return_value = True
    read.table.options.file_format.return_value = 'parquet'
    read.predicate = None
    read.read_type = [DataField(0, 'id', AtomicType('INT'))]
    read.include_row_kind = False
    read.nested_name_paths = None
    read.limit = limit
    read._read_parallelism = None
    read._predicate_extra_fields = []
    read._output_column_names = ['id']
    return read


def test_native_read_consumes_retained_rust_splits_and_enforces_limit():
    read = _table_read(limit=2)
    first, second = _Split(), _Split()
    first._native_split = object()
    second._native_split = object()
    batches = [pa.record_batch({'id': [1, 2, 3]})]

    with patch('pypaimon.read.native_plan.native_read',
               return_value=batches) as native:
        result = read.to_arrow([first, second])

    assert result.to_pydict() == {'id': [1, 2]}
    native.assert_called_once_with(
        read.table,
        [first._native_split, second._native_split],
        predicate=None,
        limit=2,
        projection=['id'],
    )


def test_native_read_falls_back_for_transformed_python_split():
    read = _table_read()
    schema = pa.schema([('id', pa.int32())])
    split = _Split()

    with patch('pypaimon.read.native_plan.native_read') as native:
        assert read._try_native_batches([split], schema) is None

    native.assert_not_called()


def test_native_read_falls_back_for_explicit_python_parallelism():
    read = _table_read()
    schema = pa.schema([('id', pa.int32())])
    split = _Split()
    split._native_split = object()

    with patch('pypaimon.read.native_plan.native_read') as native:
        assert read._try_native_batches(
            [split], schema, parallelism=2) is None

    native.assert_not_called()


def test_native_read_failure_falls_back():
    read = _table_read()
    schema = pa.schema([('id', pa.int32())])
    split = _Split()
    split._native_split = object()

    with patch('pypaimon.read.native_plan.native_read',
               side_effect=RuntimeError('unsupported')):
        assert read._try_native_batches([split], schema) is None


def test_native_read_falls_back_for_unsupported_file_format():
    read = _table_read()
    read.table.options.file_format.return_value = 'vortex'
    schema = pa.schema([('id', pa.int32())])
    split = _Split()
    split._native_split = object()

    with patch('pypaimon.read.native_plan.native_read') as native:
        assert read._try_native_batches([split], schema) is None

    native.assert_not_called()


def test_native_read_falls_back_for_unsupported_dedicated_file():
    read = _table_read()
    schema = pa.schema([('id', pa.int32())])
    split = _Split('camera.video')
    split._native_split = object()

    with patch('pypaimon.read.native_plan.native_read') as native:
        assert read._try_native_batches([split], schema) is None

    native.assert_not_called()
