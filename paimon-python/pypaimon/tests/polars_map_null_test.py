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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pyarrow as pa
import pytest

from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader


class _BatchReader(RecordBatchReader):

    def __init__(self, batch):
        self.batch = batch

    def read_arrow_batch(self):
        batch, self.batch = self.batch, None
        return batch

    def close(self):
        pass


_MAP = pa.map_(pa.string(), pa.int64())
_VALUES = [None, [], [('a', None), ('a', 2)]]
_EXPECTED = [None, [], [{'key': 'a', 'value': None}, {'key': 'a', 'value': 2}]]


@pytest.mark.parametrize('data_type, values, expected', [
    (_MAP, _VALUES, _EXPECTED),
    (pa.struct([('m', _MAP)]),
     [None, {'m': None}, {'m': []}, {'m': _VALUES[2]}],
     [None, {'m': None}, {'m': []}, {'m': _EXPECTED[2]}]),
    (pa.list_(_MAP), [None, [None, []], [_VALUES[2], None]],
     [None, [None, []], [_EXPECTED[2], None]]),
    (pa.large_list(_MAP), [None, [None, []], [_VALUES[2], None]],
     [None, [None, []], [_EXPECTED[2], None]]),
    (pa.list_(_MAP, 2), [None, [None, []], [_VALUES[2], None]],
     [None, [None, []], [_EXPECTED[2], None]]),
    (pa.map_(pa.string(), _MAP), [None, [], [('outer', None)], [('outer', _VALUES[2])]],
     [None, [], [{'key': 'outer', 'value': None}], [{'key': 'outer', 'value': _EXPECTED[2]}]]),
])
@pytest.mark.parametrize('offset', [0, 1])
def test_record_iteration_preserves_map_nulls(data_type, values, expected, offset):
    array = pa.array([None] * offset + values + [None], type=data_type).slice(offset, len(values))
    schema = pa.schema([pa.field('value', data_type, metadata={b'field': b'kept'})],
                       metadata={b'schema': b'kept'})
    batch = pa.RecordBatch.from_arrays([array], schema=schema)
    assert _BatchReader(batch).read_next_df().to_dicts() == [{'value': value} for value in expected]
    assert list(_BatchReader(batch).tuple_iterator()) == [(value,) for value in expected]
    iterator = _BatchReader(batch).read_batch()
    actual = []
    while True:
        row = iterator.next()
        if row is None:
            break
        actual.append(row.get_field(0))
    assert actual == expected
    # The local Polars view must not mutate Arrow reader types or metadata.
    assert batch.schema.equals(schema, check_metadata=True)
    assert batch.column(0).to_pylist() == values
