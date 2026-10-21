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

import pyarrow as pa
import pytest

from pypaimon.write.table_update import BatchTableUpdate


WRAPPERS = [
    pytest.param(lambda t: t, lambda v: v, id="struct"),
    pytest.param(lambda t: pa.struct([('outer', t)]),
                 lambda v: {'outer': v}, id="nested-struct"),
    pytest.param(pa.list_, lambda v: [v], id="list"),
    pytest.param(pa.large_list, lambda v: [v], id="large-list"),
    pytest.param(lambda t: pa.list_(t, 1), lambda v: [v], id="fixed-list"),
    pytest.param(lambda t: pa.map_(pa.string(), t),
                 lambda v: [('key', v)], id="map-value"),
    pytest.param(lambda t: pa.list_(pa.map_(pa.string(), t)),
                 lambda v: [[('key', v)]], id="list-map-value"),
]


@pytest.mark.parametrize("wrap_type,wrap_value", WRAPPERS)
@pytest.mark.parametrize("chunked", [False, True])
@pytest.mark.parametrize("reordered", [False, True])
def test_predicate_assignment_struct_cast(wrap_type, wrap_value, chunked, reordered):
    source = wrap_type(pa.struct([('a', pa.int32()), ('b', pa.int32())]))
    fields = [('a', pa.int64()), ('b', pa.int64())]
    target = wrap_type(pa.struct(fields[::-1] if reordered else fields))
    value = wrap_value({'a': 1, 'b': 2})
    values = pa.array([value, None], type=source)
    if chunked:
        values = pa.chunked_array([values.slice(0, 1), values.slice(1)])

    if reordered:
        with pytest.raises(ValueError, match="struct fields must match in order"):
            BatchTableUpdate._assignment_to_array(values, target, 2)
    else:
        try:
            values.cast(target)
        except pa.ArrowNotImplementedError:
            # Older Arrow versions do not implement STRUCT casts.
            with pytest.raises(pa.ArrowNotImplementedError):
                BatchTableUpdate._assignment_to_array(values, target, 2)
            return
        result = BatchTableUpdate._assignment_to_array(values, target, 2)
        assert result.type == target
        assert result.to_pylist() == [value, None]
