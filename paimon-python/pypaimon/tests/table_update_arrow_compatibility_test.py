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

from pypaimon.execution.python.write.table_update import PythonTableUpdate


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
@pytest.mark.parametrize("projection", [
    "same", "subset-first", "subset-last", "add-field", "reordered",
])
def test_predicate_assignment_struct_cast(wrap_type, wrap_value, chunked, projection):
    source = wrap_type(pa.struct([('a', pa.int32()), ('b', pa.int32())]))
    fields = [('a', pa.int64()), ('b', pa.int64())]
    if projection == "subset-first":
        fields = fields[:1]
    elif projection == "subset-last":
        fields = fields[1:]
    elif projection == "add-field":
        fields.append(('c', pa.int64()))
    elif projection == "reordered":
        fields.reverse()
    target = wrap_type(pa.struct(fields))
    value = wrap_value({'a': 1, 'b': 2})
    values = pa.array([value, None], type=source)
    if chunked:
        values = pa.chunked_array([values.slice(0, 1), values.slice(1)])

    if projection == "reordered":
        with pytest.raises(ValueError, match="struct fields must match in order"):
            PythonTableUpdate._assignment_to_array(values, target, 2)
    else:
        try:
            expected = values.cast(target).to_pylist()
        except (pa.ArrowNotImplementedError, pa.ArrowTypeError) as error:
            # Older Arrow versions reject unsupported STRUCT field projections.
            with pytest.raises(type(error)) as raised:
                PythonTableUpdate._assignment_to_array(values, target, 2)
            assert str(raised.value) == str(error)
            return
        result = PythonTableUpdate._assignment_to_array(values, target, 2)
        assert result.type == target
        assert result.to_pylist() == expected
