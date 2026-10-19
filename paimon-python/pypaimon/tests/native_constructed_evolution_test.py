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

"""Existing constructed schema changes must read physical Arrow lists natively."""

import os
from contextlib import ExitStack
from unittest.mock import patch
from urllib.parse import unquote, urlparse

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.tests import native_schema_evolution_cast_test

rest_catalog = native_schema_evolution_cast_test.rest_catalog
pytestmark = native_schema_evolution_cast_test.pytestmark

_LIST = pa.list_(pa.int32())
_LARGE_LIST = pa.large_list(pa.int32())
_CASES = [
    (_LIST, _LARGE_LIST, [[9], [1, None], [], None, [2, 3]], 'v',
     ['[9]', '[1, null]', '[]', None, '[2, 3]']),
    (pa.struct([('a', _LIST)]), pa.struct([('a', _LARGE_LIST)]),
     [{'a': None}, None, {'a': [1, None]}, {'a': []}], 'v',
     ['{null}', None, '{[1, null]}', '{[]}']),
    (pa.map_(pa.string(), _LIST), pa.map_(pa.string(), _LARGE_LIST),
     [[('one', [1, None]), ('empty', []), ('missing', None)], None, []], 'v',
     ['{one -> [1, null], empty -> [], missing -> null}', None, '{}']),
    (pa.struct([('a', _LIST)]), pa.struct([('a', _LARGE_LIST)]),
     [{'a': None}, None, {'a': [1, None]}, {'a': []}], ['v', 'a'],
     [{'a': None}, None, {'a': '[1, null]'}, {'a': '[]'}]),
]


@pytest.mark.parametrize('mode', ['append', 'pk', 'de'])
@pytest.mark.parametrize('target', ['STRING', 'CHAR(2)'])
@pytest.mark.parametrize('source,physical,values,path,expected', _CASES,
                         ids=['array', 'row', 'map', 'row-child'])
def test_large_list_constructed_strings_use_native_read(
        rest_catalog, mode, target, source, physical, values, path, expected):
    catalog, _ = rest_catalog
    native_schema_evolution_cast_test._table(catalog, mode, source, values)
    # The original nested schema must also plan through Rust, before changing
    # only the logical type and preserving the old physical data files.
    original = catalog.get_table('default.t').copy({'scan.native-plan.enabled': 'true'})
    original_plan = original.new_read_builder().new_scan().plan()
    assert sum(file.row_count for split in original_plan.splits() for file in split.files) == len(values)
    catalog.alter_table('default.t', [SchemaChange.update_column_type(path, AtomicType(target))], False)
    table = catalog.get_table('default.t')
    planning = table.copy({'scan.native-plan.enabled': 'false', 'read.native.enabled': 'false'})
    splits = planning.new_read_builder().new_scan().plan().splits()
    assert splits
    for split in splits:
        for meta in split.files:
            location = meta.file_path
            if location.startswith('file:'):
                location = unquote(urlparse(location).path)
            data = pq.read_table(location)
            index = data.schema.get_field_index('v')
            field = data.schema.field(index)
            data = data.set_column(index, pa.field(
                field.name, physical, nullable=field.nullable, metadata=field.metadata),
                data.column(index).cast(physical))
            pq.write_table(data, location, row_group_size=2)
            meta.file_size = os.stat(location).st_size
            if not pa.types.is_map(physical):
                assert pq.read_schema(location).field('v').type == physical
            # PyArrow restores LargeList at the top level and inside ROW,
            # while its Parquet reader normalizes MAP values to regular List.
    for native in (False, True):
        reader = table.copy({'read.native.enabled': str(native).lower()}).new_read_builder().new_read()
        guard = patch('pypaimon.read.table_read.TableRead._create_split_read',
                      side_effect=AssertionError('Native read fell back')) if native else ExitStack()
        with guard:
            result = reader.to_arrow(splits).sort_by([('id', 'ascending')])
        # Java's constructed casts use STRING for children and ignore the
        # outer CHAR length. NULL containers and NULL elements stay distinct.
        assert result.column('v').to_pylist() == expected
