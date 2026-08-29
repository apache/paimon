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

import types
import unittest

from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
from pypaimon.globalindex.global_index_schema_compatibility import (
    filter_compatible_global_indexes,
)
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.schema.data_types import ArrayType, AtomicType, DataField
from pypaimon.schema.table_schema import TableSchema


def _field(field_id, name, data_type):
    return DataField(field_id, name, data_type)


def _schema(schema_id, fields):
    return TableSchema(id=schema_id, fields=fields)


def _entry(file_name, schema_id, field_id, extra_field_ids=None):
    index_file = IndexFileMeta(
        index_type='bitmap',
        file_name=file_name,
        file_size=1,
        row_count=1,
        global_index_meta=GlobalIndexMeta(
            row_range_start=0,
            row_range_end=0,
            index_field_id=field_id,
            extra_field_ids=extra_field_ids,
        ),
    )
    return IndexManifestEntry(0, None, 0, index_file, schema_id)


class GlobalIndexSchemaCompatibilityTest(unittest.TestCase):

    def test_filters_by_indexed_field_types(self):
        historical = _schema(1, [
            _field(1, 'numbers', ArrayType(True, AtomicType('INT'))),
            _field(2, 'name', AtomicType('STRING', nullable=False)),
            _field(3, 'age', AtomicType('INT')),
        ])
        current = _schema(2, [
            _field(1, 'numbers', ArrayType(True, AtomicType('BIGINT'))),
            _field(2, 'renamed', AtomicType('STRING')),
            _field(3, 'age', AtomicType('BIGINT')),
        ])
        schema_lookups = []

        def get_schema(schema_id):
            schema_lookups.append(schema_id)
            return historical if schema_id == historical.id else None

        table = types.SimpleNamespace(
            table_schema=current,
            schema_manager=types.SimpleNamespace(get_schema=get_schema),
        )
        compatible = _entry('compatible', 1, 2)
        current_schema = _entry('current', 2, 2)
        changed_primary = _entry('changed-primary', 1, 1)
        changed_extra = _entry('changed-extra', 1, 2, [3])
        legacy = _entry('legacy', None, 2)
        missing_schema_a = _entry('missing-a', 99, 2)
        missing_schema_b = _entry('missing-b', 99, 2)

        result = filter_compatible_global_indexes(table, [
            compatible,
            current_schema,
            changed_primary,
            changed_extra,
            legacy,
            missing_schema_a,
            missing_schema_b,
        ])

        self.assertEqual(
            ['compatible', 'current'],
            [entry.index_file.file_name for entry in result],
        )
        self.assertEqual([1, 99], schema_lookups)


if __name__ == '__main__':
    unittest.main()
