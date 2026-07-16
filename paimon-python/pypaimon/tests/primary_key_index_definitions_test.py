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

import unittest

from pypaimon.index.pk.primary_key_index_definitions import PrimaryKeyIndexDefinitions
from pypaimon.index.pk.primary_key_index_source_file import PrimaryKeyIndexSourceFile
from pypaimon.index.pk.primary_key_index_source_meta import PrimaryKeyIndexSourceMeta
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.schema.table_schema import TableSchema


class PrimaryKeyIndexDefinitionsTest(unittest.TestCase):
    def test_definitions_follow_schema_order(self):
        schema = TableSchema(
            fields=[DataField(0, "id", AtomicType("INT")),
                    DataField(1, "name", AtomicType("STRING"))],
            options={"pk-btree.index.columns": "name",
                     "pk-bitmap.index.columns": "id",
                     "fields.name.pk-btree.index.options":
                         '{"block-size": "4096"}'},
        )

        definitions = PrimaryKeyIndexDefinitions.create(schema).definitions

        self.assertEqual(["id", "name"], [d.column for d in definitions])
        self.assertEqual(["bitmap", "btree"], [d.index_type for d in definitions])
        self.assertEqual(
            "4096", definitions[1].options.to_map()["btree-index.block-size"])

    def test_source_meta_uses_java_binary_layout(self):
        meta = PrimaryKeyIndexSourceMeta(
            2, [PrimaryKeyIndexSourceFile("数据-\0-😀.parquet", 7)])

        restored = PrimaryKeyIndexSourceMeta.deserialize(meta.serialize())

        self.assertEqual(meta, restored)


if __name__ == "__main__":
    unittest.main()
