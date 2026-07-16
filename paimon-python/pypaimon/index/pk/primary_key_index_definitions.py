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

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.index.pk.primary_key_index_definition import (
    PrimaryKeyIndexDefinition,
    PrimaryKeyIndexFamily,
)


class PrimaryKeyIndexDefinitions:
    def __init__(self, definitions):
        self.definitions = tuple(definitions)

    @staticmethod
    def create(schema):
        core = CoreOptions(Options(dict(schema.options)))
        btree = core.primary_key_btree_index_columns()
        bitmap = core.primary_key_bitmap_index_columns()
        _validate_columns(btree, CoreOptions.PK_BTREE_INDEX_COLUMNS.key())
        _validate_columns(bitmap, CoreOptions.PK_BITMAP_INDEX_COLUMNS.key())
        overlap = set(btree).intersection(bitmap)
        if overlap:
            raise ValueError("Column '%s' can own at most one primary-key index." % sorted(overlap)[0])

        definitions = []
        for field in schema.fields:
            if field.name in btree:
                definitions.append(PrimaryKeyIndexDefinition(
                    field.name, field.id, "btree",
                    core.primary_key_btree_index_options(field.name),
                    PrimaryKeyIndexFamily.BTREE))
            elif field.name in bitmap:
                definitions.append(PrimaryKeyIndexDefinition(
                    field.name, field.id, "bitmap",
                    core.primary_key_bitmap_index_options(field.name),
                    PrimaryKeyIndexFamily.BITMAP))
        return PrimaryKeyIndexDefinitions(definitions)


def _validate_columns(columns, key):
    seen = set()
    for column in columns:
        if column in seen:
            raise ValueError("%s contains duplicate column '%s'." % (key, column))
        seen.add(column)
