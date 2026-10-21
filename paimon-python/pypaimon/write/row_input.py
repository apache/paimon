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

from typing import Any, Dict, List

import pyarrow as pa

from pypaimon.schema.data_types import PyarrowFieldParser, is_blob_file_field
from pypaimon.table.special_fields import SpecialFields
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.row_utils import (inline_blob_value, require_columns, row_to_named_values, value_for_arrow)


class RowIdUpdateInputMixin:
    """Normalize Python row inputs for either update execution backend."""

    def update_row_columns(
            self,
            row,
            row_ids: List[int],
            column_names: List[str],
    ) -> List[CommitMessage]:
        return self.update_rows_columns([row], [row_ids], column_names)

    def update_rows_columns(
            self,
            rows: List,
            row_ids_by_row: List[List[int]],
            column_names: List[str],
    ) -> List[CommitMessage]:
        if not column_names:
            raise ValueError("column_names cannot be empty")
        column_names = list(dict.fromkeys(column_names))
        if len(rows) != len(row_ids_by_row):
            raise ValueError(
                "rows and row_ids_by_row must have the same length: "
                f"{len(rows)} != {len(row_ids_by_row)}"
            )

        values_by_row = [
            row_to_named_values(row, self.table.table_schema.fields)
            for row in rows
        ]
        for values_by_name in values_by_row:
            require_columns(values_by_name, column_names, "update_rows_columns")

        row_entries = []
        for values_by_name, row_ids in zip(values_by_row, row_ids_by_row):
            row_entries.extend((row_id, values_by_name) for row_id in row_ids)

        if not row_entries:
            return []

        row_entries.sort(key=lambda item: item[0])

        for col_name in column_names:
            if col_name not in self.table.field_names:
                raise ValueError(f"Column {col_name} not found in table schema")

        arrays = [
            pa.array([row_id for row_id, _ in row_entries], type=pa.int64())
        ]
        fields = [pa.field(SpecialFields.ROW_ID.name, pa.int64())]
        blob_object_columns: Dict[str, List[Any]] = {}
        descriptor_fields = self.table.options.blob_descriptor_fields()
        view_fields = self.table.options.blob_view_fields()

        for col_name in column_names:
            if self._is_blob_column(col_name):
                blob_object_columns[col_name] = [
                    values_by_name[col_name]
                    for _, values_by_name in row_entries
                ]
                continue

            table_field = self.table.field_dict[col_name]
            arrow_field = PyarrowFieldParser.from_paimon_field(table_field)
            arrays.append(
                pa.array(
                    [
                        value_for_arrow(inline_blob_value(
                            values_by_name[col_name],
                            col_name in descriptor_fields, col_name in view_fields), table_field)
                        for _, values_by_name in row_entries
                    ],
                    type=arrow_field.type,
                )
            )
            fields.append(arrow_field)

        update_data = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
        return self._write_row_columns(update_data, column_names, blob_object_columns)

    def _is_blob_column(self, column_name: str) -> bool:
        if column_name in (self.table.options.blob_descriptor_fields() | self.table.options.blob_view_fields()):
            return False
        for table_field in self.table.fields:
            if table_field.name == column_name:
                return is_blob_file_field(table_field)
        return False
