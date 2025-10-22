"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from typing import Any, List
from pypaimon.schema.data_types import DataField
from pypaimon.table.row.internal_row import InternalRow
from pypaimon.table.row.row_kind import RowKind


class BinaryRow(InternalRow):
    """
    BinaryRow is a compact binary format for storing a row of data.
    """

    def __init__(self, data: bytes, fields: List[DataField]):
        """
        Initialize BinaryRow with raw binary data and field definitions.
        """
        self.data = data
        self.arity = int.from_bytes(data[:4], 'big')
        # Skip the arity prefix (4 bytes) if present
        self.actual_data = data[4:] if len(data) >= 4 else data
        self.fields = fields
        self.row_kind = RowKind(self.actual_data[0])

    def get_field(self, index: int) -> Any:
        from pypaimon.table.row.generic_row import GenericRowDeserializer
        """Get field value by index."""
        if index >= self.arity or index < 0:
            raise IndexError(f"Field index {index} out of range [0, {self.arity})")

        if GenericRowDeserializer.is_null_at(self.actual_data, 0, index):
            return None

        return GenericRowDeserializer.parse_field_value(self.actual_data, 0,
                                                        GenericRowDeserializer.calculate_bit_set_width_in_bytes(
                                                            self.arity),
                                                        index, self.fields[index].type)

    def get_row_kind(self) -> RowKind:
        return self.row_kind

    def __len__(self):
        return self.arity
