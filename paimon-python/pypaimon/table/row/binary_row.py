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
from typing import Any
from pypaimon.schema.data_types import DataType


class BinaryRow:

    def __init__(self, data: bytes):
        """
        Initialize BinaryRow with raw binary data and field definitions.
        """
        self.data = data
        self.arity = int.from_bytes(data[:4], 'big')
        # Skip the arity prefix (4 bytes) if present
        self.actual_data = data[4:] if len(data) >= 4 else data

    def get_field_by_type(self, index: int, field_type: DataType) -> Any:
        from pypaimon.table.row.generic_row import GenericRowDeserializer
        """Get field value by index."""
        if index >= self.arity or index < 0:
            raise IndexError(f"Field index {index} out of range [0, {self.arity})")

        if GenericRowDeserializer.is_null_at(self.actual_data, 0, index):
            return None

        return GenericRowDeserializer.parse_field_value(self.actual_data, 0,
                                                        GenericRowDeserializer.calculate_bit_set_width_in_bytes(
                                                            self.arity),
                                                        index, field_type)
