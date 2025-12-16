#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

from typing import List

from ..schema.data_types import AtomicType, DataField


class SpecialFields:
    """
    Special fields in a RowType with specific field ids.
    """

    SEQUENCE_NUMBER = DataField(2147483646, "_SEQUENCE_NUMBER", AtomicType("BIGINT", nullable=False))
    VALUE_KIND = DataField(2147483645, "_VALUE_KIND", AtomicType("TINYINT", nullable=False))
    ROW_ID = DataField(2147483642, "_ROW_ID", AtomicType("BIGINT", nullable=False))

    SYSTEM_FIELD_NAMES = {
        '_SEQUENCE_NUMBER',
        '_VALUE_KIND',
        '_ROW_ID'
    }

    @staticmethod
    def is_system_field(field_name: str) -> bool:
        """Check if a field is a system field."""
        return field_name in SpecialFields.SYSTEM_FIELD_NAMES

    @staticmethod
    def find_system_fields(read_fields: List[DataField]) -> dict:
        """Find system fields in read fields and return a mapping of field name to index."""
        system_fields = {}
        for i, field in enumerate(read_fields):
            if SpecialFields.is_system_field(field.name):
                system_fields[field.name] = i
        return system_fields

    @staticmethod
    def row_type_with_row_tracking(table_fields: List[DataField],
                                   sequence_number_nullable: bool = False) -> List[DataField]:
        """
        Add row tracking fields.

        Args:
            table_fields: The original table fields
            sequence_number_nullable: Whether sequence number should be nullable
        """
        fields_with_row_tracking = list(table_fields)

        for field in fields_with_row_tracking:
            if (SpecialFields.ROW_ID.name == field.name
                    or SpecialFields.SEQUENCE_NUMBER.name == field.name):
                raise ValueError(
                    f"Row tracking field name '{field.name}' conflicts with existing field names."
                )

        fields_with_row_tracking.append(SpecialFields.ROW_ID)

        if sequence_number_nullable:
            seq_num_field = DataField(
                id=SpecialFields.SEQUENCE_NUMBER.id,
                name=SpecialFields.SEQUENCE_NUMBER.name,
                type=AtomicType("BIGINT", nullable=True)  # Make it nullable
            )
            fields_with_row_tracking.append(seq_num_field)
        else:
            fields_with_row_tracking.append(SpecialFields.SEQUENCE_NUMBER)

        return fields_with_row_tracking
