################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
#################################################################################
from dataclasses import dataclass
from typing import Dict, List, Optional

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.json_util import json_field
from pypaimon.schema.data_types import DataField, PyarrowFieldParser


@dataclass
class Schema:
    FIELD_FIELDS = "fields"
    FIELD_PARTITION_KEYS = "partitionKeys"
    FIELD_PRIMARY_KEYS = "primaryKeys"
    FIELD_OPTIONS = "options"
    FIELD_COMMENT = "comment"

    fields: List[DataField] = json_field(FIELD_FIELDS, default_factory=list)
    partition_keys: List[str] = json_field(FIELD_PARTITION_KEYS, default_factory=list)
    primary_keys: List[str] = json_field(FIELD_PRIMARY_KEYS, default_factory=list)
    options: Dict[str, str] = json_field(FIELD_OPTIONS, default_factory=dict)
    comment: Optional[str] = json_field(FIELD_COMMENT, default=None)

    def __init__(self, fields: Optional[List[DataField]] = None, partition_keys: Optional[List[str]] = None,
                 primary_keys: Optional[List[str]] = None,
                 options: Optional[Dict] = None, comment: Optional[str] = None):
        self.fields = fields if fields is not None else []
        self.partition_keys = partition_keys if partition_keys is not None else []
        self.primary_keys = primary_keys if primary_keys is not None else []
        self.options = options if options is not None else {}
        self.comment = comment

    @staticmethod
    def from_pyarrow_schema(pa_schema: pa.Schema, partition_keys: Optional[List[str]] = None,
                            primary_keys: Optional[List[str]] = None, options: Optional[Dict] = None,
                            comment: Optional[str] = None):
        # Convert PyArrow schema to Paimon fields
        fields = PyarrowFieldParser.to_paimon_schema(pa_schema)

        # Check if Blob type exists in the schema
        has_blob_type = any(
            'blob' in str(field.type).lower()
            for field in fields
        )

        # If Blob type exists, validate required options
        if has_blob_type:
            if options is None:
                options = {}

            required_options = {
                CoreOptions.ROW_TRACKING_ENABLED.key(): 'true',
                CoreOptions.DATA_EVOLUTION_ENABLED.key(): 'true'
            }

            missing_options = []
            for key, expected_value in required_options.items():
                if key not in options or options[key] != expected_value:
                    missing_options.append(f"{key}='{expected_value}'")

            if missing_options:
                raise ValueError(
                    f"Schema contains Blob type but is missing required options: {', '.join(missing_options)}. "
                    f"Please add these options to the schema."
                )

            if primary_keys is not None:
                raise ValueError("Blob type is not supported with primary key.")

        return Schema(fields, partition_keys, primary_keys, options, comment)
