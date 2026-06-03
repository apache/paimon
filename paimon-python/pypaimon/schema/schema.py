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

from dataclasses import dataclass
from typing import Dict, List, Optional

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.json_util import json_field
from pypaimon.schema.data_types import DataField, PyarrowFieldParser, VectorType


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

        # Primary key fields must be NOT NULL
        pk_set = set(primary_keys) if primary_keys else set()
        if pk_set:
            for field in fields:
                if field.name in pk_set:
                    field.type.nullable = False

        # Validate Blob type fields in the schema
        Schema._validate_blob_fields(fields, options, primary_keys)

        # Check if Vector type with dedicated file format
        vector_names = [
            field.name for field in fields
            if isinstance(field.type, VectorType)
        ]
        vector_file_format = options.get(CoreOptions.VECTOR_FILE_FORMAT.key(), '') if options else ''

        if vector_names and vector_file_format:
            if options is None:
                options = {}

            if len(fields) <= len(vector_names):
                raise ValueError(
                    "Table with VECTOR type column must have other normal columns."
                )

            partition_key_set = set(partition_keys) if partition_keys else set()
            vector_partitions = [n for n in vector_names if n in partition_key_set]
            if vector_partitions:
                raise ValueError(
                    "The vector-store columns can not be part of partition keys."
                )

            required_options = {
                CoreOptions.ROW_TRACKING_ENABLED.key(): 'true',
                CoreOptions.DATA_EVOLUTION_ENABLED.key(): 'true',
            }
            missing = [
                f"{k}='{v}'" for k, v in required_options.items()
                if options.get(k) != v
            ]
            if missing:
                raise ValueError(
                    f"Table with vector-store file format requires: {', '.join(missing)}."
                )

        return Schema(fields, partition_keys, primary_keys, options, comment)

    @staticmethod
    def _validate_blob_fields(fields, options, primary_keys):
        """Validate blob field configurations in the schema."""

        if options is None:
            options = {}

        blob_field_names = {
            field.name for field in fields if 'blob' in str(field.type).lower()
        }

        if len(fields) <= len(blob_field_names):
            raise ValueError(
                "Table with BLOB type column must have other normal columns."
            )

        core_options = CoreOptions.from_dict(options)

        # Validate blob-field configuration
        configured_blob_fields = core_options.blob_field()
        for field in configured_blob_fields:
            if field not in blob_field_names:
                raise ValueError(
                    "Field '{}' in '{}' must be a BLOB field in table schema.".format(
                        field, CoreOptions.BLOB_FIELD.key()
                    )
                )

        # Validate blob-descriptor-field and blob-view-field configuration
        descriptor_fields = core_options.blob_descriptor_fields()
        view_fields = core_options.blob_view_fields()

        # Check that configured fields are BLOB type
        all_inline_fields = descriptor_fields.union(view_fields)
        non_blob_inline_fields = all_inline_fields.difference(blob_field_names)
        if non_blob_inline_fields:
            raise ValueError(
                "Fields in 'blob-descriptor-field' or 'blob-view-field' must be blob fields "
                "in schema. Non-BLOB fields: {}".format(sorted(non_blob_inline_fields))
            )

        # Check for overlap between descriptor and view fields
        overlapping_inline_fields = descriptor_fields.intersection(view_fields)
        if overlapping_inline_fields:
            raise ValueError(
                "Fields in 'blob-descriptor-field' and 'blob-view-field' must not overlap. "
                "Overlapping fields: {}".format(sorted(overlapping_inline_fields))
            )

        # Apply BLOB-specific table constraints only when BLOB fields exist
        if blob_field_names:
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
