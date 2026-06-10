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

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.schema.schema import Schema


FORMAT_TABLE_TYPE = "format-table"


def validate_create_table(schema: Schema) -> None:
    _validate_blob_format_table(schema)


def _validate_blob_format_table(schema: Schema) -> None:
    options = schema.options or {}
    table_type = str(options.get(CoreOptions.TYPE.key(), "")).lower()
    file_format = str(options.get(CoreOptions.FILE_FORMAT.key(), "")).lower()
    if (
        table_type != FORMAT_TABLE_TYPE
        or file_format != CoreOptions.FILE_FORMAT_BLOB
    ):
        return

    partition_keys = set(schema.partition_keys or [])
    data_fields = [
        field for field in schema.fields if field.name not in partition_keys
    ]
    if len(data_fields) != 1:
        raise ValueError(
            "BLOB format table only supports one non-partition field."
        )
    if getattr(data_fields[0].type, "type", "").upper() != "BLOB":
        raise ValueError(
            "BLOB format table only supports BLOB type as non-partition field."
        )
