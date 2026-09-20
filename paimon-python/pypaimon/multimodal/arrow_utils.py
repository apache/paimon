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

"""Shared Arrow schema validation for multimodal format importers."""

import pyarrow as pa
from pypaimon.schema.arrow_schema import prepare_arrow_input


def strict_arrow_table(
        data,
        target_schema,
        source_path,
        batch_index,
        format_name):
    """Validate one Arrow batch against the target table schema.

    Reject missing, extra, or reordered columns, validate nested nullability,
    and apply only Arrow safe casts before returning a ``pyarrow.Table``.
    """
    if isinstance(data, pa.RecordBatch):
        table = pa.Table.from_batches([data])
    elif isinstance(data, pa.Table):
        table = data
    else:
        raise ValueError(
            "%s transform must return Arrow data or an iterable of Arrow data."
            % format_name)

    missing = [
        name for name in target_schema.names if name not in table.column_names
    ]
    if missing:
        raise ValueError(
            "%s batch %d from %s is missing columns: %s"
            % (format_name, batch_index, source_path, missing))
    extra = [
        name for name in table.column_names if name not in target_schema.names
    ]
    if extra:
        raise ValueError(
            "%s batch %d from %s has unexpected columns: %s"
            % (format_name, batch_index, source_path, extra))
    if table.column_names != target_schema.names:
        raise ValueError(
            "%s batch %d from %s has columns in the wrong order: %s; "
            "expected %s."
            % (format_name, batch_index, source_path, table.column_names,
               target_schema.names))
    try:
        return prepare_arrow_input(table, target_schema, validate_nullability=True)
    except (ValueError, TypeError, NotImplementedError) as error:
        raise ValueError(
            "%s batch %d from %s cannot be converted to the table schema: %s"
            % (format_name, batch_index, source_path, error)) from error
