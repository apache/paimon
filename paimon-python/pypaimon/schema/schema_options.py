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

"""Schema option validation and defaults for newly created tables."""

from copy import copy
from typing import Dict

from pypaimon.common.floating_key_order import (
    JAVA_FLOATING_KEY_ORDER_V1, has_floating_keys, validate_floating_key_order)
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.schema.schema import Schema


def validate_schema_options(options: Dict[str, str]) -> None:
    """Validate supplied options without adding defaults or modifying them."""
    core_options = CoreOptions(Options(options))
    target_file_row_num = core_options.target_file_row_num()
    max_value = CoreOptions.TARGET_FILE_ROW_NUM.default_value()
    if target_file_row_num < 1:
        raise ValueError(
            f"{CoreOptions.TARGET_FILE_ROW_NUM.key()} should be at least 1"
        )
    if target_file_row_num > max_value:
        raise ValueError(
            f"{CoreOptions.TARGET_FILE_ROW_NUM.key()} "
            f"should be at most {max_value}"
        )
    key = CoreOptions.PYTHON_FLOATING_KEY_ORDER.key()
    if key in options:
        validate_floating_key_order(options[key])


def prepare_schema_for_create(schema: Schema) -> Schema:
    """Return a copy with new-table defaults; never use for loading or altering."""
    validate_schema_options(schema.options)
    prepared = copy(schema)
    prepared.options = dict(schema.options)
    keys = set(schema.primary_keys) - set(schema.partition_keys)
    if has_floating_keys(field for field in schema.fields if field.name in keys):
        prepared.options.setdefault(CoreOptions.PYTHON_FLOATING_KEY_ORDER.key(), JAVA_FLOATING_KEY_ORDER_V1)
    return prepared
