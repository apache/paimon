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

"""Explicit upgrade acknowledgement for Python floating primary keys."""

from typing import Iterable, TYPE_CHECKING

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.schema.data_types import DataField, is_floating_type

if TYPE_CHECKING:
    from pypaimon.table.file_store_table import FileStoreTable


# Persisted FLOAT/DOUBLE ordering contract, not a Java runtime version.
# Nulls first; other values follow Float.compare/Double.compare:
# -inf < ... < -0.0 < +0.0 < ... < +inf < NaN, with all NaNs equal.
JAVA_FLOATING_KEY_ORDER_V1 = 'java-v1'


class UnverifiedFloatingKeyOrderError(ValueError):
    """The table has not acknowledged Java-compatible floating key order."""


def has_floating_keys(fields: Iterable[DataField]) -> bool:
    return any(is_floating_type(field.type) for field in fields)


def validate_floating_key_order(value: object) -> None:
    if value != JAVA_FLOATING_KEY_ORDER_V1:
        raise ValueError("Unsupported {}={!r}; expected {!r}.".format(
            CoreOptions.PYTHON_FLOATING_KEY_ORDER.key(), value, JAVA_FLOATING_KEY_ORDER_V1))


def check_floating_key_order(table: 'FileStoreTable') -> None:
    if not table.is_primary_key_table or not has_floating_keys(table.trimmed_primary_keys_fields):
        return
    key = CoreOptions.PYTHON_FLOATING_KEY_ORDER.key()
    options = table.options.options.to_map()
    if key in options:
        validate_floating_key_order(options[key])
        return
    raise UnverifiedFloatingKeyOrderError(
        "Floating primary-key order is unverified for table '{}'. "
        "This includes unmarked Java-created tables and does not imply corruption. "
        "Stop old Python writers and verify the data and history you will read, "
        "or rebuild into a new table, before explicitly setting {}={}. "
        "Setting the option does not repair data. "
        "See the PyPaimon floating-key migration guide.".format(
            table.identifier, key, JAVA_FLOATING_KEY_ORDER_V1))
