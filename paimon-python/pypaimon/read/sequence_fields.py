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

"""Sequence-field type rules shared by validation and Python comparators."""

from pypaimon.schema.data_types import AtomicType, DataField, Keyword


def _base_type_name(field: DataField) -> str:
    """Base type keyword of a field, stripping any ``(precision[, scale])``
    parameters and the ``NOT NULL`` suffix. E.g. ``DECIMAL(10, 2)`` and
    ``TIMESTAMP(6)`` map to ``DECIMAL`` / ``TIMESTAMP``.
    """
    return field.type.type.split('(')[0].split(' ')[0]


_COMPARABLE_TYPE_NAMES = frozenset(
    member.value for member in Keyword if member is not Keyword.VARIANT)


def is_comparable_seq_field(field: DataField) -> bool:
    """Whether ``field`` can serve as a ``sequence.field`` for pypaimon's
    atomic comparator: it must be an ``AtomicType`` whose base type name is
    orderable. Complex types (ARRAY / MAP / ROW / ...) and the atomic-but-
    unorderable VARIANT both return ``False``. Used by the read-builder
    guard to reject unsupported sequence fields up front.
    """
    return (isinstance(field.type, AtomicType)
            and _base_type_name(field) in _COMPARABLE_TYPE_NAMES)
