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

"""Column comment directive utilities for BLOB / VECTOR type conversion.

Mirrors Java's ColumnDirectiveUtils. Supported directives:
  __BLOB_FIELD                   -> blob-field
  __BLOB_DESCRIPTOR_FIELD        -> blob-descriptor-field
  __BLOB_VIEW_FIELD              -> blob-view-field
  __BLOB_EXTERNAL_STORAGE_FIELD  -> blob-external-storage-field + blob-descriptor-field
  __VECTOR_FIELD;dim             -> vector-field
"""

from dataclasses import dataclass
from typing import Dict, List, Optional

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.schema.data_types import (ArrayType, AtomicType, DataField,
                                        DataType, VectorType)

BLOB_FIELD_DIRECTIVE = "__BLOB_FIELD"
BLOB_DESCRIPTOR_FIELD_DIRECTIVE = "__BLOB_DESCRIPTOR_FIELD"
BLOB_VIEW_FIELD_DIRECTIVE = "__BLOB_VIEW_FIELD"
BLOB_EXTERNAL_STORAGE_FIELD_DIRECTIVE = "__BLOB_EXTERNAL_STORAGE_FIELD"
VECTOR_FIELD_DIRECTIVE = "__VECTOR_FIELD"

_BLOB_DIRECTIVES = [
    (BLOB_EXTERNAL_STORAGE_FIELD_DIRECTIVE, CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key()),
    (BLOB_VIEW_FIELD_DIRECTIVE, CoreOptions.BLOB_VIEW_FIELD.key()),
    (BLOB_DESCRIPTOR_FIELD_DIRECTIVE, CoreOptions.BLOB_DESCRIPTOR_FIELD.key()),
    (BLOB_FIELD_DIRECTIVE, CoreOptions.BLOB_FIELD.key()),
]

_BLOB_OPTIONS = [
    CoreOptions.BLOB_FIELD,
    CoreOptions.BLOB_DESCRIPTOR_FIELD,
    CoreOptions.BLOB_VIEW_FIELD,
    CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD,
]

_VECTOR_OPTIONS = [
    CoreOptions.VECTOR_FIELD,
]

# Legacy fallback keys that map to canonical option keys.
# Java's blob-descriptor-field has fallback key "blob.stored-descriptor-fields".
_FALLBACK_KEYS = {
    CoreOptions.BLOB_DESCRIPTOR_FIELD.key(): ["blob.stored-descriptor-fields"],
}


@dataclass
class ParsedDirective:
    option_key: str
    real_comment: Optional[str]
    is_vector: bool
    vector_dim: int


@dataclass
class ConvertedColumn:
    type: DataType
    comment: Optional[str]


def parse_add_column_comment(comment: Optional[str]) -> Optional[ParsedDirective]:
    if comment is None:
        return None
    comment = comment.strip()
    if not comment:
        return None

    if comment.startswith("__VECTOR"):
        return _parse_vector_directive(comment)
    if not comment.startswith("__BLOB"):
        return None

    for marker, option_key in _BLOB_DIRECTIVES:
        if not comment.startswith(marker):
            continue
        if len(comment) == len(marker):
            real_comment = None
        elif comment[len(marker)] == ';':
            real_comment = comment[len(marker) + 1:].strip() or None
        else:
            continue
        return ParsedDirective(option_key, real_comment, False, 0)

    raise ValueError(
        f"Unsupported BLOB directive in column comment: '{comment}'. "
        f"Supported directives are '{BLOB_FIELD_DIRECTIVE}', "
        f"'{BLOB_DESCRIPTOR_FIELD_DIRECTIVE}', '{BLOB_VIEW_FIELD_DIRECTIVE}' "
        f"and '{BLOB_EXTERNAL_STORAGE_FIELD_DIRECTIVE}'."
    )


def _parse_vector_directive(comment: str) -> ParsedDirective:
    marker = VECTOR_FIELD_DIRECTIVE
    if not comment.startswith(marker):
        raise ValueError(
            f"Unsupported VECTOR directive in column comment: '{comment}'. "
            f"Supported directive is '{VECTOR_FIELD_DIRECTIVE}'."
        )
    if len(comment) == len(marker) or comment[len(marker)] != ';':
        raise ValueError(
            f"VECTOR directive '{comment}' requires a dimension, "
            f"e.g. '{VECTOR_FIELD_DIRECTIVE};128' or "
            f"'{VECTOR_FIELD_DIRECTIVE};128; my comment'."
        )

    rest = comment[len(marker) + 1:]
    semi_pos = rest.find(';')
    if semi_pos < 0:
        dim_str = rest.strip()
        real_comment = None
    else:
        dim_str = rest[:semi_pos].strip()
        real_comment = rest[semi_pos + 1:].strip() or None

    try:
        dim = int(dim_str)
    except ValueError:
        raise ValueError(
            f"Expected an integer dimension after '{VECTOR_FIELD_DIRECTIVE};', "
            f"but got: '{dim_str}'."
        )
    if dim < 1:
        raise ValueError(f"Vector dimension must be >= 1, but got: {dim}.")

    option_key = CoreOptions.VECTOR_FIELD.key()
    return ParsedDirective(option_key, real_comment, True, dim)


def _convert_type(directive: ParsedDirective, field_name: str, source_type: DataType) -> DataType:
    if directive.is_vector:
        if not isinstance(source_type, ArrayType):
            raise ValueError(
                f"Column {field_name} declared with a VECTOR directive "
                f"must be of ARRAY type, but was {source_type}."
            )
        return VectorType(source_type.nullable, source_type.element, directive.vector_dim)
    else:
        type_name = getattr(source_type, 'type', None) if isinstance(source_type, AtomicType) else None
        if type_name not in ('VARBINARY', 'BINARY', 'BYTES', 'BLOB'):
            raise ValueError(
                f"Column {field_name} declared with a BLOB directive "
                f"must be of BYTES, BINARY or BLOB type, but was {source_type}."
            )
        return AtomicType('BLOB', source_type.nullable)


def _modify_field_options(option_key: str, field_name: str, options: Dict[str, str]):
    existing = options.get(option_key)
    if not existing:
        for fk in _FALLBACK_KEYS.get(option_key, []):
            fallback_value = options.pop(fk, None)
            if fallback_value:
                existing = fallback_value
                break
    if existing:
        options[option_key] = existing + "," + field_name
    else:
        options[option_key] = field_name


def apply_add_column_directive(
    comment: Optional[str],
    field_name: str,
    source_type: DataType,
    options: Dict[str, str],
) -> Optional[ConvertedColumn]:
    directive = parse_add_column_comment(comment)
    if directive is None:
        return None
    new_type = _convert_type(directive, field_name, source_type)
    _modify_field_options(directive.option_key, field_name, options)
    if directive.option_key == CoreOptions.BLOB_EXTERNAL_STORAGE_FIELD.key():
        _modify_field_options(CoreOptions.BLOB_DESCRIPTOR_FIELD.key(), field_name, options)
    return ConvertedColumn(new_type, directive.real_comment)


def apply_directives(fields: List[DataField], options: Dict[str, str]):
    """Process comment directives on fields for CREATE TABLE.

    Modifies fields list and options dict in place.
    Returns True if any directive was applied.
    """
    changed = False
    for i, field in enumerate(list(fields)):
        converted = apply_add_column_directive(
            field.description, field.name, field.type, options
        )
        if converted is not None:
            changed = True
            fields[i] = DataField(field.id, field.name, converted.type, converted.comment)
    return changed


def _remove_from_csv_option(key: str, field_name: str, options: Dict[str, str]):
    existing = options.get(key)
    if not existing:
        return
    parts = [v.strip() for v in existing.split(",") if v.strip() and v.strip() != field_name]
    if parts:
        options[key] = ",".join(parts)
    else:
        options.pop(key, None)


def remove_dropped_directive_options(
    field_name: str, type_root: str, options: Dict[str, str]
):
    """Remove directive-managed options when a BLOB or VECTOR column is dropped."""
    if type_root == 'BLOB':
        for opt in _BLOB_OPTIONS:
            _remove_from_csv_option(opt.key(), field_name, options)
            for fk in _FALLBACK_KEYS.get(opt.key(), []):
                _remove_from_csv_option(fk, field_name, options)
    elif type_root == 'VECTOR':
        for opt in _VECTOR_OPTIONS:
            _remove_from_csv_option(opt.key(), field_name, options)
        options.pop(f"field.{field_name}.vector-dim", None)
