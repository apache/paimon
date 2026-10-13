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

"""Variant extractions encoded in the reader's Paimon ROW read type."""

from typing import Any, Dict, List, Optional

import pyarrow

from pypaimon.schema.data_types import AtomicType, DataField, RowType


_VARIANT_METADATA_PREFIX = '__VARIANT_METADATA'


def with_variant_extractions(
        read_type: List[DataField],
        extractions: Optional[Dict[str, Dict[str, Any]]]) -> List[DataField]:
    """Resolve expression paths once, using Java's DataField description format."""
    if not extractions:
        return read_type
    normalized = {((name,) if isinstance(name, str) else tuple(name)): options
                  for name, options in extractions.items()}
    fields = []
    projected = set()
    for field in read_type:
        options = normalized.get((field.name,))
        nested = {path[1:]: options for path, options in normalized.items()
                  if len(path) > 1 and path[0] == field.name}
        if nested and isinstance(field.type, RowType):
            children = with_variant_extractions(field.type.fields, nested)
            fields.append(DataField(field.id, field.name, RowType(field.type.nullable, children),
                                    field.description, field.default_value))
            projected.update(path for path in normalized if path[0] == field.name)
            continue
        if options is None:
            fields.append(field)
            continue
        if not isinstance(field.type, AtomicType) or field.type.type.upper() != 'VARIANT':
            raise ValueError("Variant extraction column %r must be VARIANT" % field.name)
        if options['target_type'] != pyarrow.float32():
            raise ValueError("Variant extraction target type must be float32")
        error_policy = options['fail_on_error']
        if isinstance(error_policy, bool):
            error_policy = [error_policy] * len(options['paths'])
        if len(error_policy) != len(options['paths']):
            raise ValueError("Variant paths and error policies must match")
        children = []
        for index, (path, fail_on_error) in enumerate(
                zip(options['paths'], error_policy)):
            if ';' in path:
                raise ValueError(
                    "Variant extraction path must not contain ';': %s" % path)
            children.append(DataField(
                index, str(index), AtomicType('FLOAT'),
                '%s%s;%s;UTC' % (
                    _VARIANT_METADATA_PREFIX, path, str(fail_on_error).lower())))
        projected.add((field.name,))
        fields.append(DataField(
            field.id, field.name, RowType(field.type.nullable, children),
            field.description, field.default_value))
    missing = set(normalized) - projected
    if missing:
        raise ValueError(
            "Variant extraction column %r is not in the read type" % (min(missing),))
    return fields


def has_variant_extractions(read_type: List[DataField]) -> bool:
    """Recognize typed Variant children from their read-type metadata."""
    return any(
        isinstance(field.type, RowType)
        and bool(field.type.fields)
        and all(child.description is not None
                and child.description.startswith(_VARIANT_METADATA_PREFIX)
                for child in field.type.fields)
        for field in read_type) or any(
            isinstance(field.type, RowType) and has_variant_extractions(field.type.fields)
            for field in read_type)
