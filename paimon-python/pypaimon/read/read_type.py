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

"""Canonical reader types and the separate, flat result projection.

Reader types preserve table field IDs and nested ROW structure. Temporary MAP
ROW metadata is identical to Java MapSelectedKeysMetadataUtils. Output aliases
and order never change the reader type.
"""

from copy import copy
from typing import NamedTuple

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.data.map_shared_shredding import (
    is_map_selected_keys_field, map_selected_keys_field)
from pypaimon.schema.data_types import DataField, MapType, RowType
from pypaimon.utils.projection import Projection


class OutputProjection(NamedTuple):
    columns: list
    named: bool = False


def project_read_type(fields, paths):
    """Compile source paths once, with whole-column requests taking precedence."""
    result = []
    for field in fields:
        tails = [path[1:] for path in paths if path[0] == field.name]
        if not tails:
            continue
        if any(not tail for tail in tails):
            result.append(field)
        elif isinstance(field.type, RowType):
            children = project_read_type(field.type.fields, tails)
            result.append(DataField(field.id, field.name,
                                    RowType(field.type.nullable, children),
                                    field.description, field.default_value))
        elif isinstance(field.type, MapType) and all(len(tail) == 1 for tail in tails):
            keys = list(dict.fromkeys(tail[0] for tail in tails))
            try:
                result.append(map_selected_keys_field(field, keys))
            except ValueError:
                # The Java metadata cannot encode every literal key. A whole
                # MAP read still supplies the result projection's lookup.
                result.append(field)
        else:
            raise ValueError("Invalid reader path for %r" % field.name)
    # Preserve first source occurrence, including top-level output reordering.
    order = list(dict.fromkeys(path[0] for path in paths))
    return sorted(result, key=lambda field: order.index(field.name))


def _adapter_source(source, requested):
    if isinstance(source.type, RowType) and isinstance(requested.type, RowType):
        requested_by_id = {field.id: field for field in requested.type.fields}
        children = [_adapter_source(child, requested_by_id[child.id])
                    if child.id in requested_by_id else child for child in source.type.fields]
        return DataField(source.id, source.name, RowType(source.type.nullable, children),
                         source.description, source.default_value)
    if isinstance(requested.type, RowType) and not isinstance(source.type, MapType):
        return requested
    return source


def reader_adapter(read_type, table_fields):
    """Derive flat Python-format adapter fields from the canonical read type.

    Python split readers consume flat leaf requests. This is local adaptation;
    no source paths are stored by builders or transported to Native or Ray.
    """
    paths = []
    source_fields = list(table_fields)
    table_by_id = {field.id: field for field in table_fields}
    positions_by_id = {field.id: i for i, field in enumerate(table_fields)}

    def visit(target, source, path):
        if is_map_selected_keys_field(target):
            for child in target.type.fields:
                paths.append(path + [child.name])
            return
        if (isinstance(target.type, RowType) and isinstance(source.type, RowType)
                and target.type != source.type):
            by_id = {field.id: field for field in source.type.fields}
            for child in target.type.fields:
                visit(child, by_id.get(child.id, child), path + [child.name])
        else:
            paths.append(path)

    for field in read_type:
        source = table_by_id.get(field.id, field)
        adapted = _adapter_source(source, field)
        if field.id in positions_by_id:
            source_fields[positions_by_id[field.id]] = adapted
        else:
            source_fields.append(adapted)
        visit(field, source, [field.name])
    # Resolve aliases against the complete table, including unprojected
    # physical columns which can be referenced by a predicate.
    indexes = []
    for path in paths:
        fields = source_fields
        steps = []
        for index, name in enumerate(path):
            position = next(i for i, field in enumerate(fields) if field.name == name)
            field = fields[position]
            steps.append(position)
            if index + 1 < len(path):
                if isinstance(field.type, MapType):
                    from pypaimon.utils.projection import MapKey
                    steps.append(MapKey(path[index + 1]))
                    break
                fields = field.type.fields
        indexes.append(steps)
    flat_fields = Projection.of(indexes).project(source_fields)
    return flat_fields, paths


def _schema_path(schema, path):
    field = schema.field(path[0])
    nullable = field.nullable
    for name in path[1:]:
        if pa.types.is_struct(field.type):
            child = field.type[name]
            nullable = nullable or child.nullable
            field = child
        elif pa.types.is_map(field.type):
            field = pa.field(name, field.type.item_type, nullable=True)
            nullable = True
        else:
            raise ValueError("Cannot project path %r through %s" % (path, field.type))
    return field.with_nullable(nullable)


def output_schema(schema, projection):
    if projection is None:
        return schema
    return pa.schema([_schema_path(schema, path).with_name(alias)
                      for alias, path in projection.columns], metadata=schema.metadata)


def extract_array(batch, path):
    array = batch.column(path[0])
    parents = []
    for name in path[1:]:
        if pa.types.is_struct(array.type):
            parents.append(pc.is_null(array))
            array = array.field(name)
        elif pa.types.is_map(array.type):
            array = pc.map_lookup(array, name, 'first')
        else:
            raise ValueError("Cannot project path %r through %s" % (path, array.type))
    for nulls in reversed(parents):
        array = pc.if_else(nulls, pa.scalar(None, type=array.type), array)
    return array


def adapter_output_paths(projection, adapter_fields, adapter_paths):
    """Resolve final outputs against a derived format adapter."""
    if projection is None:
        return [(field.name, [field.name]) for field in adapter_fields]
    outputs = []
    for alias, requested in projection.columns:
        matches = [(field, path) for field, path in zip(adapter_fields, adapter_paths)
                   if requested[:len(path)] == path]
        if not matches:
            raise ValueError("Projection path %r is missing from the reader type" % requested)
        field, path = max(matches, key=lambda pair: len(pair[1]))
        outputs.append((alias, [field.name] + requested[len(path):]))
    return outputs


def output_fields(read_type, projection):
    if projection is None:
        return read_type
    result = []
    for alias, path in projection.columns:
        fields = read_type
        nullable = False
        field = None
        for name in path:
            field = next(field for field in fields if field.name == name)
            nullable = nullable or field.type.nullable
            if isinstance(field.type, RowType):
                fields = field.type.fields
            elif isinstance(field.type, MapType):
                fields = [DataField(field.id, path[-1], field.type.value)]
                nullable = True
        data_type = copy(field.type)
        data_type.nullable = nullable
        result.append(DataField(field.id, alias, data_type, field.description, field.default_value))
    return result
