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

"""Resolve primary-key managed BLOB descriptors to payload bytes."""

from pypaimon.blob.managed_blob_reference_collector import _map_items
from pypaimon.schema.data_types import (
    is_array_blob_type,
    is_blob_type,
    is_map_blob_type,
    managed_blob_field_names,
)
from pypaimon.table.row.blob import Blob, BlobDescriptor
from pypaimon.utils.path import resolve_path


def resolve_table_blob_uri(table_path, uri):
    """Join a table-relative pack URI onto the table directory.

    Absolute paths and URIs that already carry a scheme are left unchanged so
    descriptors written with a filesystem path still open.
    """
    if uri is None or table_path is None:
        return uri
    normalized = uri.replace('\\', '/')
    if (normalized.startswith('/')
            or normalized.startswith('file:')
            or '://' in normalized):
        return uri
    return resolve_path(str(table_path), normalized)


def managed_blob_output_specs(table, fields, blob_as_descriptor):
    """``(index, name, kind)`` for primary-key managed BLOB columns.

    Append tables store these columns in dedicated ``.blob`` files and the
    reader has already turned them into payload bytes. Managed packs exist
    only on primary-key tables.
    """
    if blob_as_descriptor or not table.is_primary_key_table:
        return []
    inline = (table.options.blob_descriptor_fields()
              | table.options.blob_view_fields())
    managed = managed_blob_field_names(table.fields, inline)
    managed_ids = {field.id for field in table.fields if field.name in managed}
    specs = []
    for index, field in enumerate(fields):
        # MAP-key projections rename the field but retain its source ID.
        if field.id not in managed_ids:
            continue
        if is_blob_type(field.type):
            kind = 'scalar'
        elif is_array_blob_type(field.type):
            kind = 'array'
        elif is_map_blob_type(field.type):
            kind = 'map'
        else:
            continue
        specs.append((index, field.name, kind))
    return specs


def materialize_managed_blob(value, kind, file_io, table_path):
    """Replace one managed BLOB cell with its pack payload."""
    if hasattr(value, 'as_py'):
        value = value.as_py()
    if value is None:
        return None
    if kind == 'scalar':
        return _descriptor_payload(value, file_io, table_path)
    if kind == 'array':
        return [
            None if element is None
            else _descriptor_payload(element, file_io, table_path)
            for element in value
        ]
    if kind == 'map':
        entries = _map_entries(value)
        externalized = []
        for key, map_value in entries:
            if map_value is None:
                externalized.append((key, None))
            else:
                externalized.append(
                    (key, _descriptor_payload(map_value, file_io, table_path)))
        if isinstance(value, dict):
            return dict(externalized)
        if (isinstance(value, list) and value
                and isinstance(value[0], dict)):
            return [{'key': key, 'value': map_value} for key, map_value in externalized]
        return externalized
    return value


def _map_entries(value):
    if value is None:
        return []
    if isinstance(value, dict):
        return list(value.items())
    if isinstance(value, list) and value and isinstance(value[0], dict):
        return [(entry.get('key'), entry.get('value')) for entry in value]
    return list(_map_items(value))


def _descriptor_payload(raw, file_io, table_path):
    if hasattr(raw, 'as_py'):
        raw = raw.as_py()
    if raw is None:
        return None
    if isinstance(raw, str):
        raw = raw.encode('utf-8')
    descriptor = BlobDescriptor.deserialize(bytes(raw))
    uri = resolve_table_blob_uri(table_path, descriptor.uri)
    return Blob.from_file(
        file_io, uri, descriptor.offset, descriptor.length).to_data()
