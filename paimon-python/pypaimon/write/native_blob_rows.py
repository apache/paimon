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

"""Transport row Blob streams into the core URI copy path."""

from collections.abc import Mapping
from decimal import Decimal
from uuid import uuid4

import pyarrow as pa

from pypaimon.schema.data_types import ArrayType, MapType, PyarrowFieldParser, is_blob_file_field
from pypaimon.table.row.blob import Blob, BlobDescriptor
from pypaimon.write.row_utils import inline_blob_value, value_for_arrow


class _BlobRowReader:
    def __init__(self, blob):
        self.blob = blob

    def new_input_stream(self, uri):
        return self.blob.new_input_stream()


class NativeBlobRows:
    """Expose each Python Blob lazily, without opening or materializing it.

    The synthetic descriptors only transport live objects for this operation.
    Core copies their payloads into normal .blob records; these URIs must never
    become stored descriptors. A new reader is kept for each Blob object so
    the core cannot reuse an unrelated Python stream.
    """

    def __init__(self, fallback=None):
        self.readers = {}
        self.fallback = fallback

    def _supports_uri(self, uri):
        # Ordinary descriptors retain the core FileIO/HTTP path. A configured
        # application factory owns other URIs and its errors must propagate.
        return uri in self.readers or self.fallback is not None

    def create(self, uri):
        if uri in self.readers:
            return self.readers[uri]
        return self.fallback.create(uri)

    def to_batch(self, table, rows, names):
        """Transport existing row values; core owns physical input validation."""
        descriptor_fields = table.options.blob_descriptor_fields()
        view_fields = table.options.blob_view_fields()
        fields = []
        columns = []
        for name in names:
            field = table.field_dict[name]
            arrow_field = PyarrowFieldParser.from_paimon_field(field)
            inline = name in descriptor_fields or name in view_fields
            if is_blob_file_field(field) and not inline:
                if isinstance(field.type, MapType) and not table.is_primary_key_table:
                    entries = pa.struct([
                        pa.field('key', arrow_field.type.key_type),
                        arrow_field.type.item_field.with_name('value'),
                    ])
                    arrow_field = pa.field(name, pa.list_(entries), nullable=arrow_field.nullable)
                values = [self.encode(row[name], field.type) for row in rows]
                if (isinstance(field.type, MapType) and not table.is_primary_key_table
                        and pa.types.is_decimal(arrow_field.type.value_type[0].type)):
                    # Preserve arbitrary source scale. The core rounds and checks
                    # precision before serializing the declared decimal key.
                    def key_text(key):
                        if key is None:
                            return None
                        if not isinstance(key, Decimal):
                            raise ValueError('MAP<X, BLOB> DECIMAL key must be a decimal.Decimal.')
                        return format(key, 'f')
                    values = [None if value is None else [(key_text(key), item) for key, item in value]
                              for value in values]
                    entries = arrow_field.type.value_type
                    entries = pa.struct([entries[0].with_type(pa.string()), entries[1]])
                    arrow_field = arrow_field.with_type(pa.list_(entries))
            else:
                values = [value_for_arrow(inline_blob_value(
                    row[name], name in descriptor_fields, name in view_fields), field)
                    for row in rows]
            fields.append(arrow_field)
            columns.append(pa.array(values, type=arrow_field.type))
        return pa.RecordBatch.from_arrays(columns, schema=pa.schema(fields))

    def encode(self, value, data_type):
        if hasattr(value, 'as_py'):
            value = value.as_py()
        if value is None:
            return None
        if isinstance(data_type, ArrayType):
            if isinstance(value, (bytes, bytearray, str)):
                raise ValueError('ARRAY<BLOB> field value must be a list or tuple.')
            return [self.encode(item, data_type.element) for item in value]
        if isinstance(data_type, MapType):
            if isinstance(value, (bytes, bytearray, str)) or not hasattr(value, '__iter__'):
                raise ValueError('MAP<X, BLOB> field value must be a mapping or key/value pairs')
            entries = value.items() if isinstance(value, Mapping) else value
            result = []
            for entry in entries:
                if not isinstance(entry, (list, tuple)) or len(entry) != 2:
                    raise ValueError('MAP<X, BLOB> entries must be key/value pairs')
                result.append((entry[0], self.encode(entry[1], data_type.value)))
            return result
        if isinstance(value, Blob):
            if any(value is placeholder for placeholder in (
                    Blob.PLACE_HOLDER, Blob.ARRAY_PLACE_HOLDER, Blob.MAP_PLACE_HOLDER)):
                raise ValueError('Blob placeholders are reserved for unchanged rows.')
            uri = 'pypaimon-blob-row://' + uuid4().hex
            self.readers[uri] = _BlobRowReader(value)
            return BlobDescriptor(uri, 0, -1).serialize()
        return value
