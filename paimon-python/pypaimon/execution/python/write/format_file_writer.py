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

"""Physical format output owned by the selected Python execution backend.

FileIO supplies streams and locations; it does not construct table codecs.
"""

import os

import pyarrow as pa

from pypaimon.execution.python.write.blob_format_writer import BlobFormatWriter
from pypaimon.execution.python.write.writer.format_row_writer import FormatRowWriter
from pypaimon.filesystem.vortex_utils import to_vortex_specified
from pypaimon.schema.data_types import AtomicType, DataField, PyarrowFieldParser


def write_row_file(file_io, path, data, fields=None, zstd_level=1):
    try:
        if fields is None:
            fields = PyarrowFieldParser.to_paimon_schema(data.schema)
        with file_io.new_output_stream(path) as output_stream:
            writer = FormatRowWriter(output_stream, fields, zstd_level=zstd_level)
            writer.write_table(data)
            writer.close()
    except Exception as error:
        file_io.delete_quietly(path)
        raise RuntimeError("Failed to write row file {}: {}".format(path, error)) from error


def write_blob_file(file_io, path, data):
    try:
        if data.num_columns != 1:
            raise RuntimeError(
                "Blob format only supports a single column, got {} columns".format(data.num_columns))
        field = data.schema[0]
        if pa.types.is_large_binary(field.type):
            field_type = AtomicType("BLOB")
        else:
            field_type = PyarrowFieldParser.to_paimon_type(field.type, field.nullable)
        fields = [DataField(0, field.name, field_type)]
        values = data.column(0).to_pylist()
        uri_reader_factory = file_io.uri_reader_factory
        with file_io.new_output_stream(path) as output_stream:
            writer = BlobFormatWriter(output_stream)
            for value in values:
                writer.write_value(value, fields, uri_reader_factory)
            writer.close()
    except Exception as error:
        file_io.delete_quietly(path)
        raise RuntimeError("Failed to write blob file {}: {}".format(path, error)) from error


def write_vortex_file(file_io, path, data):
    try:
        import vortex

        file_path, store_kwargs = to_vortex_specified(file_io, path)
        scheme, _, _ = file_io.parse_location(path)
        if scheme in {'file', None} or not scheme:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
        if store_kwargs:
            from vortex import store
            store.from_url(file_path, **store_kwargs).write(vortex.array(data))
        else:
            from vortex._lib.io import write
            write(vortex.array(data), file_path)
    except Exception as error:
        file_io.delete_quietly(path)
        raise RuntimeError("Failed to write Vortex file {}: {}".format(path, error)) from error
