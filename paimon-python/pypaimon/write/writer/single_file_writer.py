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

from contextlib import suppress

import pyarrow.parquet as pq

from pypaimon.common.options.core_options import CoreOptions


class SingleFileWriter:
    """Write one file incrementally; currently only Parquet is supported."""

    def __init__(self, file_io, path, schema, file_format, compression, zstd_level,
                 stats_fields, stats_collector, parquet_options=None):
        if file_format != CoreOptions.FILE_FORMAT_PARQUET:
            raise NotImplementedError(
                'SingleFileWriter only supports Parquet, got {}'.format(file_format))
        self._file_io = file_io
        self._path = path
        self._stats_fields = stats_fields
        self._stats_collector = stats_collector
        self._stream = None
        self._writer = None
        self._owns_file = False
        self._closed = False
        self.row_count = 0
        self.column_stats = {}

        kwargs = dict(parquet_options or {}, compression=compression)
        if compression.lower() == 'zstd':
            kwargs['compression_level'] = zstd_level
        try:
            self._stream = file_io.new_output_stream(path)
            self._owns_file = True
            self._writer = pq.ParquetWriter(self._stream, schema, **kwargs)
        except Exception:
            self.abort()
            raise

    def write(self, data, row_group_size=None):
        if self._closed:
            raise RuntimeError('Writer is already closed')
        try:
            kwargs = {}
            if row_group_size is not None:
                kwargs['row_group_size'] = row_group_size
            self._writer.write_table(data, **kwargs)
            self.row_count += data.num_rows
            for field in self._stats_fields:
                current = self._stats_collector(data, field.name)
                previous = self.column_stats.get(field.name)
                if previous is not None:
                    current['null_counts'] += previous['null_counts']
                    for key, choose in (
                            ('min_values', min), ('max_values', max)):
                        values = [
                            value for value in (previous[key], current[key])
                            if value is not None
                        ]
                        current[key] = choose(values) if values else None
                self.column_stats[field.name] = current
        except Exception:
            self.abort()
            raise

    def close(self):
        if self._closed:
            return
        try:
            self._writer.close()
            self._writer = None
            self._stream.close()
            self._stream = None
            self._closed = True
        except Exception:
            self.abort()
            raise

    def abort(self):
        if self._writer is not None:
            with suppress(Exception):
                self._writer.close()
            self._writer = None
        if self._stream is not None:
            with suppress(Exception):
                self._stream.close()
            self._stream = None
        self._closed = True
        if self._owns_file:
            self._file_io.delete_quietly(self._path)
            self._owns_file = False
