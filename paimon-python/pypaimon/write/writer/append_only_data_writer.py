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

import pyarrow as pa
import pyarrow.parquet as pq
import uuid

from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.writer.data_writer import DataWriter
from pypaimon.write.writer.write_buffer import WriteBuffer


class AppendOnlyDataWriter(DataWriter):
    """Data writer for append-only tables."""

    _ROW_GROUP_MAX_ROWS = 1024 * 1024

    def _process_data(self, data: pa.RecordBatch) -> pa.Table:
        return pa.Table.from_batches([data])

    def _merge_data(self, existing_data: pa.Table, new_data: pa.Table) -> pa.Table:
        return pa.concat_tables([existing_data, new_data])

    @staticmethod
    def _row_group_slice(batch, offset, count):
        piece = batch.slice(offset, count)
        # Arrow 6 nbytes counts full backing buffers even for slices. Compact
        # the slice there so both accounting and retained buffers stay bounded.
        if int(pa.__version__.split('.')[0]) < 7:
            piece = pa.RecordBatch.from_arrays(
                [pa.concat_arrays([column]) for column in piece.columns], schema=piece.schema)
        return piece

    def _row_groups(self, batches):
        """Bound row-group buffering independently of reader batch boundaries.

        Arrow bytes are an estimate, not the encoded Parquet block size.
        One oversized row and the current input batch can exceed the target.
        """
        configured = self.options.file_block_size()
        target_bytes = configured.get_bytes() if configured is not None else 128 * 1024 * 1024
        if target_bytes <= 0:
            raise ValueError('file.block-size must be positive')
        buffer = WriteBuffer(self._merge_data)
        try:
            for batch in batches:
                offset = 0
                while offset < batch.num_rows:
                    count = min(batch.num_rows - offset,
                                self._ROW_GROUP_MAX_ROWS - buffer.num_rows)
                    piece = self._row_group_slice(batch, offset, count)
                    available = target_bytes - buffer.nbytes
                    if piece.nbytes > available:
                        low, high = 0, count
                        while low < high:
                            middle = (low + high + 1) // 2
                            if self._row_group_slice(piece, 0, middle).nbytes <= available:
                                low = middle
                            else:
                                high = middle - 1
                        count = low
                        if count == 0 and buffer.num_rows:
                            yield buffer.take()
                            continue
                        count = max(1, count)
                        piece = self._row_group_slice(piece, 0, count)
                    buffer.append(pa.Table.from_batches([piece]))
                    offset += count
                    del piece
                    if buffer.nbytes >= target_bytes or buffer.num_rows >= self._ROW_GROUP_MAX_ROWS:
                        yield buffer.take()
                del batch
            if buffer.num_rows:
                yield buffer.take()
        finally:
            buffer.reset()

    def _write_batches(self, batches):
        """Write one overlay file with bounded row-group buffering.

        Used for ordinary Parquet column updates, without sidecars or
        shredding. Keep file boundaries while bounding payload memory.
        """
        file_name = '{}{}-0.parquet'.format(
            self.options.data_file_prefix(), uuid.uuid4())
        file_path = self._generate_file_path(file_name)
        row_count = 0
        stats = {}
        fields = []

        kwargs = {'compression': self.compression}
        if self.compression.lower() == 'zstd':
            kwargs['compression_level'] = self.zstd_level
        groups = self._row_groups(batches)
        try:
            # Like SingleFileWriter, own the format writer and its output stream.
            # FileIO supplies storage access, not the format writer lifecycle.
            with self.file_io.new_output_stream(file_path) as stream:
                writer = None
                try:
                    for batch in groups:
                        if not batch.num_rows:
                            continue
                        if writer is None:
                            writer = pq.ParquetWriter(stream, batch.schema, **kwargs)
                            if self.options.metadata_stats_enabled():
                                fields = PyarrowFieldParser.to_paimon_schema(batch.schema)
                        writer.write_table(batch, row_group_size=batch.num_rows)
                        row_count += batch.num_rows
                        for field in fields:
                            current = self._get_column_stats(batch, field.name)
                            previous = stats.get(field.name)
                            if previous is not None:
                                current['null_counts'] += previous['null_counts']
                                for key, choose in (('min_values', min), ('max_values', max)):
                                    values = [v for v in (previous[key], current[key]) if v is not None]
                                    current[key] = choose(values) if values else None
                            stats[field.name] = current
                        del batch
                finally:
                    if writer is not None:
                        writer.close()
            if not row_count:
                self.file_io.delete_quietly(file_path)
                return []
            meta = self._create_data_file_meta(
                file_name=file_name,
                file_path=file_path,
                row_count=row_count,
                min_key=GenericRow([], []), max_key=GenericRow([], []),
                key_stats=SimpleStats.empty_stats(),
                value_stats=self._collect_value_stats(None, fields, stats),
                min_sequence_number=0, max_sequence_number=0,
            )
            self.committed_files.append(meta)
            return [meta]
        except Exception:
            self.file_io.delete_quietly(file_path)
            raise
        finally:
            groups.close()
