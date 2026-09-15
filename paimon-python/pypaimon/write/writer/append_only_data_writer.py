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

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.writer.data_writer import DataWriter


class AppendOnlyDataWriter(DataWriter):
    """Data writer for append-only tables."""

    def _process_data(self, data: pa.RecordBatch) -> pa.Table:
        return pa.Table.from_batches([data])

    def _merge_data(self, existing_data: pa.Table, new_data: pa.Table) -> pa.Table:
        return pa.concat_tables([existing_data, new_data])

    def write_parquet_batches(self, batches):
        """Write one overlay file without retaining the input batches.

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
        try:
            with self.file_io.new_output_stream(file_path) as stream:
                writer = None
                try:
                    for batch in batches:
                        if not batch.num_rows:
                            continue
                        if writer is None:
                            writer = pq.ParquetWriter(stream, batch.schema, **kwargs)
                            if self.options.metadata_stats_enabled():
                                fields = PyarrowFieldParser.to_paimon_schema(batch.schema)
                        writer.write_table(pa.Table.from_batches([batch]))
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
                        # Do not retain the last payload while requesting the next.
                        del batch
                finally:
                    if writer is not None:
                        writer.close()
            if not row_count:
                self.file_io.delete_quietly(file_path)
                return []
            meta = DataFileMeta.create(
                file_name=file_name,
                file_size=self.file_io.get_file_size(file_path),
                row_count=row_count,
                min_key=GenericRow([], []), max_key=GenericRow([], []),
                key_stats=SimpleStats.empty_stats(),
                value_stats=self._collect_value_stats(None, fields, stats),
                min_sequence_number=0, max_sequence_number=0,
                schema_id=self.table.table_schema.id, level=0,
                extra_files=[], delete_row_count=0, file_source=0,
                value_stats_cols=None if fields else [],
                external_path=file_path if self.external_path_provider else None,
                write_cols=self.write_cols, file_path=file_path,
            )
            self.committed_files.append(meta)
            return [meta]
        except Exception:
            self.file_io.delete_quietly(file_path)
            raise
