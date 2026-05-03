################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.write.writer.data_writer import DataWriter

# DataFileMeta.file_source convention used across pypaimon's compaction path.
FILE_SOURCE_COMPACT = 1


class MergeTreeRollingWriter(DataWriter):
    """Writer for compaction output of primary-key (merge-tree) tables.

    Differences from KeyValueDataWriter:
    - Input batches are assumed to already carry the KV system fields
      (_KEY_*, _SEQUENCE_NUMBER, _VALUE_KIND) and to be sorted by
      (key ASC, sequence ASC). The writer never adds system fields itself
      and never advances the sequence generator.
    - After the parent class writes a file at level 0, we rewrite the just-
      appended DataFileMeta with the strategy-chosen output_level, the actual
      min/max sequence numbers observed in the data, the count of retract
      rows, and file_source=COMPACT.
    """

    def __init__(self, table, partition, bucket, output_level: int, options=None):
        super().__init__(
            table=table,
            partition=partition,
            bucket=bucket,
            max_seq_number=0,  # generator is not used for KV compaction output
            options=options if options is not None else table.options,
            write_cols=None,
        )
        self.output_level = output_level

    def _process_data(self, data: pa.RecordBatch) -> pa.Table:
        # Already enriched + sorted by upstream SortMergeReader/MergeFunction.
        return pa.Table.from_batches([data])

    def _merge_data(self, existing_data: pa.Table, new_data: pa.Table) -> pa.Table:
        # Both halves are already in (key, seq) order and the second half's
        # smallest key is guaranteed to be >= the first half's largest key
        # (caller is feeding a monotonic merge stream), so concat is enough.
        return pa.concat_tables([existing_data, new_data])

    def _write_data_to_file(self, data: pa.Table):
        if data.num_rows == 0:
            return

        # Snapshot the file count so we can find the entry the parent appends.
        before = len(self.committed_files)
        super()._write_data_to_file(data)
        if len(self.committed_files) <= before:
            return  # parent skipped (e.g. empty after processing)

        produced = self.committed_files[before:]
        # The parent always writes a single file per call, but be defensive.
        for idx, original in enumerate(produced):
            min_seq, max_seq = self._extract_seq_bounds(data)
            delete_count = self._count_retract_rows(data)
            self.committed_files[before + idx] = self._patch(
                original,
                self.output_level,
                min_seq,
                max_seq,
                delete_count,
            )

    @staticmethod
    def _extract_seq_bounds(data: pa.Table):
        seq = data.column("_SEQUENCE_NUMBER")
        return pc.min(seq).as_py(), pc.max(seq).as_py()

    @staticmethod
    def _count_retract_rows(data: pa.Table) -> int:
        # _VALUE_KIND == 0 → INSERT; anything else (UPDATE_AFTER/DELETE/...) is
        # treated as a retract for the purposes of the file-level counter.
        kind = data.column("_VALUE_KIND")
        return int(pc.sum(pc.cast(pc.not_equal(kind, 0), pa.int64())).as_py() or 0)

    @staticmethod
    def _patch(
        original: DataFileMeta,
        level: int,
        min_seq: int,
        max_seq: int,
        delete_count: int,
    ) -> DataFileMeta:
        return DataFileMeta(
            file_name=original.file_name,
            file_size=original.file_size,
            row_count=original.row_count,
            min_key=original.min_key,
            max_key=original.max_key,
            key_stats=original.key_stats,
            value_stats=original.value_stats,
            min_sequence_number=min_seq,
            max_sequence_number=max_seq,
            schema_id=original.schema_id,
            level=level,
            extra_files=original.extra_files,
            creation_time=original.creation_time,
            delete_row_count=delete_count,
            embedded_index=original.embedded_index,
            file_source=FILE_SOURCE_COMPACT,
            value_stats_cols=original.value_stats_cols,
            external_path=original.external_path,
            first_row_id=original.first_row_id,
            write_cols=original.write_cols,
            file_path=original.file_path,
        )
