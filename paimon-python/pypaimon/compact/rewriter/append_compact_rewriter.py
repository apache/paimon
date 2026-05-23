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

from typing import Iterator, List, Tuple

import pyarrow as pa
import pyarrow.dataset as ds

from pypaimon.compact.rewriter.merge_tree_rolling_writer import \
    FILE_SOURCE_COMPACT
from pypaimon.compact.rewriter.rewriter import CompactRewriter
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.split_read import format_identifier
from pypaimon.write.writer.append_only_data_writer import AppendOnlyDataWriter


class AppendCompactRollingWriter(AppendOnlyDataWriter):
    """AppendOnlyDataWriter variant that mirrors Java RowDataRollingFileWriter
    used by BaseAppendFileStoreWrite.compactRewrite.

    Two behavior tweaks vs. the regular append writer:

    1. Sequence number bookkeeping. The base AppendOnlyDataWriter never
       advances `sequence_generator.current`, which is fine for plain
       writes (where every committed file ends up with min_seq == max_seq
       == the batch's seed) but wrong for compaction output. Java's
       LongCounter is incremented per row written, so each rolled output
       file ends up with precisely [first_row_seq, last_row_seq] in its
       metadata. We replicate that here, treating the seed value as the
       seq for the first row written: file N covering R rows gets
       [next_seq, next_seq + R - 1] in its meta. The base writer's
       SequenceGenerator has an off-by-one quirk (current is "+1 after
       last assigned"), so we set up the generator each call to make
       super()._write_data_to_file emit the exact bounds we want without
       touching the parent class.

    2. Provenance. Java passes FileSource.COMPACT into the rewriter
       constructor. The base writer hardcodes file_source=APPEND in
       _write_data_to_file, so we patch the just-appended DataFileMeta
       afterwards (same shape MergeTreeRollingWriter uses on the PK side).
    """

    def _write_data_to_file(self, data: pa.Table) -> None:
        n = data.num_rows
        if n == 0:
            return
        # `start` is treated as "next-to-assign" seq, matching Java's
        # LongCounter semantics. The slice we're about to write covers
        # [seq_start, seq_end].
        seq_start = self.sequence_generator.start
        seq_end = seq_start + n - 1
        # Drive the parent's metadata accounting: parent reads
        #   min_seq = start, max_seq = current
        # then sets start = current. Putting current at seq_end yields
        # exactly the [seq_start, seq_end] range Java would have written.
        self.sequence_generator.current = seq_end

        before = len(self.committed_files)
        super()._write_data_to_file(data)
        # Advance both fields past this batch so the next slice starts at
        # seq_end + 1 (the parent only moved start to current, which is
        # also seq_end now — we want next_to_assign, not last_assigned).
        self.sequence_generator.start = seq_end + 1
        self.sequence_generator.current = seq_end + 1

        # Stamp provenance on whatever the parent appended (rolling may emit
        # multiple files in one super call in principle; loop defensively).
        for i in range(before, len(self.committed_files)):
            self.committed_files[i].file_source = FILE_SOURCE_COMPACT


class AppendCompactRewriter(CompactRewriter):
    """Reads input append-only files and re-writes them via a Java-aligned
    rolling writer. Direct port of org.apache.paimon.operation
    .BaseAppendFileStoreWrite.compactRewrite.

    Sequence numbers: the rolling writer's counter is seeded with
    files[0].min_sequence_number (matching Java
    `LongCounter(toCompact.get(0).minSequenceNumber())`) and bumped per
    row written. Output files therefore carry contiguous, non-overlapping
    seq ranges starting from that seed — exactly what Java produces.

    NOTE (deferred):
    - Schema evolution: input batches are read straight off disk via
      pyarrow.dataset, not through table-aware ReadForCompact. Inputs
      spanning a schema change need the read path to evolve them; that's
      part of the schema-evolution work scheduled later.
    - Deletion vectors: Java's compactRewrite accepts a dvFactory that
      applies per-file DV during read and persists the resulting index
      delta into CompactIncrement.{newIndexFiles, deletedIndexFiles}.
      Pypaimon's compaction path will plug DV in alongside the broader
      DV support phase.
    """

    def __init__(self, table):
        self.table = table

    def rewrite(
        self,
        partition: Tuple,
        bucket: int,
        files: List[DataFileMeta],
    ) -> List[DataFileMeta]:
        if not files:
            return []

        # Java seeds the rolling writer's counter from the first file in the
        # (size-sorted) input. files comes from AppendCompactCoordinator
        # which sorts by size ascending — same shape Java's pack() produces —
        # so files[0] is the smallest input file's min_seq.
        seed_seq = files[0].min_sequence_number

        writer = AppendCompactRollingWriter(
            table=self.table,
            partition=partition,
            bucket=bucket,
            max_seq_number=seed_seq,
            options=self.table.options,
            write_cols=None,
        )
        try:
            try:
                for batch in self._read_input_batches(partition, bucket, files):
                    if batch.num_rows > 0:
                        writer.write(batch)
                new_files = writer.prepare_commit()
            except Exception:
                # Roll back any rewriter output written so far so a failed task
                # doesn't leave orphan files in the warehouse.
                writer.abort()
                raise
        finally:
            writer.close()

        return new_files

    def _read_input_batches(
        self,
        partition: Tuple,
        bucket: int,
        files: List[DataFileMeta],
    ) -> Iterator[pa.RecordBatch]:
        """Stream record batches from each input file in order.

        We resolve each file's read path locally (preferring external_path,
        matching SplitRead.file_reader_supplier) instead of mutating the
        DataFileMeta returned by the manifest — those objects may be cached or
        shared with other readers.
        """
        path_factory = self.table.path_factory()
        bucket_path = path_factory.bucket_path(partition, bucket).rstrip("/")
        for f in files:
            read_path = f.external_path if f.external_path else (
                f.file_path if f.file_path else f"{bucket_path}/{f.file_name}"
            )
            file_format = format_identifier(f.file_name)
            file_path_for_pyarrow = self.table.file_io.to_filesystem_path(read_path)
            dataset = ds.dataset(
                file_path_for_pyarrow,
                format=file_format,
                filesystem=self.table.file_io.filesystem,
            )
            for batch in dataset.to_batches():
                yield batch
