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

from pypaimon.compact.rewriter.rewriter import CompactRewriter
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.split_read import format_identifier
from pypaimon.table.bucket_mode import BucketMode
from pypaimon.write.writer.append_only_data_writer import AppendOnlyDataWriter


class AppendCompactRewriter(CompactRewriter):
    """Reads input append-only files and re-writes them via the table's normal
    AppendOnlyDataWriter, leveraging its built-in target_file_size rolling so
    output files are sized consistently with what the table would produce on a
    fresh INSERT.

    Notes on sequence numbers: append-only writers do not advance the per-row
    sequence generator (only PK writers do). To stay consistent with the
    write path (FileStoreWrite._create_data_writer), we seed the generator
    with 0 for BUCKET_UNAWARE tables and with max(input.max_sequence_number)
    for HASH_FIXED tables.

    NOTE (deferred): schema evolution across input files is not handled — we
    feed batches at their on-disk schema directly into the writer. Inputs
    spanning a schema change will be addressed alongside the broader schema
    evolution support in a later phase.
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

        max_seq = self._initial_max_seq(files)
        writer = AppendOnlyDataWriter(
            table=self.table,
            partition=partition,
            bucket=bucket,
            max_seq_number=max_seq,
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

        for f in new_files:
            # Mark provenance — Java's CompactWriter also tags compact output.
            f.file_source = 1  # 1 = COMPACT in pypaimon DataFileMeta convention

        return new_files

    def _initial_max_seq(self, files: List[DataFileMeta]) -> int:
        """Pick the writer's seed sequence number consistent with the write path.

        Mirrors FileStoreWrite._create_data_writer: BUCKET_UNAWARE always seeds
        with 0; bucketed append seeds with max(input.max_seq) so subsequent
        writes / reads observe a monotonically advancing seq.
        """
        if self.table.bucket_mode() == BucketMode.BUCKET_UNAWARE:
            return 0
        return max(f.max_sequence_number for f in files)

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
