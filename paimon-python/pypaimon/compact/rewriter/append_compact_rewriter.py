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
from pypaimon.write.writer.append_only_data_writer import AppendOnlyDataWriter


class AppendCompactRewriter(CompactRewriter):
    """Reads input append-only files and re-writes them via the table's normal
    AppendOnlyDataWriter, leveraging its built-in target_file_size rolling so
    output files are sized consistently with what the table would produce on a
    fresh INSERT.

    Notes on sequence numbers: append-only writers do not advance the per-row
    sequence generator (only PK writers do), so input/output min_seq == max_seq.
    To keep the new files >= every consumed file, we seed the generator with
    max(input.max_sequence_number).
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

        # Materialize the file paths the writer's downstream readers expect.
        # The coordinator hands us DataFileMeta straight off the manifest, so
        # file_path is None until we resolve it against the table layout.
        path_factory = self.table.path_factory()
        bucket_path = path_factory.bucket_path(partition, bucket).rstrip("/")
        for f in files:
            if f.file_path is None:
                f.file_path = f"{bucket_path}/{f.file_name}"

        max_seq = max(f.max_sequence_number for f in files)
        writer = AppendOnlyDataWriter(
            table=self.table,
            partition=partition,
            bucket=bucket,
            max_seq_number=max_seq,
            options=self.table.options,
            write_cols=None,
        )
        try:
            for batch in self._read_input_batches(files):
                if batch.num_rows > 0:
                    writer.write(batch)
            new_files = writer.prepare_commit()
        finally:
            writer.close()

        for f in new_files:
            # Mark provenance — Java's CompactWriter also tags compact output.
            f.file_source = 1  # 1 = COMPACT in pypaimon DataFileMeta convention

        return new_files

    def _read_input_batches(self, files: List[DataFileMeta]) -> Iterator[pa.RecordBatch]:
        """Stream record batches from each input file in order.

        We feed the writer batch-by-batch (rather than loading every file into
        memory first) so the AppendOnlyDataWriter's target_file_size rolling
        kicks in even for large inputs.
        """
        for f in files:
            file_format = format_identifier(f.file_name)
            file_path_for_pyarrow = self.table.file_io.to_filesystem_path(f.file_path)
            dataset = ds.dataset(
                file_path_for_pyarrow,
                format=file_format,
                filesystem=self.table.file_io.filesystem,
            )
            for batch in dataset.to_batches():
                yield batch
