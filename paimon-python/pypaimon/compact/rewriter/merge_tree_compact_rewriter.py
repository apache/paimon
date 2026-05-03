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

"""Primary-key (merge-tree) compaction rewriter.

Reads each section of the input plan via SortMergeReader, applies the
table's MergeFunction (Deduplicate by default), optionally drops retract
rows, and writes the merged stream out via MergeTreeRollingWriter so the
target_file_size rolling stays consistent with the regular write path.

Sections are produced by IntervalPartition before reaching us — that's the
existing utility used by MergeFileSplitRead, so we get identical "key
intervals don't overlap inside a sorted run" guarantees here.
"""

from functools import partial
from typing import Callable, List, Optional

import pyarrow as pa

from pypaimon.compact.rewriter.merge_tree_rolling_writer import \
    MergeTreeRollingWriter
from pypaimon.compact.rewriter.rewriter import CompactRewriter
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.interval_partition import SortedRun
from pypaimon.read.reader.concat_record_reader import ConcatRecordReader
from pypaimon.read.reader.drop_delete_reader import DropDeleteRecordReader
from pypaimon.read.reader.format_avro_reader import FormatAvroReader
from pypaimon.read.reader.format_pyarrow_reader import FormatPyArrowReader
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.read.reader.key_value_wrap_reader import KeyValueWrapReader
from pypaimon.read.reader.merge_function import MergeFunctionFactory
from pypaimon.read.reader.sort_merge_reader import SortMergeReaderWithMinHeap
from pypaimon.read.split_read import (KEY_PREFIX, build_kv_file_fields,
                                      format_identifier)
from pypaimon.schema.data_types import DataField

# Buffer KVs from the merge stream this many at a time before handing the
# resulting RecordBatch to the writer. Sized to amortize per-row Python
# overhead without ballooning peak memory for wide PK rows.
DEFAULT_BUFFER_ROWS = 4096


class MergeTreeCompactRewriter(CompactRewriter):

    def __init__(
        self,
        table,
        mf_factory: MergeFunctionFactory,
        buffer_rows: int = DEFAULT_BUFFER_ROWS,
    ):
        self.table = table
        self.mf_factory = mf_factory
        self.buffer_rows = buffer_rows

        # Schema of the on-disk KV file: [_KEY_pk, _SEQUENCE_NUMBER,
        # _VALUE_KIND, value_cols...]. Computed once per rewriter to avoid
        # repeated per-section work.
        self._kv_fields: List[DataField] = self._build_kv_fields()
        self._kv_field_names: List[str] = [f.name for f in self._kv_fields]
        self._key_arity = sum(
            1 for f in self._kv_fields if f.name.startswith(KEY_PREFIX)
        )
        self._value_arity = (
            len(self._kv_fields) - self._key_arity - 2  # minus seq + kind
        )
        self._arrow_schema = self._build_arrow_schema()

    def rewrite(
        self,
        partition,
        bucket: int,
        output_level: int,
        sections: List[List[SortedRun]],
        drop_delete: bool,
    ) -> List[DataFileMeta]:
        if not sections:
            return []

        writer = MergeTreeRollingWriter(
            table=self.table,
            partition=partition,
            bucket=bucket,
            output_level=output_level,
            options=self.table.options,
        )

        try:
            try:
                for section in sections:
                    self._consume_section(section, drop_delete, writer)
                files = writer.prepare_commit()
            except Exception:
                writer.abort()
                raise
        finally:
            writer.close()

        return files

    # ---- internals ---------------------------------------------------------

    def _consume_section(
        self,
        section: List[SortedRun],
        drop_delete: bool,
        writer: MergeTreeRollingWriter,
    ) -> None:
        # Each rewrite() call already knows its (partition, bucket); compute
        # the bucket directory once here so each file's read_path is a cheap
        # string concat instead of repeating path-factory work per file.
        partition = writer.partition
        bucket_path = self.table.path_factory().bucket_path(partition, writer.bucket).rstrip("/")

        readers: List[RecordReader] = []
        for sorted_run in section:
            suppliers: List[Callable[[], RecordReader]] = []
            for f in sorted_run.files:
                suppliers.append(partial(self._kv_reader_supplier, f, bucket_path))
            readers.append(ConcatRecordReader(suppliers))

        merge_reader: RecordReader = SortMergeReaderWithMinHeap(
            readers=readers,
            schema=self.table.table_schema,
            merge_function=self.mf_factory.create(),
        )
        if drop_delete:
            merge_reader = DropDeleteRecordReader(merge_reader)

        try:
            self._stream_to_writer(merge_reader, writer)
        finally:
            merge_reader.close()

    def _stream_to_writer(
        self,
        merge_reader: RecordReader,
        writer: MergeTreeRollingWriter,
    ) -> None:
        buffer: List[tuple] = []
        while True:
            iterator = merge_reader.read_batch()
            if iterator is None:
                break
            while True:
                kv = iterator.next()
                if kv is None:
                    break
                # Snapshot the row tuple — KeyValue is reused across calls.
                buffer.append(tuple(kv.row_tuple))
                if len(buffer) >= self.buffer_rows:
                    writer.write(self._tuples_to_batch(buffer))
                    buffer.clear()
        if buffer:
            writer.write(self._tuples_to_batch(buffer))
            buffer.clear()

    def _tuples_to_batch(self, tuples: List[tuple]) -> pa.RecordBatch:
        # Transpose to columnar form, then build the RecordBatch with the
        # KV-file schema we precomputed at __init__.
        columns = list(zip(*tuples)) if tuples else [() for _ in self._kv_field_names]
        arrays = [
            pa.array(list(col), type=self._arrow_schema.field(i).type)
            for i, col in enumerate(columns)
        ]
        return pa.RecordBatch.from_arrays(arrays, schema=self._arrow_schema)

    def _kv_reader_supplier(self, file: DataFileMeta, bucket_path: str) -> RecordReader:
        # Resolve read path locally (preferring external_path, matching
        # SplitRead.file_reader_supplier) without mutating the manifest meta.
        read_path = (
            file.external_path
            if file.external_path
            else (file.file_path if file.file_path else f"{bucket_path}/{file.file_name}")
        )
        file_format = format_identifier(file.file_name)
        file_io = self.table.file_io
        if file_format == "avro":
            file_batch_reader = FormatAvroReader(
                file_io, read_path, self._kv_fields, push_down_predicate=None,
            )
        else:
            file_batch_reader = FormatPyArrowReader(
                file_io,
                file_format,
                read_path,
                read_fields=self._kv_fields,
                push_down_predicate=None,
                options=self.table.options,
            )
        return KeyValueWrapReader(file_batch_reader, self._key_arity, self._value_arity)

    def _build_kv_fields(self) -> List[DataField]:
        # Same helper SplitRead._create_key_value_fields uses, so the on-disk
        # KV file schema cannot drift between read and compact paths.
        return build_kv_file_fields(
            table_fields=self.table.fields,
            trimmed_primary_keys=self.table.trimmed_primary_keys,
            value_fields=self.table.fields,
        )

    def _build_arrow_schema(self) -> pa.Schema:
        from pypaimon.schema.data_types import PyarrowFieldParser
        return PyarrowFieldParser.from_paimon_schema(self._kv_fields)
