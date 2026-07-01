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

import unittest

import pyarrow as pa

from pypaimon.deletionvectors.apply_deletion_vector_reader import (
    ApplyDeletionVectorReader,
    PositionMappedDeletionVector,
)
from pypaimon.deletionvectors.bitmap_deletion_vector import BitmapDeletionVector
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.read.reader.concat_batch_reader import (
    BlobFallbackBatchReader,
    DataEvolutionMergeReader,
)
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.split import DataSplit
from pypaimon.table.row.blob import Blob, BlobData
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.source.deletion_file import DeletionFile
from pypaimon.utils.range import Range
from pypaimon.utils.data_evolution_utils import retrieve_anchor_file


class _OneBatchReader(RecordBatchReader):
    def __init__(self, values):
        self._batch = pa.record_batch([pa.array(values, type=pa.int64())], names=["v"])
        self._returned = False

    def read_arrow_batch(self):
        if self._returned:
            return None
        self._returned = True
        return self._batch

    def close(self):
        pass


class _MultiBatchReader(RecordBatchReader):
    def __init__(self, value_batches, name="v"):
        self._batches = [
            pa.record_batch([pa.array(values, type=pa.int64())], names=[name])
            for values in value_batches
        ]
        self._next = 0

    def read_arrow_batch(self):
        if self._next >= len(self._batches):
            return None
        batch = self._batches[self._next]
        self._next += 1
        return batch

    def close(self):
        pass


class _BlobFallbackBatchReaderForTest(BlobFallbackBatchReader):
    def __init__(self, files, values_by_file_name, deletion_vector=None):
        super().__init__(
            [(file, lambda: None) for file in files],
            "blob_col",
            pa.binary(),
            row_ranges=None,
            blob_as_descriptor=False,
            deletion_vector=deletion_vector,
        )
        self._values_by_file_name = values_by_file_name

    def _read_blob_values(self, file, supplier):
        return self._values_by_file_name[file.file_name]


def _file(name, first_row_id, row_count, max_sequence_number):
    empty_row = GenericRow([], [])
    return DataFileMeta(
        file_name=name,
        file_size=1,
        row_count=row_count,
        min_key=empty_row,
        max_key=empty_row,
        key_stats=SimpleStats.empty_stats(),
        value_stats=SimpleStats.empty_stats(),
        min_sequence_number=max_sequence_number,
        max_sequence_number=max_sequence_number,
        schema_id=0,
        level=0,
        extra_files=[],
        first_row_id=first_row_id,
    )


class DataEvolutionDeletionVectorTest(unittest.TestCase):
    def test_retrieve_anchor_file_uses_oldest_normal_file(self):
        files = [
            _file("field-2.blob", 0, 5, 1),
            _file("normal-b.parquet", 0, 5, 1),
            _file("normal-a.parquet", 0, 5, 1),
            _file("newer.parquet", 0, 5, 2),
        ]

        self.assertEqual("normal-a.parquet", retrieve_anchor_file(files).file_name)

    def test_data_evolution_merged_row_count_subtracts_deletion_vectors(self):
        split = DataSplit(
            files=[
                _file("anchor-0.parquet", 0, 5, 1),
                _file("blob-0.blob", 0, 5, 2),
                _file("anchor-5.parquet", 5, 5, 3),
            ],
            partition=GenericRow([], []),
            bucket=0,
            raw_convertible=False,
            data_deletion_files=[
                DeletionFile("dv", 0, 1, cardinality=2),
                None,
                DeletionFile("dv", 1, 1, cardinality=1),
            ],
        )

        self.assertEqual(7, split.merged_row_count())

    def test_data_evolution_merged_row_count_unknown_without_cardinality(self):
        split = DataSplit(
            files=[_file("anchor.parquet", 0, 5, 1)],
            partition=GenericRow([], []),
            bucket=0,
            raw_convertible=False,
            data_deletion_files=[DeletionFile("dv", 0, 1, cardinality=None)],
        )

        self.assertIsNone(split.merged_row_count())

    def test_apply_deletion_vector_reader_uses_mapped_deletion_vector(self):
        deletion_vector = BitmapDeletionVector()
        deletion_vector.delete(12)
        mapped_dv = PositionMappedDeletionVector(
            deletion_vector,
            file_offset=10,
            row_positions=[0, 2, 4],
        )

        reader = ApplyDeletionVectorReader(
            _OneBatchReader([0, 2, 4]),
            mapped_dv,
        )

        batch = reader.read_arrow_batch()
        self.assertEqual([0, 4], batch.column(0).to_pylist())
        self.assertTrue(reader.deletion_vector().is_deleted(1))
        self.assertFalse(reader.deletion_vector().is_deleted(2))

    def test_apply_deletion_vector_reader_skips_empty_filtered_batches(self):
        deletion_vector = BitmapDeletionVector()
        deletion_vector.delete(0)
        deletion_vector.delete(1)

        reader = DataEvolutionMergeReader(
            row_offsets=[0, 1],
            field_offsets=[0, 0],
            readers=[
                ApplyDeletionVectorReader(
                    _MultiBatchReader([[0, 1], [2, 3]], "v"),
                    deletion_vector,
                ),
                ApplyDeletionVectorReader(
                    _MultiBatchReader([[10, 11], [12, 13]], "w"),
                    deletion_vector,
                ),
            ],
            schema=pa.schema([pa.field("v", pa.int64()), pa.field("w", pa.int64())]),
        )

        batch = reader.read_arrow_batch()
        self.assertEqual([2, 3], batch.column(0).to_pylist())
        self.assertEqual([12, 13], batch.column(1).to_pylist())
        self.assertIsNone(reader.read_arrow_batch())

    def test_blob_fallback_batch_reader_applies_deletion_vector(self):
        files = [
            _file("blob-old.blob", 0, 5, 1),
            _file("blob-new.blob", 0, 5, 2),
        ]
        deletion_vector = BitmapDeletionVector()
        deletion_vector.delete(1)
        deletion_vector.delete(4)

        reader = _BlobFallbackBatchReaderForTest(
            files,
            {
                "blob-old.blob": [
                    BlobData(b"old-0"),
                    BlobData(b"old-1"),
                    BlobData(b"old-2"),
                    BlobData(b"old-3"),
                    BlobData(b"old-4"),
                ],
                "blob-new.blob": [
                    Blob.PLACE_HOLDER,
                    BlobData(b"new-1"),
                    BlobData(b"new-2"),
                    Blob.PLACE_HOLDER,
                    BlobData(b"new-4"),
                ],
            },
            deletion_vector=(Range(0, 4), deletion_vector),
        )

        batch = reader.read_arrow_batch()
        self.assertEqual(
            [b"old-0", b"new-2", b"old-3"],
            batch.column(0).to_pylist(),
        )
        self.assertIsNone(reader.read_arrow_batch())


if __name__ == "__main__":
    unittest.main()
