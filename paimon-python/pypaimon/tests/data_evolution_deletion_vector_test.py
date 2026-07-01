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
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.split import DataSplit
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.source.deletion_file import DeletionFile
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


if __name__ == "__main__":
    unittest.main()
