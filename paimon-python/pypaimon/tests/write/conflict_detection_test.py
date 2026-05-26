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
from dataclasses import dataclass
from typing import List

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.commit.conflict_detection import (
    ConflictDetection,
    RowIdColumnConflictChecker,
)


def _make_file(file_name, row_count=100, first_row_id=None,
               schema_id=0, write_cols=None):
    return DataFileMeta(
        file_name=file_name,
        file_size=1024,
        row_count=row_count,
        min_key=None,
        max_key=None,
        key_stats=None,
        value_stats=None,
        min_sequence_number=0,
        max_sequence_number=0,
        schema_id=schema_id,
        level=0,
        extra_files=[],
        first_row_id=first_row_id,
        write_cols=write_cols,
    )


_EMPTY_PARTITION = GenericRow([], [])


def _make_entry(file_name, kind=0, bucket=0, first_row_id=None,
                row_count=100, write_cols=None, schema_id=0):
    return ManifestEntry(
        kind=kind,
        partition=_EMPTY_PARTITION,
        bucket=bucket,
        total_buckets=1,
        file=_make_file(file_name, row_count=row_count,
                        first_row_id=first_row_id, schema_id=schema_id,
                        write_cols=write_cols),
    )


@dataclass
class _FakeSchema:
    id: int
    fields: List[DataField]


class _FakeSchemaManager:

    def __init__(self, schemas=None):
        self._schemas = {}
        if schemas:
            for s in schemas:
                self._schemas[s.id] = s

    def get_schema(self, schema_id):
        return self._schemas.get(schema_id)


_DEFAULT_SCHEMA = _FakeSchema(
    id=0,
    fields=[
        DataField(1, "col_a", AtomicType("INT")),
        DataField(2, "col_b", AtomicType("STRING")),
        DataField(3, "col_c", AtomicType("BIGINT")),
    ],
)


class TestCheckRowIdExistence(unittest.TestCase):

    def _make_detection(self):
        return ConflictDetection(
            data_evolution_enabled=True,
            snapshot_manager=None,
            manifest_list_manager=None,
            table=None,
            commit_scanner=None,
        )

    def test_no_conflict_when_base_file_exists(self):
        detection = self._make_detection()
        base = [_make_entry("f1", kind=0, first_row_id=0, row_count=100)]
        delta = [_make_entry("p1", kind=0, first_row_id=0, row_count=100)]
        self.assertIsNone(
            detection.check_row_id_existence(base, delta, next_row_id=200))

    def test_conflict_when_base_file_removed(self):
        detection = self._make_detection()
        base = []
        delta = [_make_entry("p1", kind=0, first_row_id=0, row_count=100)]
        result = detection.check_row_id_existence(base, delta, next_row_id=200)
        self.assertIsNotNone(result)
        self.assertIn("Row ID existence conflict", str(result))

    def test_conflict_when_base_file_rewritten(self):
        detection = self._make_detection()
        base = [_make_entry("f2", kind=0, first_row_id=0, row_count=200)]
        delta = [_make_entry("p1", kind=0, first_row_id=0, row_count=100)]
        result = detection.check_row_id_existence(base, delta, next_row_id=200)
        self.assertIsNotNone(result)
        self.assertIn("Row ID existence conflict", str(result))

    def test_skip_newly_appended_files(self):
        detection = self._make_detection()
        base = []
        delta = [_make_entry("p1", kind=0, first_row_id=200, row_count=100)]
        self.assertIsNone(
            detection.check_row_id_existence(base, delta, next_row_id=200))

    def test_skip_when_no_pre_assigned_row_id(self):
        detection = self._make_detection()
        base = []
        delta = [_make_entry("f1", kind=0)]
        self.assertIsNone(
            detection.check_row_id_existence(base, delta, next_row_id=200))

    def test_skip_delete_entries(self):
        detection = self._make_detection()
        base = []
        delta = [_make_entry("f1", kind=1, first_row_id=0, row_count=100)]
        self.assertIsNone(
            detection.check_row_id_existence(base, delta, next_row_id=200))

    def test_skip_when_data_evolution_disabled(self):
        detection = ConflictDetection(
            data_evolution_enabled=False,
            snapshot_manager=None,
            manifest_list_manager=None,
            table=None,
            commit_scanner=None,
        )
        base = []
        delta = [_make_entry("p1", kind=0, first_row_id=0, row_count=100)]
        self.assertIsNone(
            detection.check_row_id_existence(base, delta, next_row_id=200))

    def test_skip_when_next_row_id_is_none(self):
        detection = self._make_detection()
        base = []
        delta = [_make_entry("p1", kind=0, first_row_id=0, row_count=100)]
        self.assertIsNone(
            detection.check_row_id_existence(base, delta, next_row_id=None))


class TestRowIdColumnConflictChecker(unittest.TestCase):

    def _make_checker(self, delta_files, schema=None):
        schema_mgr = _FakeSchemaManager([schema or _DEFAULT_SCHEMA])
        return RowIdColumnConflictChecker.from_data_files(schema_mgr, delta_files)

    def test_no_conflict_disjoint_rows(self):
        delta_files = [
            _make_file("d1", row_count=100, first_row_id=0, write_cols=["col_a"]),
        ]
        checker = self._make_checker(delta_files)
        committed = _make_file("c1", row_count=100, first_row_id=200,
                               write_cols=["col_a"])
        self.assertFalse(checker.conflicts_with(committed))

    def test_no_conflict_same_rows_different_columns(self):
        delta_files = [
            _make_file("d1", row_count=100, first_row_id=0, write_cols=["col_a"]),
        ]
        checker = self._make_checker(delta_files)
        committed = _make_file("c1", row_count=100, first_row_id=0,
                               write_cols=["col_b"])
        self.assertFalse(checker.conflicts_with(committed))

    def test_conflict_same_rows_same_columns(self):
        delta_files = [
            _make_file("d1", row_count=100, first_row_id=0, write_cols=["col_a"]),
        ]
        checker = self._make_checker(delta_files)
        committed = _make_file("c1", row_count=100, first_row_id=0,
                               write_cols=["col_a"])
        self.assertTrue(checker.conflicts_with(committed))

    def test_conflict_overlapping_rows_overlapping_columns(self):
        delta_files = [
            _make_file("d1", row_count=100, first_row_id=0,
                       write_cols=["col_a", "col_b"]),
        ]
        checker = self._make_checker(delta_files)
        committed = _make_file("c1", row_count=100, first_row_id=50,
                               write_cols=["col_b", "col_c"])
        self.assertTrue(checker.conflicts_with(committed))

    def test_conflict_null_write_cols_committed(self):
        """null write_cols means full-schema write — always conflicts on column dimension."""
        delta_files = [
            _make_file("d1", row_count=100, first_row_id=0, write_cols=["col_a"]),
        ]
        checker = self._make_checker(delta_files)
        committed = _make_file("c1", row_count=100, first_row_id=0,
                               write_cols=None)
        self.assertTrue(checker.conflicts_with(committed))

    def test_conflict_null_write_cols_delta(self):
        """null write_cols in delta means all columns are in the write range."""
        delta_files = [
            _make_file("d1", row_count=100, first_row_id=0, write_cols=None),
        ]
        checker = self._make_checker(delta_files)
        committed = _make_file("c1", row_count=100, first_row_id=0,
                               write_cols=["col_b"])
        self.assertTrue(checker.conflicts_with(committed))

    def test_no_conflict_committed_file_no_row_id(self):
        delta_files = [
            _make_file("d1", row_count=100, first_row_id=0, write_cols=["col_a"]),
        ]
        checker = self._make_checker(delta_files)
        committed = _make_file("c1", row_count=100, first_row_id=None,
                               write_cols=["col_a"])
        self.assertFalse(checker.conflicts_with(committed))

    def test_none_when_no_delta_files_with_row_id(self):
        delta_files = [
            _make_file("d1", row_count=100, first_row_id=None),
        ]
        schema_mgr = _FakeSchemaManager([_DEFAULT_SCHEMA])
        checker = RowIdColumnConflictChecker.from_data_files(schema_mgr, delta_files)
        self.assertIsNone(checker)

    def test_system_fields_skipped(self):
        """System fields like _ROW_ID should not count as column conflicts."""
        delta_files = [
            _make_file("d1", row_count=100, first_row_id=0,
                       write_cols=["_ROW_ID", "col_a"]),
        ]
        checker = self._make_checker(delta_files)
        committed = _make_file("c1", row_count=100, first_row_id=0,
                               write_cols=["_ROW_ID", "col_b"])
        self.assertFalse(checker.conflicts_with(committed))

    def test_cross_schema_field_id_resolution(self):
        """Fields with same ID but different names across schema versions should still match."""
        schema_v0 = _FakeSchema(
            id=0,
            fields=[
                DataField(1, "col_a", AtomicType("INT")),
                DataField(2, "col_b", AtomicType("STRING")),
            ],
        )
        schema_v1 = _FakeSchema(
            id=1,
            fields=[
                DataField(1, "col_a_renamed", AtomicType("INT")),
                DataField(2, "col_b", AtomicType("STRING")),
                DataField(3, "col_c", AtomicType("BIGINT")),
            ],
        )
        schema_mgr = _FakeSchemaManager([schema_v0, schema_v1])
        delta_files = [
            _make_file("d1", row_count=100, first_row_id=0,
                       schema_id=0, write_cols=["col_a"]),
        ]
        checker = RowIdColumnConflictChecker.from_data_files(schema_mgr, delta_files)
        committed_same_field = _make_file(
            "c1", row_count=100, first_row_id=0,
            schema_id=1, write_cols=["col_a_renamed"])
        self.assertTrue(checker.conflicts_with(committed_same_field))
        committed_diff_field = _make_file(
            "c2", row_count=100, first_row_id=0,
            schema_id=1, write_cols=["col_c"])
        self.assertFalse(checker.conflicts_with(committed_diff_field))


if __name__ == '__main__':
    unittest.main()
