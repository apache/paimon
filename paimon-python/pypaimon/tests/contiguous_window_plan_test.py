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

import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pytest

import pypaimon.multimodal as pmm
from pypaimon import Schema
from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.multimodal.query import ScanQuery
from pypaimon.multimodal.table import MultimodalTable
from pypaimon.read.native_plan import native_reader_available
from pypaimon.read.reader.format_pyarrow_reader import FormatPyArrowReader
from pypaimon.read.table_scan import TableScan


class ContiguousWindowPlanTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="pypaimon_window_plan_")
        self.conn = pmm.connect(options={
            "warehouse": os.path.join(self.temp_dir, "warehouse"),
        })
        self.schema = pa.schema([
            pa.field("episode", pa.string(), nullable=False),
            pa.field("step", pa.int32(), nullable=False),
            pa.field("value", pa.int32()),
        ])

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _table(self, partitioned=None):
        return self.conn.create_table(
            "frames", schema=self.schema, partitioned=partitioned, options={
                "row-tracking.enabled": "true",
                "data-evolution.enabled": "true",
                "deletion-vectors.enabled": "true",
                "file.format": "parquet",
            })

    @staticmethod
    def _rows(episode, steps):
        return [
            {"episode": episode, "step": step, "value": step + 10}
            for step in steps
        ]

    @staticmethod
    def _windows(query):
        return query.to_contiguous_window_dataset(
            window_size=2, columns=["value"],
            group_key="episode", order_key="step")

    @pytest.mark.python_plan
    @pytest.mark.python_read
    def test_task_query_keeps_its_pruned_files_for_window_reads(self):
        table = self._table(partitioned=["episode"])
        table.add(self._rows("wanted", range(4)))
        table.add(self._rows("wanted", range(4, 8)))
        table.add(self._rows("unrelated", range(8)))
        plans = []
        opened = []
        original_plan = TableScan.plan
        original_reader = FormatPyArrowReader.__init__

        def record_plan(scan):
            plan = original_plan(scan)
            plans.append(plan)
            return plan

        def record_reader(reader, *args, **kwargs):
            opened.append(args[2])
            original_reader(reader, *args, **kwargs)

        with patch.object(TableScan, "plan", record_plan), \
                patch.object(FormatPyArrowReader, "__init__", record_reader):
            dataset = self._windows(table.scan().where(
                "episode = 'wanted' AND step >= 4"))
            samples = dataset.__getitems__(list(range(len(dataset))))

        self.assertEqual([[14, 15], [15, 16], [16, 17]], [
            sample["value"] for sample in samples])
        # A deferred unfiltered payload plan would visit the other task too.
        self.assertEqual(1, len(plans))
        self.assertEqual([("wanted",)], [
            tuple(split.partition.values) for split in plans[0].splits()])
        planned_files = {
            file.file_name for split in plans[0].splits() for file in split.files
        }
        # File statistics may prune the earlier file in the wanted partition.
        self.assertEqual(planned_files, {os.path.basename(path) for path in opened})

    @pytest.mark.python_plan
    @pytest.mark.python_read
    def test_payload_authorization_controls_index_membership_and_masking(self):
        table = self._table()
        table.add(self._rows("denied", range(4)))
        table.add(self._rows("allowed", range(4)))
        auth = TableQueryAuthResult(
            filter=[json.dumps({
                "kind": "LEAF",
                "transform": {
                    "name": "FIELD_REF",
                    "fieldRef": {"name": "episode", "type": "STRING NOT NULL"},
                },
                "function": "EQUAL",
                "literals": ["allowed"],
            })],
            column_masking={"value": json.dumps({"name": "NULL"})},
        )

        def authorize(select):
            return auth if "value" in select else None

        table.raw_table.catalog_environment.table_query_auth = (
            lambda options, identifier: authorize)
        dataset = self._windows(table.scan())
        samples = dataset.__getitems__(list(range(len(dataset))))

        # An index-only plan misses both rules; losing its wrapper leaks values.
        self.assertEqual(3, len(samples))
        self.assertEqual(["allowed"] * 3, [sample["episode"] for sample in samples])
        self.assertEqual([0, 1, 2], [sample["step"] for sample in samples])
        self.assertEqual([[None, None]] * 3, [sample["value"] for sample in samples])

    @pytest.mark.python_plan
    @pytest.mark.python_read
    def test_independent_payload_column_files_are_read(self):
        self._check_independent_column_files(native=False)

    @unittest.skipUnless(native_reader_available(),
                         "pypaimon-rust native reader API not installed")
    def test_native_plan_retains_independent_payload_column_files(self):
        self._check_independent_column_files(native=True)

    def _check_independent_column_files(self, native):
        table = self._table()
        builder = table.raw_table.new_batch_write_builder()
        keys = builder.new_write().with_write_type(["episode", "step"])
        values = builder.new_write().with_write_type(["value"])
        commit = builder.new_commit()
        try:
            keys.write_arrow(pa.Table.from_pylist(
                self._rows("episode", range(4)),
                schema=pa.schema([self.schema.field("episode"), self.schema.field("step")]),
            ))
            values.write_arrow(pa.table(
                {"value": [100, 200, 300, 400]},
                schema=pa.schema([self.schema.field("value")]),
            ))
            messages = keys.prepare_commit() + values.prepare_commit()
            for message in messages:
                for file in message.new_files:
                    file.first_row_id = 0
            commit.commit(messages)
        finally:
            keys.close()
            values.close()
            commit.close()

        raw = table.raw_table.copy({
            "scan.native-plan.enabled": str(native).lower(),
            "read.native.enabled": str(native).lower(),
        })
        native_plans = []
        original_native_plan = TableScan._try_native_plan

        def record_native_plan(scan):
            plan = original_native_plan(scan)
            native_plans.append(plan)
            return plan

        with patch.object(TableScan, "_try_native_plan", record_native_plan):
            dataset = self._windows(ScanQuery(raw))
            samples = dataset.__getitems__([2, 0, 1])

        self.assertEqual([[300, 400], [100, 200], [200, 300]], [
            sample["value"] for sample in samples])
        if native:
            # Do not count a silent fallback as coverage of projection pruning.
            self.assertEqual(1, len(native_plans))
            self.assertIsNotNone(native_plans[0])

    def test_predicate_limit_and_deleted_rows_define_the_context(self):
        table = self._table()
        table.add(self._rows("episode", range(7)))
        table.delete(where="step = 1")

        dataset = self._windows(table.scan().where("value >= 11").limit(4))
        samples = dataset.__getitems__(list(range(len(dataset))))

        # The deleted first match must not consume the limit. Visible steps
        # are 2, 3, 4, 5; step 6 is outside the context despite sharing its file.
        self.assertEqual([2, 3, 4], [sample["step"] for sample in samples])
        self.assertEqual([[12, 13], [13, 14], [14, 15]], [
            sample["value"] for sample in samples])

    @pytest.mark.python_read
    def test_non_de_sparse_windows_keep_file_offsets(self):
        # The multimodal creation facade requires DE. CWD also accepts raw
        # row-tracked tables, so create this append-only fixture via catalog.
        catalog = self.conn.catalog
        catalog.create_database("default", True)
        identifier = "default.frames"
        catalog.create_table(identifier, Schema.from_pyarrow_schema(
            self.schema, options={
                "row-tracking.enabled": "true",
                "data-evolution.enabled": "false",
                "file.format": "parquet",
            }), False)
        table = MultimodalTable(catalog, identifier, catalog.get_table(identifier))
        for start in (0, 4, 8):
            table.add(self._rows("episode", range(start, start + 4)))
        dataset = self._windows(table.scan())

        samples = dataset.__getitems__([8, 2, 8])
        self.assertEqual([8, 2, 8], [sample["step"] for sample in samples])
        self.assertEqual([[18, 19], [12, 13], [18, 19]], [
            sample["value"] for sample in samples])
