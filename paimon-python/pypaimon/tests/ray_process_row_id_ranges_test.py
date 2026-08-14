# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import shutil
import tempfile
import unittest
import uuid
from unittest import mock

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.ray import process_row_id_ranges
from pypaimon.utils.range import Range


class ProcessRowIdRangesTest(unittest.TestCase):

    schema = pa.schema([
        ("id", pa.int32()),
        ("name", pa.string()),
    ])

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog_options = {
            "warehouse": os.path.join(cls.tempdir, "warehouse")
        }
        cls.catalog = CatalogFactory.create(cls.catalog_options)
        cls.catalog.create_database("default", True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create(self, options=None):
        target = "default.ranges_{}".format(uuid.uuid4().hex[:8])
        self.catalog.create_table(
            target,
            Schema.from_pyarrow_schema(self.schema, options=options or {}),
            False,
        )
        return target

    def _write(self, target, row_count):
        table = self.catalog.get_table(target)
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        writer.write_arrow(pa.Table.from_pydict({
            "id": list(range(row_count)),
            "name": ["n{}".format(i) for i in range(row_count)],
        }, schema=self.schema))
        commit = builder.new_commit()
        commit.commit(writer.prepare_commit())
        writer.close()
        commit.close()

    def test_processes_file_groups_in_target_sized_sequential_batches(self):
        target = self._create({
            "row-tracking.enabled": "true",
            "data-evolution.enabled": "true",
            "target-file-row-num": "3",
        })
        self._write(target, 10)

        # Add an overlapping data-evolution file. It belongs to the first
        # logical file group and must not be counted as another range.
        table = self.catalog.get_table(target)
        builder = table.new_batch_write_builder()
        messages = (
            builder.new_update()
            .with_update_type(["name"])
            .update_by_arrow_with_row_id(pa.table({
                "_ROW_ID": pa.array([1], type=pa.int64()),
                "name": pa.array(["updated"], type=pa.string()),
            }))
        )
        commit = builder.new_commit()
        commit.commit(messages)
        commit.close()

        batches = []
        process_row_id_ranges(
            target,
            self.catalog_options,
            rows_per_commit=7,
            processor=batches.append,
        )

        self.assertEqual([
            [Range(0, 2), Range(3, 5), Range(6, 8)],
            [Range(9, 9)],
        ], batches)

    def test_processor_failure_stops_later_batches(self):
        target = self._create({
            "row-tracking.enabled": "true",
            "data-evolution.enabled": "true",
            "target-file-row-num": "2",
        })
        self._write(target, 6)
        calls = []

        def processor(ranges):
            calls.append(ranges)
            if len(calls) == 2:
                raise RuntimeError("processor failed")

        with self.assertRaisesRegex(RuntimeError, "processor failed"):
            process_row_id_ranges(
                target,
                self.catalog_options,
                rows_per_commit=2,
                processor=processor,
            )

        self.assertEqual([[Range(0, 1)], [Range(2, 3)]], calls)

    def test_empty_table_does_not_call_processor(self):
        target = self._create({"row-tracking.enabled": "true"})
        processor = mock.Mock()

        process_row_id_ranges(
            target,
            self.catalog_options,
            rows_per_commit=10,
            processor=processor,
        )

        processor.assert_not_called()

    def test_requires_row_tracking(self):
        target = self._create()
        with self.assertRaisesRegex(ValueError, "row-tracking.enabled"):
            process_row_id_ranges(
                target,
                self.catalog_options,
                rows_per_commit=10,
                processor=lambda ranges: None,
            )

    def test_validates_arguments_before_loading_table(self):
        invalid_values = [True, False, 0, -1, 1.5, "1", None]
        for value in invalid_values:
            with self.subTest(rows_per_commit=value), mock.patch(
                "pypaimon.catalog.catalog_factory.CatalogFactory.create"
            ) as create:
                with self.assertRaisesRegex(ValueError, "positive integer"):
                    process_row_id_ranges(
                        "default.missing",
                        {},
                        rows_per_commit=value,
                        processor=lambda ranges: None,
                    )
                create.assert_not_called()

        with mock.patch(
            "pypaimon.catalog.catalog_factory.CatalogFactory.create"
        ) as create:
            with self.assertRaisesRegex(
                ValueError, "processor must be callable"
            ):
                process_row_id_ranges(
                    "default.missing",
                    {},
                    rows_per_commit=1,
                    processor=None,
                )
            create.assert_not_called()


if __name__ == "__main__":
    unittest.main()
