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

import os
import unittest
from datetime import datetime

import pandas as pd
import pyarrow as pa
from pypaimon.catalog.catalog_factory import CatalogFactory
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.schema.schema import Schema


class JavaPyReadWriteTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tempdir = os.path.abspath(".")
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({
            'warehouse': cls.warehouse
        })
        cls.catalog.create_database('default', True)

    def test_py_write_read(self):
        test_timestamp = datetime(2024, 1, 15, 10, 30, 0)

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('name', pa.string()),
            ('category', pa.string()),
            ('value', pa.float64()),
            ('ts', pa.timestamp('ms'))  # Add timestamp field
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema,
            partition_keys=['category'],
            options={'dynamic-partition-overwrite': 'false'}
        )

        # Clean up table if it exists
        try:
            existing_table = self.catalog.get_table('default.mixed_test_tablep')
            table_path = self.catalog.get_table_path(existing_table.identifier)
            if self.catalog.file_io.exists(table_path):
                self.catalog.file_io.delete(table_path, recursive=True)
        except Exception:
            pass

        self.catalog.create_table('default.mixed_test_tablep', schema, False)
        table = self.catalog.get_table('default.mixed_test_tablep')

        initial_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5, 6],
            'name': ['Apple', 'Banana', 'Carrot', 'Broccoli', 'Chicken', 'Beef'],
            'category': ['Fruit', 'Fruit', 'Vegetable', 'Vegetable', 'Meat', 'Meat'],
            'value': [1.5, 0.8, 0.6, 1.2, 5.0, 8.0],
            'ts': [test_timestamp] * 6  # All rows have the same timestamp
        })
        # Write initial data
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()

        table_write.write_pandas(initial_data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        # Verify initial data
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        initial_result = table_read.to_pandas(table_scan.plan().splits())
        print(initial_result)
        self.assertEqual(len(initial_result), 6)
        self.assertListEqual(
            initial_result['name'].tolist(),
            ['Apple', 'Banana', 'Carrot', 'Broccoli', 'Chicken', 'Beef']
        )

        read_timestamps = initial_result['ts'].tolist()
        for read_ts in read_timestamps:
            if read_ts.tzinfo is not None:
                read_ts = read_ts.replace(tzinfo=None)
            self.assertEqual(
                read_ts.replace(microsecond=0),  # Ignore microseconds for comparison
                test_timestamp.replace(microsecond=0),
                f"Timestamp mismatch: expected {test_timestamp}, got {read_ts}. "
                f"This indicates a timezone conversion bug."
            )

        manifest_file_manager = ManifestFileManager(table)
        manifest_list_manager = ManifestListManager(table)
        snapshot_manager = table.snapshot_manager()
        current_snapshot = snapshot_manager.get_latest_snapshot()
        if current_snapshot:
            manifest_files = manifest_list_manager.read_all(current_snapshot)
            for manifest_file_meta in manifest_files:
                entries = manifest_file_manager.read(manifest_file_meta.file_name)
                for entry in entries:
                    if entry.file.get_creation_time() is not None:
                        creation_time_ts = entry.file.get_creation_time()
                        creation_time_dt = creation_time_ts.to_local_date_time()
                        self.assertIsNone(
                            creation_time_dt.tzinfo,
                            "Manifest file creation_time should be naive datetime (no timezone)"
                        )
                        write_time = datetime.now()
                        time_diff = abs((creation_time_dt - write_time).total_seconds())
                        self.assertLess(
                            time_diff, 60,
                            f"Manifest file creation_time {creation_time_dt} is too far from "
                            f"write time {write_time}. This indicates a timezone conversion bug "
                            f"(likely 8-hour offset)."
                        )

    def test_read(self):
        table = self.catalog.get_table('default.mixed_test_tablej')
        read_builder = table.new_read_builder()
        table_scan = read_builder.new_scan()
        table_read = read_builder.new_read()
        res = table_read.to_pandas(table_scan.plan().splits())
        print(res)
