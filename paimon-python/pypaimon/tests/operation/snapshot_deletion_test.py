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

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


class SnapshotDeletionIntegrationTest(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp()
        self.warehouse = os.path.join(self.tempdir, "warehouse")
        self.catalog = CatalogFactory.create({"warehouse": self.warehouse})
        self.catalog.create_database("default", True)
        self.pa_schema = pa.schema([
            ("id", pa.int32()),
            ("name", pa.string()),
            ("dt", pa.string()),
        ])

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def _write_data(self, table, ids, names, dts):
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data = pa.Table.from_pydict(
            {"id": ids, "name": names, "dt": dts},
            schema=self.pa_schema,
        )
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

    def test_clean_unused_data_files_basic(self):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema, partition_keys=["dt"], options={"bucket": "-1"},
        )
        self.catalog.create_table("default.del_test", schema, False)
        table = self.catalog.get_table("default.del_test")
        self._write_data(table, [1], ["a"], ["p1"])
        self._write_data(table, [2], ["b"], ["p1"])

        from pypaimon.operation.snapshot_deletion import SnapshotDeletion
        deletion = SnapshotDeletion(table)
        snapshot1 = table.snapshot_manager().get_snapshot_by_id(1)
        deletion.clean_unused_data_files(snapshot1, skipper=lambda entry: False)
        deletion.clean_empty_directories()

    def test_clean_unused_manifests(self):
        schema = Schema.from_pyarrow_schema(
            self.pa_schema, partition_keys=["dt"], options={"bucket": "-1"},
        )
        self.catalog.create_table("default.manifest_test", schema, False)
        table = self.catalog.get_table("default.manifest_test")
        self._write_data(table, [1], ["a"], ["p1"])
        self._write_data(table, [2], ["b"], ["p1"])

        from pypaimon.operation.snapshot_deletion import SnapshotDeletion
        deletion = SnapshotDeletion(table)
        snapshot2 = table.snapshot_manager().get_snapshot_by_id(2)
        skipping_set = deletion.manifest_skipping_set([snapshot2])
        snapshot1 = table.snapshot_manager().get_snapshot_by_id(1)
        deletion.clean_unused_manifests(snapshot1, skipping_set)

if __name__ == '__main__':
    unittest.main()
