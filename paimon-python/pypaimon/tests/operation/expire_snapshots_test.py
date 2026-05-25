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


class ExpireSnapshotsTest(unittest.TestCase):

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

    def _create_table(self, name, **opts):
        options = {"bucket": "-1"}
        options.update(opts)
        schema = Schema.from_pyarrow_schema(
            self.pa_schema, partition_keys=["dt"], options=options,
        )
        self.catalog.create_table(f"default.{name}", schema, False)
        return self.catalog.get_table(f"default.{name}")

    def _write(self, table, ids, names, dts):
        wb = table.new_batch_write_builder()
        tw = wb.new_write()
        tc = wb.new_commit()
        data = pa.Table.from_pydict(
            {"id": ids, "name": names, "dt": dts}, schema=self.pa_schema,
        )
        tw.write_arrow(data)
        tc.commit(tw.prepare_commit())
        tw.close()
        tc.close()

    def _read_ids(self, table):
        rb = table.new_read_builder()
        result = rb.new_read().to_arrow(rb.new_scan().plan().splits())
        return sorted(result.column("id").to_pylist())

    def test_expire_with_retain_max(self):
        table = self._create_table("expire_max")
        for i in range(5):
            self._write(table, [i], [f"n{i}"], ["p1"])

        sm = table.snapshot_manager()
        self.assertEqual(sm.latest_snapshot_id(), 5)
        self.assertEqual(sm.earliest_snapshot_id(), 1)

        from pypaimon.options.expire_config import ExpireConfig
        config = ExpireConfig(snapshot_retain_max=2, snapshot_retain_min=1)
        expired = table.new_expire_snapshots().config(config).expire()

        self.assertGreater(expired, 0)
        self.assertGreaterEqual(sm.earliest_snapshot_id(), 4)
        self.assertEqual(self._read_ids(table), [0, 1, 2, 3, 4])

    def test_expire_with_retain_min(self):
        table = self._create_table("expire_min")
        for i in range(3):
            self._write(table, [i], [f"n{i}"], ["p1"])

        from pypaimon.options.expire_config import ExpireConfig
        config = ExpireConfig(snapshot_retain_max=3, snapshot_retain_min=2)
        table.new_expire_snapshots().config(config).expire()

        sm = table.snapshot_manager()
        remaining = sm.latest_snapshot_id() - sm.earliest_snapshot_id() + 1
        self.assertGreaterEqual(remaining, 2)

    def test_expire_protects_tagged_snapshot_files(self):
        table = self._create_table("expire_tag")
        self._write(table, [1], ["a"], ["p1"])
        table.create_tag("v1", snapshot_id=1)
        self._write(table, [2], ["b"], ["p1"])
        self._write(table, [3], ["c"], ["p1"])

        from pypaimon.options.expire_config import ExpireConfig
        config = ExpireConfig(snapshot_retain_max=1, snapshot_retain_min=1)
        table.new_expire_snapshots().config(config).expire()

        self.assertEqual(self._read_ids(table), [1, 2, 3])

    def test_expire_protects_consumer(self):
        table = self._create_table("expire_consumer")
        for i in range(5):
            self._write(table, [i], [f"n{i}"], ["p1"])

        from pypaimon.consumer.consumer import Consumer
        cm = table.consumer_manager()
        cm.reset_consumer("test-consumer", Consumer(next_snapshot=3))

        from pypaimon.options.expire_config import ExpireConfig
        config = ExpireConfig(snapshot_retain_max=1, snapshot_retain_min=1)
        table.new_expire_snapshots().config(config).expire()

        sm = table.snapshot_manager()
        self.assertLessEqual(sm.earliest_snapshot_id(), 3)

    def test_expire_no_snapshots(self):
        table = self._create_table("expire_empty")
        from pypaimon.options.expire_config import ExpireConfig
        config = ExpireConfig(snapshot_retain_max=1)
        expired = table.new_expire_snapshots().config(config).expire()
        self.assertEqual(expired, 0)

    def test_expire_max_deletes(self):
        table = self._create_table("expire_limit")
        for i in range(10):
            self._write(table, [i], [f"n{i}"], ["p1"])

        from pypaimon.options.expire_config import ExpireConfig
        config = ExpireConfig(snapshot_retain_max=1, snapshot_retain_min=1, snapshot_max_deletes=3)
        expired = table.new_expire_snapshots().config(config).expire()

        self.assertLessEqual(expired, 3)

if __name__ == '__main__':
    unittest.main()
