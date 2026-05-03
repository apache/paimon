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
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.compact.options import CompactOptions

try:
    import ray  # noqa: F401
    HAS_RAY = True
except ImportError:
    HAS_RAY = False


@unittest.skipUnless(HAS_RAY, "ray is not installed")
class RayExecutorE2ETest(unittest.TestCase):
    """End-to-end Ray execution: rewrite via worker tasks, commit on driver.

    Uses a real local Ray runtime — the executor under test ships task
    payloads through ray.remote, which exercises the full CompactTask
    serde path AppendCompactTask / MergeTreeCompactTask were extended
    with this phase.
    """

    @classmethod
    def setUpClass(cls):
        import ray
        cls.temp_dir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.temp_dir, "warehouse")
        cls.catalog_options = {"warehouse": cls.warehouse}
        cls.catalog = CatalogFactory.create(cls.catalog_options)
        cls.catalog.create_database("ray_db", False)
        # local_mode keeps the test single-process; faster startup, no port
        # races and the same code path as a real cluster.
        ray.init(local_mode=True, ignore_reinit_error=True, log_to_driver=False)

    @classmethod
    def tearDownClass(cls):
        import ray
        ray.shutdown()
        shutil.rmtree(cls.temp_dir, ignore_errors=True)

    def _make_unaware_table(self, name: str):
        full = f"ray_db.{name}"
        opts = {
            CoreOptions.BUCKET.key(): "-1",
            CoreOptions.TARGET_FILE_SIZE.key(): "10mb",
        }
        pa_schema = pa.schema([("id", pa.int32()), ("name", pa.string())])
        schema = Schema.from_pyarrow_schema(pa_schema, options=opts)
        self.catalog.create_table(full, schema, True)
        return self.catalog.get_table(full)

    def _write_one(self, table, batch: pa.Table):
        builder = table.new_batch_write_builder()
        write = builder.new_write()
        commit = builder.new_commit()
        write.write_arrow(batch)
        commit.commit(write.prepare_commit())
        write.close()
        commit.close()

    def _read_sorted(self, table) -> pa.Table:
        rb = table.new_read_builder()
        scan = rb.new_scan()
        splits = scan.plan().splits()
        return rb.new_read().to_arrow(splits).sort_by("id")

    def test_append_compact_via_ray_executor(self):
        from pypaimon.compact.executor.ray_executor import RayExecutor

        table = self._make_unaware_table("ray_append")
        for i in range(5):
            self._write_one(table, pa.Table.from_pydict({
                "id": pa.array([i * 2, i * 2 + 1], type=pa.int32()),
                "name": [f"r-{i}-a", f"r-{i}-b"],
            }))

        table = self.catalog.get_table("ray_db.ray_append")
        before_data = self._read_sorted(table)

        # Note: deliberately omit table_identifier — exercises the default
        # path (table.identifier.get_full_name()) which the worker uses
        # via Identifier.from_string. A regression here would surface as
        # "Cannot get splits from 'Identifier(...)'" inside the Ray task.
        job = table.new_compact_job(
            compact_options=CompactOptions(min_file_num=5),
            executor=RayExecutor(),
            catalog_options=self.catalog_options,
        )
        messages = job.execute()

        self.assertEqual(1, len(messages))
        self.assertGreaterEqual(len(messages[0].compact_before), 5)

        table = self.catalog.get_table("ray_db.ray_append")
        after_data = self._read_sorted(table)
        self.assertEqual(before_data, after_data)
        self.assertEqual("COMPACT", table.snapshot_manager().get_latest_snapshot().commit_kind)


if __name__ == "__main__":
    unittest.main()
