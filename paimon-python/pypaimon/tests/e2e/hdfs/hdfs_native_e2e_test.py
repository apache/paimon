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

"""End-to-end tests for the native HDFS FileIO backend.

Disabled by default. To run:
  1. Start an HDFS cluster — see docker-compose.yml in this directory.
  2. Install pypaimon with the hdfs extra:
       pip install -e '.[hdfs]'
  3. Point the tests at the cluster and run:
       PYPAIMON_HDFS_E2E_URL=hdfs://localhost:8020 \\
         python -m pytest pypaimon/tests/e2e/hdfs/hdfs_native_e2e_test.py -v

To exercise the REST-catalog config-delivery path (no local xml), put the
Hadoop config k/v in catalog options under the `dfs.*` / `fs.*` namespaces
or via `hdfs.config.*` — both are forwarded to the underlying client.
"""

import os
import unittest
import uuid

import pandas as pd
import pyarrow as pa

E2E_URL = os.environ.get("PYPAIMON_HDFS_E2E_URL")
SKIP_REASON = ("PYPAIMON_HDFS_E2E_URL not set; skipping HDFS e2e. "
               "See docker-compose.yml in this directory.")


@unittest.skipIf(not E2E_URL, SKIP_REASON)
class HdfsNativeE2ETest(unittest.TestCase):
    """Smoke-test the native HDFS backend end-to-end against a live cluster."""

    @classmethod
    def setUpClass(cls):
        try:
            import hdfs_native  # noqa: F401
        except ImportError as e:
            raise unittest.SkipTest(
                "hdfs-native not installed. pip install 'pypaimon[hdfs]'"
            ) from e

        from pypaimon.catalog.catalog_factory import CatalogFactory
        cls.warehouse = (
            f"{E2E_URL}/pypaimon-e2e/warehouse-{uuid.uuid4().hex[:8]}"
        )
        cls.catalog = CatalogFactory.create({
            "warehouse": cls.warehouse,
            "hdfs.client.impl": "native",
        })
        cls.catalog.create_database("default", True)

    def _make_table(self, name, schema):
        from pypaimon.schema.schema import Schema
        fqn = f"default.{name}"
        s = Schema.from_pyarrow_schema(
            schema,
            options={"file.format": "parquet"},
        )
        self.catalog.create_table(fqn, s, False)
        return self.catalog.get_table(fqn)

    def test_write_then_read_back(self):
        pa_schema = pa.schema([
            ("id", pa.int64()),
            ("payload", pa.string()),
        ])
        table = self._make_table(f"t_{uuid.uuid4().hex[:8]}", pa_schema)

        data = pd.DataFrame({
            "id": list(range(100)),
            "payload": [f"row-{i}" for i in range(100)],
        })

        writer = table.new_batch_write_builder().new_write()
        writer.write_pandas(data)
        commit_msgs = writer.prepare_commit()
        committer = table.new_batch_write_builder().new_commit()
        committer.commit(commit_msgs)
        writer.close()
        committer.close()

        scan = table.new_read_builder().new_scan()
        reader = table.new_read_builder().new_read()
        splits = scan.plan().splits()
        result = reader.to_arrow(splits)

        self.assertEqual(result.num_rows, 100)


if __name__ == "__main__":
    unittest.main()
