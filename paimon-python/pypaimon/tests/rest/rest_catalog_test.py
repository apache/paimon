#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

import unittest

import pyarrow as pa

from pypaimon import Schema
from pypaimon.common.identifier import Identifier
from pypaimon.table.instant import Instant
from pypaimon.tests.rest.rest_base_test import RESTBaseTest


class RESTCatalogRollbackTest(RESTBaseTest):

    def test_table_rollback(self):
        """Test table rollback to snapshot and tag, mirroring Java testTableRollback."""
        table_name = "default.table_for_rollback"
        pa_schema = pa.schema([('col1', pa.int32())])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)
        identifier = Identifier.from_string(table_name)

        # Write 10 commits and create a tag for each
        for i in range(10):
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            data = pa.Table.from_pydict({'col1': [i]}, schema=pa_schema)
            table_write.write_arrow(data)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()
            table.create_tag("tag-{}".format(i + 1))

        snapshot_mgr = table.snapshot_manager()
        tag_mgr = table.tag_manager()

        # Verify we have 10 snapshots and 10 tags
        latest = snapshot_mgr.get_latest_snapshot()
        self.assertEqual(latest.id, 10)

        # --- Rollback to snapshot 4 ---
        rollback_to_snapshot_id = 4
        self.rest_catalog.rollback_to(
            identifier, Instant.snapshot(rollback_to_snapshot_id))

        # After rollback, latest snapshot should be 4
        latest_after = snapshot_mgr.get_latest_snapshot()
        self.assertEqual(latest_after.id, rollback_to_snapshot_id)

        # Tags with snapshot > 4 should be cleaned (tag-6 and above)
        self.assertFalse(tag_mgr.tag_exists("tag-{}".format(rollback_to_snapshot_id + 2)))

        # Snapshots > 4 should not exist
        snapshot_5 = snapshot_mgr.get_snapshot_by_id(rollback_to_snapshot_id + 1)
        self.assertIsNone(snapshot_5)

        # Rollback to a non-existent snapshot (5) should fail
        with self.assertRaises(Exception):
            self.rest_catalog.rollback_to(
                identifier, Instant.snapshot(rollback_to_snapshot_id + 1))

        # --- Rollback to tag-3 (snapshot 3) ---
        rollback_to_tag_name = "tag-{}".format(rollback_to_snapshot_id - 1)
        self.rest_catalog.rollback_to(identifier, Instant.tag(rollback_to_tag_name))

        tag_snapshot = tag_mgr.get_or_throw(rollback_to_tag_name).trim_to_snapshot()
        latest_after_tag = snapshot_mgr.get_latest_snapshot()
        self.assertEqual(latest_after_tag.id, tag_snapshot.id)

        # --- Rollback to snapshot 2 with from_snapshot check ---
        # from_snapshot=4 should fail because latest is 3
        with self.assertRaises(Exception) as context:
            self.rest_catalog.rollback_to(
                identifier, Instant.snapshot(2), from_snapshot=4)
        self.assertIn("Latest snapshot 3 is not 4", str(context.exception))

        # from_snapshot=3 should succeed
        self.rest_catalog.rollback_to(
            identifier, Instant.snapshot(2), from_snapshot=3)
        latest_final = snapshot_mgr.get_latest_snapshot()
        self.assertEqual(latest_final.id, 2)


if __name__ == '__main__':
    unittest.main()
