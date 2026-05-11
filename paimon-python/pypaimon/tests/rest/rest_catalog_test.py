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


class RESTCatalogTest(RESTBaseTest):

    def test_catalog_rollback(self):
        """Test table rollback to snapshot and tag."""
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
        with self.assertRaises(ValueError) as context:
            self.rest_catalog.rollback_to(
                identifier, Instant.snapshot(rollback_to_snapshot_id + 1))
        self.assertIn("Rollback snapshot", str(context.exception))
        self.assertIn("doesn't exist", str(context.exception))

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

    def test_catalog_rollback_to_nonexistent_tag(self):
        """Test that rollback to a non-existent tag raises an error."""
        table_name = "default.table_rollback_no_tag"
        pa_schema = pa.schema([('col1', pa.int32())])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)
        identifier = Identifier.from_string(table_name)

        # Write one commit so the table has a snapshot
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data = pa.Table.from_pydict({'col1': [1]}, schema=pa_schema)
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        with self.assertRaises(ValueError) as context:
            self.rest_catalog.rollback_to(
                identifier, Instant.tag("nonexistent-tag"))
        self.assertIn("Rollback tag", str(context.exception))
        self.assertIn("nonexistent-tag", str(context.exception))
        self.assertIn("doesn't exist", str(context.exception))

    def test_catalog_rollback_with_string_identifier(self):
        """Test rollback using a string identifier instead of Identifier object."""
        table_name = "default.table_rollback_str_id"
        pa_schema = pa.schema([('col1', pa.int32())])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)

        for i in range(3):
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            data = pa.Table.from_pydict({'col1': [i]}, schema=pa_schema)
            table_write.write_arrow(data)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

        snapshot_mgr = table.snapshot_manager()
        self.assertEqual(snapshot_mgr.get_latest_snapshot().id, 3)

        # Use string identifier directly (not Identifier object)
        self.rest_catalog.rollback_to(table_name, Instant.snapshot(2))
        self.assertEqual(snapshot_mgr.get_latest_snapshot().id, 2)

    def test_catalog_rollback_on_nonexistent_table(self):
        """Test that rollback on a non-existent table raises an error."""
        from pypaimon.catalog.catalog_exception import TableNotExistException
        identifier = Identifier.from_string("default.no_such_table")
        with self.assertRaises(TableNotExistException) as context:
            self.rest_catalog.rollback_to(identifier, Instant.snapshot(1))
        self.assertIn("default.no_such_table", str(context.exception))
        self.assertIn("does not exist", str(context.exception))

    def test_table_rollback_to_snapshot(self):
        """Test table-level rollback_to_snapshot via FileStoreTable."""
        table_name = "default.table_level_rollback_snapshot"
        pa_schema = pa.schema([('col1', pa.int32())])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)

        # Write 5 commits
        for i in range(5):
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            data = pa.Table.from_pydict({'col1': [i]}, schema=pa_schema)
            table_write.write_arrow(data)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

        snapshot_mgr = table.snapshot_manager()
        self.assertEqual(snapshot_mgr.get_latest_snapshot().id, 5)

        # Rollback to snapshot 3 via table method (singledispatch on int)
        table.rollback_to(3)

        self.assertEqual(snapshot_mgr.get_latest_snapshot().id, 3)
        self.assertIsNone(snapshot_mgr.get_snapshot_by_id(4))
        self.assertIsNone(snapshot_mgr.get_snapshot_by_id(5))

    def test_table_rollback_to_tag(self):
        """Test table-level rollback_to_tag via FileStoreTable."""
        table_name = "default.table_level_rollback_tag"
        pa_schema = pa.schema([('col1', pa.int32())])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)

        # Write 5 commits and create tags
        for i in range(5):
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            data = pa.Table.from_pydict({'col1': [i]}, schema=pa_schema)
            table_write.write_arrow(data)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()
            table.create_tag("v{}".format(i + 1))

        snapshot_mgr = table.snapshot_manager()
        tag_mgr = table.tag_manager()
        self.assertEqual(snapshot_mgr.get_latest_snapshot().id, 5)

        # Rollback to tag v3 (singledispatch on str)
        table.rollback_to("v3")

        self.assertEqual(snapshot_mgr.get_latest_snapshot().id, 3)
        # Tags with snapshot > 3 should be cleaned
        self.assertFalse(tag_mgr.tag_exists("v4"))
        self.assertFalse(tag_mgr.tag_exists("v5"))
        # Tag v3 should still exist
        self.assertTrue(tag_mgr.tag_exists("v3"))

    def test_table_rollback_to_nonexistent_snapshot(self):
        """Test that table-level rollback to non-existent snapshot raises ValueError."""
        table_name = "default.table_level_rollback_no_snap"
        pa_schema = pa.schema([('col1', pa.int32())])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)

        # Write 2 commits
        for i in range(2):
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            data = pa.Table.from_pydict({'col1': [i]}, schema=pa_schema)
            table_write.write_arrow(data)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

        # Rollback to snapshot 99 should fail
        with self.assertRaises(Exception) as context:
            table.rollback_to(99)
        self.assertIn("Rollback snapshot", str(context.exception))
        self.assertIn("99", str(context.exception))
        self.assertIn("doesn't exist", str(context.exception))

    def test_table_rollback_to_nonexistent_tag(self):
        """Test that table-level rollback to non-existent tag raises ValueError."""
        table_name = "default.table_level_rollback_no_tag"
        pa_schema = pa.schema([('col1', pa.int32())])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)

        # Write 1 commit
        write_builder = table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        data = pa.Table.from_pydict({'col1': [1]}, schema=pa_schema)
        table_write.write_arrow(data)
        table_commit.commit(table_write.prepare_commit())
        table_write.close()
        table_commit.close()

        with self.assertRaises(Exception) as context:
            table.rollback_to("no-such-tag")
        self.assertIn("Rollback tag", str(context.exception))
        self.assertIn("no-such-tag", str(context.exception))
        self.assertIn("doesn't exist", str(context.exception))

    def test_catalog_load_snapshot(self):
        """Test RESTCatalog.load_snapshot returns the latest snapshot."""
        table_name = "default.table_for_load_snapshot"
        pa_schema = pa.schema([('col1', pa.int32())])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)
        identifier = Identifier.from_string(table_name)

        # Initially, no snapshot exists
        table_snapshot = self.rest_catalog.load_snapshot(identifier)
        self.assertIsNone(table_snapshot)

        # Write 3 commits
        for i in range(3):
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            data = pa.Table.from_pydict({'col1': [i]}, schema=pa_schema)
            table_write.write_arrow(data)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

        # Load snapshot should return the latest (snapshot 3)
        snapshot = self.rest_catalog.load_snapshot(identifier).snapshot
        self.assertIsNotNone(snapshot)
        self.assertEqual(snapshot.id, 3)
        self.assertEqual(snapshot.schema_id, 0)

    def test_catalog_load_snapshot_with_string_identifier(self):
        """Test load_snapshot using a string identifier instead of Identifier object."""
        table_name = "default.table_load_snapshot_str_id"
        pa_schema = pa.schema([('col1', pa.int32())])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)

        # Write 2 commits
        for i in range(2):
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            data = pa.Table.from_pydict({'col1': [i]}, schema=pa_schema)
            table_write.write_arrow(data)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

        # Use string identifier directly
        table_snapshot = self.rest_catalog.load_snapshot(table_name)
        self.assertIsNotNone(table_snapshot)
        self.assertEqual(table_snapshot.snapshot.id, 2)

    def test_catalog_load_snapshot_nonexistent_table(self):
        """Test that load_snapshot on a non-existent table raises an error."""
        from pypaimon.catalog.catalog_exception import TableNotExistException
        identifier = Identifier.from_string("default.no_such_table_for_snapshot")
        with self.assertRaises(TableNotExistException) as context:
            self.rest_catalog.load_snapshot(identifier)
        self.assertIn("no_such_table_for_snapshot", str(context.exception))
        self.assertIn("does not exist", str(context.exception))

    def test_catalog_load_snapshot_after_rollback(self):
        """Test load_snapshot returns correct snapshot after rollback."""
        table_name = "default.table_load_snapshot_after_rollback"
        pa_schema = pa.schema([('col1', pa.int32())])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.rest_catalog.create_table(table_name, schema, False)
        table = self.rest_catalog.get_table(table_name)
        identifier = Identifier.from_string(table_name)

        # Write 5 commits
        for i in range(5):
            write_builder = table.new_batch_write_builder()
            table_write = write_builder.new_write()
            table_commit = write_builder.new_commit()
            data = pa.Table.from_pydict({'col1': [i]}, schema=pa_schema)
            table_write.write_arrow(data)
            table_commit.commit(table_write.prepare_commit())
            table_write.close()
            table_commit.close()

        # Verify latest snapshot is 5
        table_snapshot = self.rest_catalog.load_snapshot(identifier)
        self.assertEqual(table_snapshot.snapshot.id, 5)

        # Rollback to snapshot 3
        from pypaimon.table.instant import Instant
        self.rest_catalog.rollback_to(identifier, Instant.snapshot(3))

        # Load snapshot should now return snapshot 3
        table_snapshot = self.rest_catalog.load_snapshot(identifier)
        self.assertIsNotNone(table_snapshot)
        self.assertEqual(table_snapshot.snapshot.id, 3)


if __name__ == '__main__':
    unittest.main()
