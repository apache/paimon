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

"""Tests for the manager-level helpers that back system tables.

The snapshots / schemas / branches system tables need bulk-listing and
mtime accessors that the corresponding managers had not surfaced.
This file pins down their contracts:

* :meth:`SnapshotManager.list_snapshots` — enumerate persisted
  snapshots in ID order, skipping IDs whose files have been expired.
* :meth:`SchemaManager.list_all` — return every committed table
  schema in ID order.
* :meth:`BranchManager.branch_create_time` — millisecond timestamp
  of when a branch was created (filesystem implementation only;
  remote-backed managers fall back to ``None``).
"""

import os
import shutil
import tempfile
import time
import unittest

from pypaimon import CatalogFactory, Schema
from pypaimon.branch.branch_manager import BranchManager
from pypaimon.branch.filesystem_branch_manager import FileSystemBranchManager
from pypaimon.common.file_io import FileIO
from pypaimon.common.json_util import JSON
from pypaimon.schema.data_types import DataField
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.snapshot.snapshot_manager import SnapshotManager


def _write_snapshot(file_io: FileIO, snapshot_dir: str, snapshot_id: int,
                    schema_id: int = 0):
    """Write a minimal snapshot JSON to ``snapshot_dir``."""
    snapshot = Snapshot(
        version=3,
        id=snapshot_id,
        schema_id=schema_id,
        base_manifest_list="base-{}.avro".format(snapshot_id),
        delta_manifest_list="delta-{}.avro".format(snapshot_id),
        total_record_count=0,
        delta_record_count=0,
        commit_user="test-user",
        commit_identifier=snapshot_id,
        commit_kind="APPEND",
        time_millis=int(time.time() * 1000),
    )
    file_io.try_to_write_atomic(
        "{}/snapshot-{}".format(snapshot_dir, snapshot_id),
        JSON.to_json(snapshot),
    )


def _new_warehouse():
    tmp = tempfile.mkdtemp(prefix="mgrext_")
    return tmp, os.path.join(tmp, "warehouse")


class SnapshotManagerListSnapshotsTest(unittest.TestCase):

    def setUp(self):
        self.tmp, self.warehouse = _new_warehouse()
        self.catalog = CatalogFactory.create({"warehouse": self.warehouse})
        self.catalog.create_database("db", False)
        fields = [DataField.from_dict({"id": 0, "name": "v", "type": "INT"})]
        self.catalog.create_table("db.t", Schema(fields=fields), False)
        self.table = self.catalog.get_table("db.t")
        self.file_io = self.table.file_io
        self.snapshot_dir = "{}/snapshot".format(self.table.table_path)
        self.file_io.mkdirs(self.snapshot_dir)
        self.manager = SnapshotManager(
            self.file_io, self.table.table_path, branch="main"
        )

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_empty_table_returns_empty_list(self):
        self.assertEqual([], self.manager.list_snapshots())

    def test_lists_persisted_snapshots_in_id_order(self):
        for sid in (3, 1, 2):  # write out of order to prove sort
            _write_snapshot(self.file_io, self.snapshot_dir, sid)
        result = self.manager.list_snapshots()
        self.assertEqual([1, 2, 3], [s.id for s in result])

    def test_skips_gaps_from_expired_snapshots(self):
        # snapshot-2 missing — simulates an expired ID still bracketed by
        # earlier and later live snapshots.
        for sid in (1, 3):
            _write_snapshot(self.file_io, self.snapshot_dir, sid)
        result = self.manager.list_snapshots()
        self.assertEqual([1, 3], [s.id for s in result])


class SchemaManagerListAllTest(unittest.TestCase):

    def setUp(self):
        self.tmp, self.warehouse = _new_warehouse()
        self.catalog = CatalogFactory.create({"warehouse": self.warehouse})
        self.catalog.create_database("db", False)
        fields = [
            DataField.from_dict({"id": 0, "name": "v", "type": "INT"}),
        ]
        self.catalog.create_table("db.t", Schema(fields=fields), False)
        self.manager = self.catalog.get_table("db.t").schema_manager

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_returns_single_schema_initially(self):
        result = self.manager.list_all()
        self.assertEqual([0], [s.id for s in result])

    def test_lists_every_committed_schema_in_id_order(self):
        for new_value in ("a", "b", "c"):
            self.manager.commit_changes([SchemaChange.set_option(
                "user-tag", new_value)])
        result = self.manager.list_all()
        self.assertEqual([0, 1, 2, 3], [s.id for s in result])
        # Cache shouldn't return stale objects across calls.
        again = self.manager.list_all()
        self.assertEqual([0, 1, 2, 3], [s.id for s in again])


class BranchCreateTimeTest(unittest.TestCase):

    def setUp(self):
        self.tmp, self.warehouse = _new_warehouse()
        self.catalog = CatalogFactory.create({"warehouse": self.warehouse})
        self.catalog.create_database("db", False)
        fields = [DataField.from_dict({"id": 0, "name": "v", "type": "INT"})]
        self.catalog.create_table("db.t", Schema(fields=fields), False)
        self.table = self.catalog.get_table("db.t")

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _make_branch(self, name: str):
        # Write a snapshot first so create_branch (which requires one)
        # can proceed.
        snapshot_dir = "{}/snapshot".format(self.table.table_path)
        self.table.file_io.mkdirs(snapshot_dir)
        _write_snapshot(self.table.file_io, snapshot_dir, 1)
        self.table.file_io.try_to_write_atomic(
            "{}/LATEST".format(snapshot_dir), "1")
        self.table.file_io.try_to_write_atomic(
            "{}/EARLIEST".format(snapshot_dir), "1")
        branch_mgr = self.table.branch_manager()
        branch_mgr.create_branch(name)
        return branch_mgr

    def test_filesystem_branch_returns_ms_timestamp(self):
        before_ms = int(time.time() * 1000)
        mgr = self._make_branch("dev")
        after_ms = int(time.time() * 1000)

        self.assertIsInstance(mgr, FileSystemBranchManager)
        ts = mgr.branch_create_time("dev")
        self.assertIsNotNone(ts)
        # Allow a small skew on slower filesystems.
        self.assertGreaterEqual(ts, before_ms - 5000)
        self.assertLessEqual(ts, after_ms + 5000)

    def test_filesystem_branch_returns_none_for_missing_branch(self):
        mgr = self._make_branch("dev")
        self.assertIsNone(mgr.branch_create_time("does_not_exist"))

    def test_base_class_default_is_none(self):
        # Any BranchManager subclass that has no native answer must
        # return None rather than raise — system tables rely on this
        # to render a placeholder cell.
        self.assertIsNone(
            BranchManager.branch_create_time(BranchManager(), "x")
        )


if __name__ == "__main__":
    unittest.main()
