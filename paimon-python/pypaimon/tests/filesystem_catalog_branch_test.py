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

"""End-to-end tests for branch CRUD on ``FileSystemCatalog``.

Mirrors the ``RESTCatalogBranchCRUDTest`` matrix from the REST branch
tests but exercises the local filesystem path. Pins down the exception
types and return shapes the catalog layer must produce regardless of
which catalog implementation is in use.
"""

import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.catalog.catalog_exception import (BranchAlreadyExistException,
                                                BranchNotExistException,
                                                TableNotExistException,
                                                TagNotExistException)
from pypaimon.common.identifier import Identifier
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.schema.data_types import AtomicType
from pypaimon.schema.schema_change import SchemaChange


class FileSystemCatalogBranchCRUDTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="unittest_fs_branch_")
        warehouse = os.path.join(self.temp_dir, "warehouse")
        os.makedirs(warehouse, exist_ok=True)
        self.catalog = CatalogFactory.create({"warehouse": warehouse})
        self.catalog.create_database("default", True)

        self.pa_schema = pa.schema([
            ("id", pa.int64()),
            ("value", pa.string()),
        ])
        self.identifier = Identifier.from_string("default.test_branch_table")
        self.catalog.create_table(
            self.identifier,
            Schema.from_pyarrow_schema(self.pa_schema),
            False,
        )
        # Commit one batch so the table has a snapshot to base branches on.
        table = self.catalog.get_table(self.identifier)
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        w.write_arrow(pa.Table.from_pydict(
            {"id": [1, 2, 3], "value": ["a", "b", "c"]},
            schema=self.pa_schema,
        ))
        wb.new_commit().commit(w.prepare_commit())
        w.close()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    @staticmethod
    def _write(table, data):
        builder = table.new_batch_write_builder()
        writer = builder.new_write()
        try:
            writer.write_arrow(data)
            builder.new_commit().commit(writer.prepare_commit())
        finally:
            writer.close()

    @staticmethod
    def _read(table):
        builder = table.new_read_builder()
        return builder.new_read().to_arrow(builder.new_scan().plan().splits())

    # -- create + list --------------------------------------------------------

    def test_create_branch_without_from_tag(self):
        self.catalog.create_branch(self.identifier, "b1")
        self.assertEqual(self.catalog.list_branches(self.identifier), ["b1"])

    def test_alter_table_isolated_to_branch(self):
        self.catalog.create_branch(self.identifier, "b1")
        branch_identifier = Identifier(
            self.identifier.get_database_name(),
            self.identifier.get_table_name(),
            branch="b1",
        )

        self.catalog.alter_table(
            branch_identifier,
            [SchemaChange.add_column("branch_col", AtomicType("STRING"))],
        )

        self.assertNotIn(
            "branch_col", self.catalog.get_table(self.identifier).field_names)
        self.assertIn(
            "branch_col", self.catalog.get_table(branch_identifier).field_names)

    def test_write_blob_to_data_evolution_branch(self):
        schema = pa.schema([
            ("id", pa.int64()),
            ("payload", pa.large_binary()),
        ])
        identifier = Identifier.from_string("default.test_de_blob_branch")
        self.catalog.create_table(
            identifier,
            Schema.from_pyarrow_schema(
                schema,
                options={
                    "data-evolution.enabled": "true",
                    "row-tracking.enabled": "true",
                    "blob-field": "payload",
                },
            ),
            False,
        )
        main = self.catalog.get_table(identifier)
        self._write(main, pa.table({
            "id": [1],
            "payload": pa.array([b"main"], pa.large_binary()),
        }, schema=schema))
        main.create_tag("base")
        self.catalog.create_branch(identifier, "b1", tag_name="base")

        branch_identifier = Identifier(
            identifier.get_database_name(), identifier.get_table_name(), branch="b1")
        branch = self.catalog.get_table(branch_identifier)
        self._write(branch, pa.table({
            "id": [2],
            "payload": pa.array([b"branch"], pa.large_binary()),
        }, schema=schema))

        self.assertEqual(
            self._read(main).to_pydict(),
            {"id": [1], "payload": [b"main"]},
        )
        self.assertEqual(
            self._read(branch).to_pydict(),
            {"id": [1, 2], "payload": [b"main", b"branch"]},
        )

    def test_create_branch_duplicate_raises(self):
        self.catalog.create_branch(self.identifier, "b1")
        with self.assertRaises(BranchAlreadyExistException) as cm:
            self.catalog.create_branch(self.identifier, "b1")
        self.assertEqual(cm.exception.branch, "b1")

    def test_create_branch_table_not_exists(self):
        with self.assertRaises(TableNotExistException):
            self.catalog.create_branch(
                Identifier.from_string("default.no_such_table"), "b1")

    def test_create_branch_from_nonexistent_tag_raises(self):
        with self.assertRaises(TagNotExistException) as cm:
            self.catalog.create_branch(
                self.identifier, "b1", tag_name="absent_tag")
        self.assertEqual(cm.exception.tag, "absent_tag")

    def test_create_branch_from_existing_tag(self):
        # The from-tag happy path: create_tag then create_branch(tag_name=...)
        # must land the branch files under ``branch/branch-<name>/`` and not
        # raise (regresses the historical src == dst SameFileError).
        table = self.catalog.get_table(self.identifier)
        table.create_tag("t1")
        self.catalog.create_branch(self.identifier, "b1", tag_name="t1")
        self.assertIn("b1", self.catalog.list_branches(self.identifier))
        branch_root = "{}/branch/branch-b1".format(
            table.table_path.rstrip('/'))
        self.assertTrue(os.path.isdir(branch_root))
        self.assertTrue(os.path.isfile("{}/tag/tag-t1".format(branch_root)))

    def test_create_branch_from_tag_after_snapshot_expired(self):
        table = self.catalog.get_table(self.identifier)
        snapshot = table.snapshot_manager().get_latest_snapshot()
        table.create_tag("t1", time_retained="1d")
        tag = table.tag_manager().get_or_throw("t1")
        self._write(table, pa.table({"id": [4], "value": ["d"]}))

        # Simulate snapshot expiration while the tag retains its metadata and data.
        snapshot_path = table.snapshot_manager().get_snapshot_path(snapshot.id)
        table.file_io.delete(snapshot_path)
        expected = {"id": [1, 2, 3], "value": ["a", "b", "c"]}
        self.assertEqual(
            self._read(table.copy({"scan.tag-name": "t1"})).to_pydict(), expected)

        self.catalog.create_branch(self.identifier, "b1", tag_name="t1")

        branch_identifier = Identifier(
            self.identifier.get_database_name(), self.identifier.get_table_name(), branch="b1")
        branch = self.catalog.get_table(branch_identifier)
        self.assertEqual(branch.snapshot_manager().get_latest_snapshot(), snapshot)
        self.assertEqual(branch.tag_manager().get_or_throw("t1"), tag)
        self.assertEqual(self._read(branch).to_pydict(), expected)
        self.assertFalse(table.file_io.exists(snapshot_path))
        self.assertEqual(table.snapshot_manager().get_latest_snapshot().id, snapshot.id + 1)
        self.assertEqual(
            self._read(table).sort_by("id").to_pydict(),
            {"id": [1, 2, 3, 4], "value": ["a", "b", "c", "d"]})

    # -- list -----------------------------------------------------------------

    def test_list_branches_returns_created(self):
        for name in ("b1", "b2", "b3"):
            self.catalog.create_branch(self.identifier, name)
        self.assertEqual(
            sorted(self.catalog.list_branches(self.identifier)),
            ["b1", "b2", "b3"],
        )

    def test_list_branches_empty(self):
        # Fresh table with no branches created.
        self.assertEqual(self.catalog.list_branches(self.identifier), [])

    def test_list_branches_table_not_exists(self):
        with self.assertRaises(TableNotExistException):
            self.catalog.list_branches(
                Identifier.from_string("default.no_such_table"))

    # -- rename ---------------------------------------------------------------

    def test_rename_branch_happy(self):
        self.catalog.create_branch(self.identifier, "b1")
        self.catalog.rename_branch(self.identifier, "b1", "b2")
        listed = self.catalog.list_branches(self.identifier)
        self.assertNotIn("b1", listed)
        self.assertIn("b2", listed)

    def test_rename_branch_to_existing_raises(self):
        self.catalog.create_branch(self.identifier, "b1")
        self.catalog.create_branch(self.identifier, "b2")
        with self.assertRaises(BranchAlreadyExistException) as cm:
            self.catalog.rename_branch(self.identifier, "b1", "b2")
        self.assertEqual(cm.exception.branch, "b2")

    def test_rename_branch_from_missing_raises(self):
        with self.assertRaises(BranchNotExistException) as cm:
            self.catalog.rename_branch(self.identifier, "absent", "b2")
        self.assertEqual(cm.exception.branch, "absent")

    # -- drop -----------------------------------------------------------------

    def test_drop_branch_happy(self):
        self.catalog.create_branch(self.identifier, "b1")
        self.catalog.drop_branch(self.identifier, "b1")
        self.assertNotIn(
            "b1", self.catalog.list_branches(self.identifier))

    def test_drop_branch_missing_raises(self):
        with self.assertRaises(BranchNotExistException) as cm:
            self.catalog.drop_branch(self.identifier, "absent")
        self.assertEqual(cm.exception.branch, "absent")

    def test_drop_main_branch_preserves_table(self):
        table = self.catalog.get_table(self.identifier)
        with self.assertRaisesRegex(ValueError, r"^The main branch cannot be deleted\.$"):
            self.catalog.drop_branch(self.identifier, "main")
        self.assertTrue(os.path.isdir(table.table_path))
        self.assertEqual(
            self._read(self.catalog.get_table(self.identifier)).to_pydict(),
            {"id": [1, 2, 3], "value": ["a", "b", "c"]},
        )

    # -- fast_forward ---------------------------------------------------------

    def test_fast_forward_missing_raises(self):
        with self.assertRaises(BranchNotExistException) as cm:
            self.catalog.fast_forward(self.identifier, "absent")
        self.assertEqual(cm.exception.branch, "absent")

    def test_fast_forward_after_create_branch_from_tag(self):
        # Happy path: create a branch from a tag, then fast-forward main to
        # it. Must not raise (regresses the historical src == dst error).
        table = self.catalog.get_table(self.identifier)
        table.create_tag("t1")
        self.catalog.create_branch(self.identifier, "b1", tag_name="t1")
        self.catalog.fast_forward(self.identifier, "b1")

    def test_fast_forward_table_not_exists(self):
        with self.assertRaises(TableNotExistException):
            self.catalog.fast_forward("default.no_such_table", "b1")

    def test_fast_forward_rejects_system_table(self):
        table = self.catalog.get_table(self.identifier)
        table.create_tag("base")
        self.catalog.create_branch(self.identifier, "b1", tag_name="base")
        with self.assertRaises(ValueError):
            self.catalog.fast_forward(self.identifier.get_full_name() + "$snapshots", "b1")
        self.assertEqual(
            self._read(self._fresh_table()).to_pydict(),
            {"id": [1, 2, 3], "value": ["a", "b", "c"]})

    def test_fast_forward_rejects_fallback_branch(self):
        catalog = CatalogFactory.create({
            "warehouse": os.path.join(self.temp_dir, "warehouse"),
            CoreOptions.SCAN_FALLBACK_BRANCH.key(): "b1",
        })
        with self.assertRaisesRegex(ValueError, "Unsupported CoreOption"):
            catalog.fast_forward(self.identifier, "b1")

    def _fresh_table(self, branch=None):
        catalog = CatalogFactory.create({
            "warehouse": os.path.join(self.temp_dir, "warehouse"),
        })
        return catalog.get_table(Identifier(
            self.identifier.get_database_name(), self.identifier.get_table_name(), branch=branch))

    def test_fast_forward_replaces_main_snapshots_and_can_write(self):
        table = self.catalog.get_table(self.identifier)
        table.create_tag("base")
        self.catalog.create_branch(self.identifier, "b1", tag_name="base")
        branch = self._fresh_table("b1")
        self._write(branch, pa.table({"id": [4], "value": ["branch"]}))
        for value in (5, 6):
            self._write(table, pa.table({"id": [value], "value": ["main"]}))

        self.catalog.fast_forward(self.identifier, "b1")

        main = self._fresh_table()
        expected = {"id": [1, 2, 3, 4], "value": ["a", "b", "c", "branch"]}
        self.assertEqual(self._read(main).sort_by("id").to_pydict(), expected)
        self.assertEqual(main.snapshot_manager().get_latest_snapshot().id, 2)
        self.assertEqual([s.id for s in main.snapshot_manager().list_snapshots()], [1, 2])
        self._write(main, pa.table({"id": [7], "value": ["after"]}))
        self.assertEqual(
            self._read(self._fresh_table()).sort_by("id").to_pydict(),
            {"id": [1, 2, 3, 4, 7], "value": ["a", "b", "c", "branch", "after"]})
        self.assertEqual(self._read(self._fresh_table("b1")).sort_by("id").to_pydict(), expected)

    def test_fast_forward_copies_branch_schema(self):
        table = self.catalog.get_table(self.identifier)
        table.create_tag("base")
        self.catalog.create_branch(self.identifier, "b1", tag_name="base")
        branch_identifier = self._fresh_table("b1").identifier
        self.catalog.alter_table(
            branch_identifier, [SchemaChange.add_column("branch_col", AtomicType("STRING"))])
        branch = self._fresh_table("b1")
        self._write(branch, pa.table({"id": [4], "value": ["branch"], "branch_col": ["added"]}))

        self.catalog.fast_forward(self.identifier, "b1")

        main = self._fresh_table()
        self.assertEqual(
            self._read(main).sort_by("id").to_pydict(),
            {"id": [1, 2, 3, 4], "value": ["a", "b", "c", "branch"],
             "branch_col": [None, None, None, "added"]})
        self.assertEqual(main.schema(), branch.schema())
        self._write(main, pa.table({"id": [5], "value": ["after"], "branch_col": ["next"]}))
        self.assertEqual(
            self._read(self._fresh_table()).sort_by("id").to_pydict(),
            {"id": [1, 2, 3, 4, 5], "value": ["a", "b", "c", "branch", "after"],
             "branch_col": [None, None, None, "added", "next"]})
        self.assertEqual(self._read(self._fresh_table("b1")).num_rows, 4)

    def test_fast_forward_replaces_divergent_schemas_without_branch_commit(self):
        table = self.catalog.get_table(self.identifier)
        table.create_tag("base")
        self.catalog.create_branch(self.identifier, "b1", tag_name="base")
        branch_identifier = self._fresh_table("b1").identifier
        self.catalog.alter_table(
            branch_identifier, [SchemaChange.add_column("branch_col", AtomicType("STRING"))])
        self.catalog.alter_table(
            self.identifier, [SchemaChange.add_column("main_col", AtomicType("STRING"))])
        self.catalog.alter_table(
            self.identifier, [SchemaChange.add_column("main_extra", AtomicType("INT"))])

        # A schema change need not have a corresponding snapshot yet.
        self.catalog.fast_forward(self.identifier, "b1")

        main = self._fresh_table()
        self.assertEqual(main.schema(), self._fresh_table("b1").schema())
        self.assertEqual([s.id for s in main.schema_manager.list_all()], [0, 1])
        self.assertEqual(
            self._read(main).to_pydict(),
            {"id": [1, 2, 3], "value": ["a", "b", "c"], "branch_col": [None, None, None]})

    def test_fast_forward_preserves_history_and_replaces_tags(self):
        table = self.catalog.get_table(self.identifier)
        table.create_tag("older")
        self._write(table, pa.table({"id": [4], "value": ["base"]}))
        table.create_tag("base")
        self.catalog.create_branch(self.identifier, "b1", tag_name="base")
        branch = self._fresh_table("b1")
        self._write(branch, pa.table({"id": [5], "value": ["branch"]}))
        branch.create_tag("branch_tip", time_retained="1d")
        self._write(table, pa.table({"id": [6], "value": ["main"]}))
        table.create_tag("main_tip")
        self._write(table, pa.table({"id": [7], "value": ["main"]}))
        branch_snapshot = branch.snapshot_manager().get_latest_snapshot()
        branch_tag = branch.tag_manager().get_or_throw("branch_tip")

        for _ in range(2):
            self.catalog.fast_forward(self.identifier, "b1")
            main = self._fresh_table()
            self.assertEqual(main.snapshot_manager().get_latest_snapshot(), branch_snapshot)
            self.assertEqual(main.snapshot_manager().try_get_earliest_snapshot().id, 1)
            self.assertEqual(sorted(main.list_tags()), ["base", "branch_tip", "older"])
            self.assertEqual(main.tag_manager().get_or_throw("branch_tip"), branch_tag)
            expected_old = {"id": [1, 2, 3], "value": ["a", "b", "c"]}
            for options in ({"scan.snapshot-id": "1"}, {"scan.tag-name": "older"}):
                self.assertEqual(self._read(main.copy(options)).to_pydict(), expected_old)
            self.assertEqual(
                self._read(main.copy({"scan.tag-name": "branch_tip"})).sort_by("id").to_pydict(),
                {"id": [1, 2, 3, 4, 5], "value": ["a", "b", "c", "base", "branch"]})
        self.assertEqual(sorted(self._fresh_table("b1").list_tags()), ["base", "branch_tip"])

    def test_fast_forward_branch_without_tags(self):
        table = self.catalog.get_table(self.identifier)
        table.create_tag("main_tip")
        self.catalog.create_branch(self.identifier, "b1")
        branch = self._fresh_table("b1")
        self._write(branch, pa.table({"id": [4], "value": ["branch"]}))
        self._write(table, pa.table({"id": [5], "value": ["main"]}))
        self.assertFalse(branch.file_io.exists(branch.tag_manager().tag_directory()))

        self.catalog.fast_forward(self.identifier, "b1")

        main = self._fresh_table()
        self.assertEqual(self._read(main).to_pydict(), {"id": [4], "value": ["branch"]})
        self.assertEqual(main.list_tags(), [])

    def test_fast_forward_retries_partial_snapshot_copy(self):
        table = self.catalog.get_table(self.identifier)
        table.create_tag("base")
        self.catalog.create_branch(self.identifier, "b1", tag_name="base")
        branch = self._fresh_table("b1")
        self._write(branch, pa.table({"id": [4], "value": ["branch"]}))
        self._write(table, pa.table({"id": [5], "value": ["main"]}))
        snapshot_path = table.snapshot_manager().get_snapshot_path(2)
        copy_file = table.file_io.copy_file

        def fail_copy(source, target, overwrite=False):
            if target == snapshot_path:
                table.file_io.write_file(target, "{", overwrite=True)
                raise OSError("Interrupted snapshot copy")
            return copy_file(source, target, overwrite)

        with patch.object(table.file_io, "copy_file", side_effect=fail_copy):
            with self.assertRaises(RuntimeError) as cm:
                self.catalog.fast_forward(self.identifier, "b1")
        self.assertIsInstance(cm.exception.__cause__, OSError)
        self.assertEqual(table.file_io.read_file_utf8(snapshot_path), "{")

        self.catalog.fast_forward(self.identifier, "b1")

        expected = {"id": [1, 2, 3, 4], "value": ["a", "b", "c", "branch"]}
        self.assertEqual(self._read(self._fresh_table()).sort_by("id").to_pydict(), expected)
        self.assertEqual(self._read(self._fresh_table("b1")).sort_by("id").to_pydict(), expected)

    def test_fast_forward_retries_partial_base_schema_copy(self):
        self._assert_fast_forward_retries_partial_schema_copy(0)

    def test_fast_forward_retries_partial_new_schema_copy(self):
        self._assert_fast_forward_retries_partial_schema_copy(1)

    def test_fast_forward_retries_partial_divergent_schema_copy(self):
        self._assert_fast_forward_retries_partial_schema_copy(1, divergent_main=True)

    def _assert_fast_forward_retries_partial_schema_copy(self, schema_id, divergent_main=False):
        table = self.catalog.get_table(self.identifier)
        table.create_tag("base")
        self.catalog.create_branch(self.identifier, "b1", tag_name="base")
        branch = self._fresh_table("b1")
        if schema_id == 1:
            self.catalog.alter_table(
                branch.identifier, [SchemaChange.add_column("branch_col", AtomicType("STRING"))])
            branch = self._fresh_table("b1")
        if divergent_main:
            self.catalog.alter_table(
                self.identifier, [SchemaChange.add_column("main_col", AtomicType("STRING"))])
            table = self._fresh_table()
        branch_row = {"id": [4], "value": ["branch"]}
        main_row = {"id": [5], "value": ["main"]}
        if schema_id == 1:
            branch_row["branch_col"] = ["added"]
        if divergent_main:
            main_row["main_col"] = ["old"]
        self._write(branch, pa.table(branch_row))
        self._write(table, pa.table(main_row))
        expected = self._read(branch).sort_by("id").to_pydict()
        schema_path = table.schema_manager._to_schema_path(schema_id)
        copy_file = self.catalog.file_io.copy_file

        def fail_copy(source, target, overwrite=False):
            if os.path.normpath(target) == os.path.normpath(schema_path):
                self.catalog.file_io.write_file(target, "{", overwrite=True)
                raise OSError("Interrupted schema copy")
            return copy_file(source, target, overwrite)

        with patch.object(self.catalog.file_io, "copy_file", side_effect=fail_copy):
            with self.assertRaises(RuntimeError) as cm:
                self.catalog.fast_forward(self.identifier, "b1")
        self.assertIsInstance(cm.exception.__cause__, OSError)
        with self.assertRaises(RuntimeError):
            self._fresh_table()

        catalog = CatalogFactory.create({"warehouse": os.path.join(self.temp_dir, "warehouse")})
        catalog.fast_forward(self.identifier, "b1")

        main = self._fresh_table()
        self.assertEqual(self._read(main).sort_by("id").to_pydict(), expected)
        self.assertEqual(main.schema(), branch.schema())
        next_row = dict(branch_row, id=[6], value=["after"])
        self._write(main, pa.table(next_row))
        self.assertEqual(
            self._read(self._fresh_table()).sort_by("id").to_pydict(),
            {key: values + next_row[key] for key, values in expected.items()})
        self.assertEqual(self._read(self._fresh_table("b1")).sort_by("id").to_pydict(), expected)

    def test_fast_forward_retries_partial_tag_copy(self):
        table = self.catalog.get_table(self.identifier)
        table.create_tag("base")
        self.catalog.create_branch(self.identifier, "b1", tag_name="base")
        branch = self._fresh_table("b1")
        self._write(branch, pa.table({"id": [4], "value": ["branch"]}))
        self._write(table, pa.table({"id": [5], "value": ["main"]}))
        tag_path = table.tag_manager().tag_path("base")
        copy_file = self.catalog.file_io.copy_file

        def fail_copy(source, target, overwrite=False):
            if os.path.normpath(target) == os.path.normpath(tag_path):
                self.catalog.file_io.write_file(target, "{", overwrite=True)
                raise OSError("Interrupted tag copy")
            return copy_file(source, target, overwrite)

        with patch.object(self.catalog.file_io, "copy_file", side_effect=fail_copy):
            with self.assertRaises(RuntimeError) as cm:
                self.catalog.fast_forward(self.identifier, "b1")
        self.assertIsInstance(cm.exception.__cause__, OSError)
        catalog = CatalogFactory.create({"warehouse": os.path.join(self.temp_dir, "warehouse")})
        catalog.fast_forward(self.identifier, "b1")

        main = self._fresh_table()
        self.assertEqual(main.tag_manager().get_or_throw("base"), branch.tag_manager().get_or_throw("base"))
        self.assertEqual(
            self._read(main.copy({"scan.tag-name": "base"})).to_pydict(),
            {"id": [1, 2, 3], "value": ["a", "b", "c"]})
        self.assertEqual(
            self._read(main).sort_by("id").to_pydict(),
            self._read(self._fresh_table("b1")).sort_by("id").to_pydict())

    def test_fast_forward_reports_failed_metadata_deletion(self):
        table = self.catalog.get_table(self.identifier)
        table.create_tag("base")
        self.catalog.create_branch(self.identifier, "b1", tag_name="base")
        for value in (4, 5):
            self._write(table, pa.table({"id": [value], "value": ["main"]}))
        snapshot_path = table.snapshot_manager().get_snapshot_path(3)
        delete = table.file_io.delete

        def fail_delete(path, recursive=False):
            if os.path.normpath(path) == os.path.normpath(snapshot_path):
                return False
            return delete(path, recursive)

        with patch.object(table.file_io, "delete", side_effect=fail_delete):
            with self.assertRaises(RuntimeError) as cm:
                self.catalog.fast_forward(self.identifier, "b1")
        self.assertIsInstance(cm.exception.__cause__, OSError)
        self.assertIn("Failed to delete metadata file", str(cm.exception.__cause__))

        self.catalog.fast_forward(self.identifier, "b1")

        main = self._fresh_table()
        self.assertEqual(main.snapshot_manager().get_latest_snapshot().id, 1)
        self.assertEqual(self._read(main).to_pydict(), {"id": [1, 2, 3], "value": ["a", "b", "c"]})

    def test_fast_forward_rejects_incomplete_branch_listing(self):
        table = self.catalog.get_table(self.identifier)
        table.create_tag("base")
        self.catalog.create_branch(self.identifier, "b1", tag_name="base")
        branch = self._fresh_table("b1")
        self._write(table, pa.table({"id": [4], "value": ["main"]}))
        expected_snapshot = table.snapshot_manager().get_latest_snapshot()
        list_status = table.file_io.list_status

        for directory in (branch.snapshot_manager().snapshot_dir, branch.schema_manager.schema_path):
            with self.subTest(directory=directory):
                def incomplete_listing(path):
                    return [] if path == directory else list_status(path)

                with patch.object(table.file_io, "list_status", side_effect=incomplete_listing):
                    with self.assertRaises(RuntimeError) as cm:
                        self.catalog.fast_forward(self.identifier, "b1")
                self.assertIn("Incomplete snapshot or schema metadata", str(cm.exception.__cause__))
                main = self._fresh_table()
                self.assertEqual(main.snapshot_manager().get_latest_snapshot(), expected_snapshot)
                self.assertEqual(
                    self._read(main).sort_by("id").to_pydict(),
                    {"id": [1, 2, 3, 4], "value": ["a", "b", "c", "main"]})

    def _prepare_cached_fast_forward(self, disk):
        options = {"warehouse": os.path.join(self.temp_dir, "warehouse"),
                   "local-cache.enabled": "true", "local-cache.max-size": "1 mb",
                   "local-cache.block-size": "64 b"}
        if disk:
            options["local-cache.dir"] = os.path.join(self.temp_dir, "cache")
        catalog = CatalogFactory.create(options)
        main = catalog.get_table(self.identifier)
        main.create_tag("base")
        catalog.create_branch(self.identifier, "b1", tag_name="base")
        branch_id = self._fresh_table("b1").identifier
        catalog.alter_table(branch_id, [SchemaChange.add_column("branch_col", AtomicType("STRING"))])
        branch = catalog.get_table(branch_id)
        self._write(branch, pa.table({"id": [4], "value": ["branch"], "branch_col": ["added"]}))
        catalog.alter_table(self.identifier, [SchemaChange.add_column("main_col", AtomicType("STRING"))])
        self._write(catalog.get_table(self.identifier), pa.table({
            "id": [5], "value": ["main"], "main_col": ["old"]}))
        self._read(catalog.get_table(self.identifier))
        catalog.alter_table(self.identifier, [SchemaChange.add_column("main_extra", AtomicType("INT"))])
        self._write(catalog.get_table(self.identifier), pa.table({
            "id": [6], "value": ["main"], "main_col": ["old"],
            "main_extra": pa.array([1], type=pa.int32())}))
        self._read(catalog.get_table(self.identifier))
        return catalog, options, self._read(branch).sort_by("id").to_pydict()

    def test_fast_forward_with_memory_cache(self):
        self._assert_fast_forward_with_cache(False)

    def test_fast_forward_with_disk_cache(self):
        self._assert_fast_forward_with_cache(True)

    def _assert_fast_forward_with_cache(self, disk):
        catalog, options, expected = self._prepare_cached_fast_forward(disk)
        other_id = Identifier.from_string("default.other")
        catalog.create_table(other_id, Schema.from_pyarrow_schema(self.pa_schema), False)
        other = catalog.get_table(other_id)
        self._write(other, pa.table({"id": [99], "value": ["other"]}))
        other_snapshot = other.snapshot_manager().get_snapshot_path(1)
        # Warm the FileIO cache independently of the scan backend.
        catalog.file_io.read_file_utf8(other_snapshot)
        other_block = catalog._cache_manager.get_block(other_snapshot, 0)
        self.assertIsNotNone(other_block)

        catalog.fast_forward(self.identifier, "b1")

        for current in (catalog, CatalogFactory.create(options)):
            self.assertEqual(
                self._read(current.get_table(self.identifier)).sort_by("id").to_pydict(), expected)
        self.assertEqual(other_block, catalog._cache_manager.get_block(other_snapshot, 0))
        self.assertEqual(self._read(catalog.get_table(other_id)).to_pydict(),
                         {"id": [99], "value": ["other"]})
        # Reuse the schema and snapshot IDs deleted by fast_forward.
        catalog.alter_table(self.identifier, [SchemaChange.add_column("after_col", AtomicType("STRING"))])
        main = catalog.get_table(self.identifier)
        self.assertEqual(["id", "value", "branch_col", "after_col"], main.field_names)
        self._write(main, pa.table({"id": [7], "value": ["after"],
                                   "branch_col": ["next"], "after_col": ["new"]}))
        expected_after = {key: list(values) for key, values in expected.items()}
        for key, value in (("id", 7), ("value", "after"), ("branch_col", "next")):
            expected_after[key].append(value)
        expected_after["after_col"] = [None] * 4 + ["new"]
        for current in (catalog, CatalogFactory.create(options)):
            self.assertEqual(
                self._read(current.get_table(self.identifier)).sort_by("id").to_pydict(), expected_after)
        self.assertEqual(self._read(self._fresh_table("b1")).sort_by("id").to_pydict(), expected)

    def test_fast_forward_retries_cached_memory_snapshot(self):
        self._assert_cached_fast_forward_retry(False, "snapshot")

    def test_fast_forward_retries_cached_disk_snapshot(self):
        self._assert_cached_fast_forward_retry(True, "snapshot")

    def test_fast_forward_retries_cached_memory_schema(self):
        self._assert_cached_fast_forward_retry(False, "schema")

    def test_fast_forward_retries_cached_disk_schema(self):
        self._assert_cached_fast_forward_retry(True, "schema")

    def _assert_cached_fast_forward_retry(self, disk, metadata):
        catalog, options, expected = self._prepare_cached_fast_forward(disk)
        main = catalog.get_table(self.identifier)
        target_path = (main.snapshot_manager().get_snapshot_path(2) if metadata == "snapshot"
                       else main.schema_manager._to_schema_path(1))
        copy_file = catalog.file_io.copy_file

        def fail_copy(source, target, overwrite=False):
            if os.path.normpath(target) == os.path.normpath(target_path):
                catalog.file_io.write_file(target, "{", overwrite=True)
                raise OSError("Interrupted metadata copy")
            return copy_file(source, target, overwrite)

        with patch.object(catalog.file_io, "copy_file", side_effect=fail_copy):
            with self.assertRaises(RuntimeError) as cm:
                catalog.fast_forward(self.identifier, "b1")
        self.assertIsInstance(cm.exception.__cause__, OSError)
        # Read the partial file, caching its shorter length, before retrying.
        self.assertEqual("{", catalog.file_io.read_file_utf8(target_path))
        retry = CatalogFactory.create(options) if disk else catalog
        retry.fast_forward(self.identifier, "b1")
        self.assertEqual(self._read(retry.get_table(self.identifier)).sort_by("id").to_pydict(), expected)

    def test_fast_forward_cache_failure_preserves_main(self):
        catalog, _, _ = self._prepare_cached_fast_forward(True)
        main = catalog.get_table(self.identifier)
        before = self._read(main).sort_by("id").to_pydict()
        snapshot_path = main.snapshot_manager().get_snapshot_path(2)
        cache_path = catalog._cache_manager._cache_path(snapshot_path, 0)
        unlink = os.unlink

        def fail_unlink(path, *args, **kwargs):
            if path == cache_path:
                raise PermissionError("Cannot remove cached snapshot")
            return unlink(path, *args, **kwargs)

        with patch("os.unlink", side_effect=fail_unlink):
            with self.assertRaises(RuntimeError) as cm:
                catalog.fast_forward(self.identifier, "b1")
        self.assertIsInstance(cm.exception.__cause__, PermissionError)
        self.assertEqual(self._read(self._fresh_table()).sort_by("id").to_pydict(), before)


if __name__ == "__main__":
    unittest.main()
