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

"""``data-file.path-directory`` places data files under a sub-directory."""

import os
import shutil
import tempfile
import unittest
from unittest import mock

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


class DataFilePathDirectoryTest(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="data_file_dir_")
        warehouse = os.path.join(self.tmp, "warehouse")
        self.catalog = CatalogFactory.create({"warehouse": warehouse})
        self.catalog.create_database("db", False)

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _write(self, identifier):
        table = self.catalog.get_table(identifier)
        wb = table.new_batch_write_builder()
        writer = wb.new_write()
        commit = wb.new_commit()
        writer.write_arrow(pa.table({
            "id": pa.array([1, 2, 3], type=pa.int32()),
            "v": ["a", "b", "c"],
        }))
        commit.commit(writer.prepare_commit())
        writer.close()
        commit.close()
        return table

    @staticmethod
    def _data_files(root):
        found = []
        for dirpath, _, filenames in os.walk(root):
            for name in filenames:
                if name.startswith("data-"):
                    found.append(os.path.relpath(
                        os.path.join(dirpath, name), root))
        return found

    def _create_with_options(self, identifier, options):
        self.catalog.create_table(
            identifier,
            Schema(fields=Schema.from_pyarrow_schema(pa.schema([
                ("id", pa.int32()), ("v", pa.string())])).fields,
                options=options),
            False,
        )
        return self.catalog.get_table(identifier)

    def test_data_files_written_under_configured_directory(self):
        self.catalog.create_table(
            "db.t",
            Schema(fields=Schema.from_pyarrow_schema(pa.schema([
                ("id", pa.int32()), ("v", pa.string())])).fields,
                options={"data-file.path-directory": "data"}),
            False,
        )
        table = self._write("db.t")

        data_files = self._data_files(str(table.table_path))
        self.assertTrue(data_files, "no data files were written")
        for rel in data_files:
            self.assertEqual(
                "data", rel.split(os.sep)[0],
                "data file not under the configured directory: {}".format(rel))

        # The write is readable back through the same path factory.
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan().splits()
        result = read_builder.new_read().to_arrow(splits)
        self.assertEqual(sorted(result.column("id").to_pylist()), [1, 2, 3])

    def test_native_plan_falls_back_when_directory_configured(self):
        # ``data-file.path-directory`` only resolves under Python planning;
        # the native planner still looks under the bucket root and would 404.
        # Even with scan.native-plan.enabled the scan must refuse native
        # planning and fall back to Python, so the relocated files are still
        # found. Regression for the Python/Rust Plan divergence raised in
        # review (native read failed with NotFound before the guard).
        self.catalog.create_table(
            "db.t_native",
            Schema(fields=Schema.from_pyarrow_schema(pa.schema([
                ("id", pa.int32()), ("v", pa.string())])).fields,
                options={"data-file.path-directory": "data",
                         "scan.native-plan.enabled": "true"}),
            False,
        )
        table = self._write("db.t_native")

        scan = table.new_read_builder().new_scan()
        # Force the runtime probe to succeed so the assertion isolates the
        # directory guard (not merely a missing pypaimon-rust): the gate must
        # still refuse native planning because the directory is configured.
        with mock.patch(
                "pypaimon.read.native_plan.native_runtime_available",
                return_value=True):
            self.assertFalse(scan._native_plan_supported())

        # End-to-end: a native-requested scan returns the rows through the
        # Python fallback rather than failing to locate the relocated files.
        splits = scan.plan().splits()
        result = table.new_read_builder().new_read().to_arrow(splits)
        self.assertEqual(sorted(result.column("id").to_pylist()), [1, 2, 3])

    def test_native_write_falls_back_when_directory_configured(self):
        # write.native.enabled + data-file.path-directory: the write builder
        # must return the Python writer (native probe returns None) so the
        # relocated directory is honored; the native writer writes at the root.
        # Patch the native constructor so the guard, not a missing runtime, is
        # what forces the fallback (without the guard this returns the sentinel).
        table = self._create_with_options(
            "db.t_native_write",
            {"data-file.path-directory": "data",
             "write.native.enabled": "true"})
        with mock.patch(
                "pypaimon.write.native_write.create_native_write",
                return_value=object()):
            self.assertIsNone(table.new_batch_write_builder()._native_write())

    def test_native_read_falls_back_when_directory_configured(self):
        # read.native.enabled + data-file.path-directory: the native read
        # probe short-circuits to the Python reader before loading the Rust
        # runtime, so the relocated files are resolved.
        table = self._create_with_options(
            "db.t_native_read",
            {"data-file.path-directory": "data",
             "read.native.enabled": "true"})
        self._write("db.t_native_read")
        rb = table.new_read_builder()
        splits = rb.new_scan().plan().splits()
        self.assertIsNone(rb.new_read()._try_native_batches(
            splits, pa.schema([("id", pa.int32()), ("v", pa.string())])))

    def test_native_commit_falls_back_when_directory_configured(self):
        # commit.native.enabled + data-file.path-directory: the native commit
        # probe returns None so the Python committer records the relocated
        # paths.
        table = self._create_with_options(
            "db.t_native_commit",
            {"data-file.path-directory": "data",
             "commit.native.enabled": "true"})
        commit = table.new_batch_write_builder().new_commit()
        try:
            self.assertIsNone(commit._prepare_native_commit([]))
        finally:
            commit.close()

    def test_default_keeps_data_files_at_bucket_root(self):
        self.catalog.create_table(
            "db.t_default",
            Schema(fields=Schema.from_pyarrow_schema(pa.schema([
                ("id", pa.int32()), ("v", pa.string())])).fields),
            False,
        )
        table = self._write("db.t_default")

        data_files = self._data_files(str(table.table_path))
        self.assertTrue(data_files, "no data files were written")
        for rel in data_files:
            self.assertTrue(
                rel.split(os.sep)[0].startswith("bucket-"),
                "unexpected data file location: {}".format(rel))


if __name__ == "__main__":
    unittest.main()
