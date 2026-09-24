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
