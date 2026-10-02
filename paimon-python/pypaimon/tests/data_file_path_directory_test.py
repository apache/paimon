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

import pytest
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

    def test_native_plan_accepts_configured_directory(self):
        table = self._create_with_options(
            "db.t_native", {"data-file.path-directory": "data"})
        scan = table.new_read_builder().new_scan()
        with mock.patch("pypaimon.read.native_plan.native_runtime_available", return_value=True):
            self.assertTrue(scan._native_plan_supported())

    def test_native_write_dispatches_with_configured_directory(self):
        table = self._create_with_options(
            "db.t_native_write", {"data-file.path-directory": "data",
                                  "write.native.enabled": "true"})
        sentinel = object()
        with mock.patch("pypaimon.write.native_write.create_native_write", return_value=sentinel):
            self.assertIs(table.new_batch_write_builder()._native_write(), sentinel)

    def test_native_commit_dispatches_with_configured_directory(self):
        table = self._create_with_options(
            "db.t_native_commit", {"data-file.path-directory": "data",
                                   "commit.native.enabled": "true"})
        commit = table.new_batch_write_builder().new_commit()
        sentinel = object()
        try:
            with mock.patch("pypaimon.write.native_commit.native_messages_supported", return_value=True), \
                    mock.patch("pypaimon.write.native_commit.create_native_commit", return_value=sentinel), \
                    mock.patch("pypaimon.write.native_commit.to_native_commit_messages", return_value=[]):
                self.assertEqual(commit._prepare_native_commit([]), (sentinel, []))
        finally:
            # The sentinel represents a prepared committer, not a real resource.
            commit._native_commit = None
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


# Expected strings were produced by org.apache.paimon.fs.Path from this tree.
@pytest.mark.parametrize('parent,child,expected', [
    ('/warehouse/t', 'data/nested', '/warehouse/t/data/nested'),
    ('/warehouse/t/', 'data//discard/../nested/.', '/warehouse/t/data/nested'),
    ('s3://bucket/warehouse/t', '/shared/data', 's3://bucket/shared/data'),
    ('s3://bucket/t', 's3://other/data', 's3://other/data'),
    ('file:/warehouse/t', 'file:///other/data', 'file:/other/data'),
    ('memory:/t', '../other', 'memory:/other'),
    ('warehouse/t', '../data', 'warehouse/data'),
    ('warehouse/t', 'data', 'warehouse/t/data'),
    ('s3://bucket/t', '//other/data', 's3://other/data'),
    ('s3://bucket/t', 'data 100%?#/你好', 's3://bucket/t/data 100%?#/你好'),
    ('file:/t', './data', 'file:/t/data'),
    ('/', 'data', '/data'),
    ('/t', '.', '/t'),
    ('/t', '..', '/'),
    ('/t', '../../data', '/../data'),
    ('s3://bucket/', 'data', 's3://bucket/data'),
    ('s3://bucket', '.', 's3://bucket'),
    ('s3://bucket', 'x/..', 's3://bucket'),
    ('warehouse', 'a/../b:c', 'warehouse/b:c'),
    ('memory:/t', 'a/../../b', 'memory:/b'),
    ('/t', '///data', '/data'),
    ('/t', '////data', '/data'),
    ('file:/t', 'file:////data', 'file:/data'),
    ('/t', '//', '/'),
    ('file:/t', '//', 'file:/'),
    ('s3://bucket/t', '//other:9000/data', 's3://other:9000/data'),
    ('//host:8020/table', '//other/data', '//other/data'),
    ('relative', '../b:c', './b:c'),
    ('relative', '../a:bb', './a:bb'),
    ('relative', '../b:c/x', './b:c/x'),
    ('file:/t', 'file:/data', 'file:/data'),
])
@pytest.mark.parametrize('windows', [False, True])
def test_path_resolution_matches_java(monkeypatch, parent, child, expected, windows):
    import pypaimon.utils.path as paths
    monkeypatch.setattr(paths, '_WINDOWS', windows)
    assert paths.resolve_path(parent, child) == expected


def test_empty_path_is_rejected():
    from pypaimon.utils.path import resolve_path
    with pytest.raises(ValueError, match='empty string'):
        resolve_path('/warehouse/t', '')


@pytest.mark.parametrize('stored', [None, 'data'])
@pytest.mark.parametrize('requested', [None, 'data', 'other'])
def test_data_directory_cannot_change_on_table_copy(tmp_path, stored, requested):
    catalog = CatalogFactory.create({'warehouse': str(tmp_path)})
    catalog.create_database('db', True)
    options = {} if stored is None else {'data-file.path-directory': stored}
    catalog.create_table('db.t', Schema.from_pyarrow_schema(pa.schema([('id', pa.int32())]), options=options), False)
    table = catalog.get_table('db.t')
    for copy in (table.copy, table.copy_without_time_travel):
        if stored == requested:
            assert copy({'data-file.path-directory': requested}).options.data_file_path_directory() == stored
        else:
            with pytest.raises(ValueError, match='immutable option'):
                copy({'data-file.path-directory': requested})


@pytest.mark.parametrize('parent,child,expected', [
    ('relative', '../b:c', './b:c'),
    ('relative', '../a:bb', './a:bb'),
    ('relative', '../b:c/x', './b:c/x'),
    ('warehouse/t', 'data/../../../b:c', './b:c'),
    ('.', 'a/../b:c', './b:c'),
    ('s3://bucket', '.', 's3://bucket'),
    (r'C:\warehouse\table', '../data', 'C:/warehouse/data'),
    (r'C:\warehouse\table', r'..\data', 'C:/warehouse/data'),
    ('C:/warehouse/table', '/data', '/data'),
    ('C:/warehouse/table', '../..', 'C:/'),
    ('file:/C:/warehouse/table', '../..', 'file:/C:/'),
    (r'\\server\share\table', 'data', '//server/share/table/data'),
    ('s3://bucket/table', r'\\other\share', 's3://other/share'),
    ('file:/C:/warehouse/table', r'..\data', 'file:/C:/warehouse/data'),
    ('s3://bucket/t', r'data\literal', 's3://bucket/t/data/literal'),
])
def test_windows_paths_match_java(monkeypatch, parent, child, expected):
    import pypaimon.utils.path as paths
    monkeypatch.setattr(paths, '_WINDOWS', True)
    assert paths.resolve_path(parent, child) == expected


@pytest.mark.parametrize('windows,path,expected', [
    (False, 'file:/tmp/data%2Fwith space?#part', '/tmp/data%2Fwith space?#part'),
    (False, 'file:///tmp/data%20', '/tmp/data%20'),
    (False, 'file://localhost/tmp/data%20', '/tmp/data%20'),
    (False, 'file://host/share/data%20', '//host/share/data%20'),
    (True, 'file:/C:/data%20', 'C:/data%20'),
    (True, 'file://C:/data%20', 'C:/data%20'),
    (True, 'file://host/share/data%20', '//host/share/data%20'),
    (False, '/tmp/data%2F?#', '/tmp/data%2F?#'),
    (False, 's3://bucket/data%2F?#', 's3://bucket/data%2F?#'),
    (False, 'file:/tmp/plain', 'file:/tmp/plain'),
])
def test_file_io_paths_keep_literal_characters(monkeypatch, windows, path, expected):
    import pypaimon.utils.path as paths
    monkeypatch.setattr(paths, '_WINDOWS', windows)
    assert paths.to_file_io_path(path) == expected
