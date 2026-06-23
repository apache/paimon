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

"""Integration tests for the top-level ``tag`` CLI subcommands."""

import json
import os
import shutil
import tempfile
import unittest
from io import StringIO
from unittest.mock import patch

import pyarrow as pa

from pypaimon import CatalogFactory, Schema
from pypaimon.cli.cli import main


class CliTagTest(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix="cli_tag_")
        self.warehouse = os.path.join(self.tempdir, 'warehouse')
        self.catalog = CatalogFactory.create({'warehouse': self.warehouse})
        self.catalog.create_database('db', True)

        pa_schema = pa.schema([('id', pa.int32()), ('name', pa.string())])
        self.catalog.create_table(
            'db.t', Schema.from_pyarrow_schema(pa_schema), False)
        # Two commits so snapshot ids 1 and 2 both exist.
        table = self.catalog.get_table('db.t')
        for rows in ([{'id': 1, 'name': 'a'}], [{'id': 2, 'name': 'b'}]):
            wb = table.new_batch_write_builder()
            w = wb.new_write()
            c = wb.new_commit()
            w.write_arrow(pa.Table.from_pylist(rows, schema=pa_schema))
            c.commit(w.prepare_commit())
            w.close()
            c.close()

        self.config_file = os.path.join(self.tempdir, 'paimon.yaml')
        with open(self.config_file, 'w') as f:
            f.write("metastore: filesystem\nwarehouse: {}\n".format(
                self.warehouse))

    def tearDown(self):
        shutil.rmtree(self.tempdir, ignore_errors=True)

    def _run(self, *argv):
        """Run the CLI; return (stdout, stderr, exit_code)."""
        out, err = StringIO(), StringIO()
        code = 0
        full = ['paimon', '-c', self.config_file] + list(argv)
        with patch('sys.argv', full):
            with patch('sys.stdout', out), patch('sys.stderr', err):
                try:
                    main()
                except SystemExit as e:
                    code = 0 if e.code is None else (
                        e.code if isinstance(e.code, int) else 1)
        return out.getvalue(), err.getvalue(), code

    # -- create + list -------------------------------------------------------

    def test_create_then_list(self):
        out, _, code = self._run('tag', 'create', 'db.t', 'v1')
        self.assertEqual(0, code)
        self.assertIn("created", out)

        out, _, code = self._run('tag', 'list', 'db.t')
        self.assertEqual(0, code)
        self.assertIn("v1", out)

    def test_list_empty(self):
        out, _, code = self._run('tag', 'list', 'db.t')
        self.assertEqual(0, code)
        self.assertIn("No tags found.", out)

    def test_list_json_empty(self):
        out, _, code = self._run(
            'tag', 'list', 'db.t', '--format', 'json')
        self.assertEqual(0, code)
        self.assertEqual([], json.loads(out))

    def test_list_json(self):
        self._run('tag', 'create', 'db.t', 'v1')
        self._run('tag', 'create', 'db.t', 'v2')
        out, _, code = self._run('tag', 'list', 'db.t', '--format', 'json')
        self.assertEqual(0, code)
        self.assertEqual({"v1", "v2"}, set(json.loads(out)))

    def test_list_prefix(self):
        self._run('tag', 'create', 'db.t', 'prod_v1')
        self._run('tag', 'create', 'db.t', 'dev_v1')
        out, _, code = self._run(
            'tag', 'list', 'db.t', '--prefix', 'prod_', '-f', 'json')
        self.assertEqual(0, code)
        self.assertEqual(["prod_v1"], json.loads(out))

    # -- create options ------------------------------------------------------

    def test_create_with_snapshot_id(self):
        out, _, code = self._run(
            'tag', 'create', 'db.t', 'v1', '--snapshot-id', '1')
        self.assertEqual(0, code)
        out, _, _ = self._run('tag', 'get', 'db.t', 'v1')
        self.assertIn("Snapshot ID: 1", out)

    def test_create_duplicate_raises(self):
        self._run('tag', 'create', 'db.t', 'v1')
        out, err, code = self._run('tag', 'create', 'db.t', 'v1')
        self.assertEqual(1, code)
        self.assertIn("already exists", err)

    def test_create_duplicate_ignore_if_exists(self):
        self._run('tag', 'create', 'db.t', 'v1')
        _, _, code = self._run(
            'tag', 'create', 'db.t', 'v1', '--ignore-if-exists')
        self.assertEqual(0, code)

    # -- get -----------------------------------------------------------------

    def test_get_table_format(self):
        self._run('tag', 'create', 'db.t', 'v1', '--snapshot-id', '2')
        out, _, code = self._run('tag', 'get', 'db.t', 'v1')
        self.assertEqual(0, code)
        self.assertIn("Tag: v1", out)
        self.assertIn("Snapshot ID: 2", out)

    def test_get_json_format(self):
        self._run('tag', 'create', 'db.t', 'v1')
        out, _, code = self._run(
            'tag', 'get', 'db.t', 'v1', '--format', 'json')
        self.assertEqual(0, code)
        parsed = json.loads(out)
        self.assertEqual("v1", parsed["tagName"])

    def test_get_not_exists(self):
        _, err, code = self._run('tag', 'get', 'db.t', 'absent')
        self.assertEqual(1, code)
        self.assertIn("does not exist", err)

    # -- delete --------------------------------------------------------------

    def test_delete(self):
        self._run('tag', 'create', 'db.t', 'v1')
        out, _, code = self._run('tag', 'delete', 'db.t', 'v1')
        self.assertEqual(0, code)
        self.assertIn("deleted", out)
        out, _, _ = self._run('tag', 'list', 'db.t')
        self.assertNotIn("v1", out)

    def test_delete_not_exists(self):
        _, err, code = self._run('tag', 'delete', 'db.t', 'absent')
        self.assertEqual(1, code)
        self.assertIn("does not exist", err)

    # -- bad input -----------------------------------------------------------

    def test_invalid_identifier(self):
        _, err, code = self._run('tag', 'create', 'nodot', 'v1')
        self.assertEqual(1, code)
        self.assertIn("Invalid table identifier", err)


if __name__ == "__main__":
    unittest.main()
