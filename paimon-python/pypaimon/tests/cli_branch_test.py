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

"""Integration tests for the top-level ``branch`` CLI command."""

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


class CliBranchTest(unittest.TestCase):

    def setUp(self):
        self.tempdir = tempfile.mkdtemp(prefix="cli_branch_")
        self.warehouse = os.path.join(self.tempdir, 'warehouse')
        self.catalog = CatalogFactory.create({'warehouse': self.warehouse})
        self.catalog.create_database('db', True)

        pa_schema = pa.schema([('id', pa.int32()), ('name', pa.string())])
        self.catalog.create_table(
            'db.t', Schema.from_pyarrow_schema(pa_schema), False)
        # One commit so there is a snapshot, plus a tag for from-tag tests.
        table = self.catalog.get_table('db.t')
        wb = table.new_batch_write_builder()
        w = wb.new_write()
        c = wb.new_commit()
        w.write_arrow(pa.Table.from_pylist([{'id': 1, 'name': 'a'}],
                                           schema=pa_schema))
        c.commit(w.prepare_commit())
        w.close()
        c.close()
        table.create_tag("v1")

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
        out, _, code = self._run('branch', 'create', 'db.t', 'b1')
        self.assertEqual(0, code)
        self.assertIn("created", out)

        out, _, code = self._run('branch', 'list', 'db.t')
        self.assertEqual(0, code)
        self.assertIn("b1", out)

    def test_create_from_tag(self):
        out, _, code = self._run(
            'branch', 'create', 'db.t', 'b1', '--tag', 'v1')
        self.assertEqual(0, code)
        self.assertIn("from tag 'v1'", out)
        out, _, _ = self._run('branch', 'list', 'db.t')
        self.assertIn("b1", out)

    def test_create_from_missing_tag(self):
        _, err, code = self._run(
            'branch', 'create', 'db.t', 'b1', '--tag', 'absent')
        self.assertEqual(1, code)
        self.assertIn("does not exist", err)

    def test_create_duplicate_raises(self):
        self._run('branch', 'create', 'db.t', 'b1')
        _, err, code = self._run('branch', 'create', 'db.t', 'b1')
        self.assertEqual(1, code)
        self.assertIn("already exists", err)

    def test_list_empty(self):
        out, _, code = self._run('branch', 'list', 'db.t')
        self.assertEqual(0, code)
        self.assertIn("No branches found.", out)

    def test_list_json(self):
        self._run('branch', 'create', 'db.t', 'b1')
        self._run('branch', 'create', 'db.t', 'b2')
        out, _, code = self._run('branch', 'list', 'db.t', '--format', 'json')
        self.assertEqual(0, code)
        self.assertEqual({"b1", "b2"}, set(json.loads(out)))

    # -- delete --------------------------------------------------------------

    def test_delete(self):
        self._run('branch', 'create', 'db.t', 'b1')
        out, _, code = self._run('branch', 'delete', 'db.t', 'b1')
        self.assertEqual(0, code)
        self.assertIn("deleted", out)
        out, _, _ = self._run('branch', 'list', 'db.t')
        self.assertNotIn("b1", out)

    def test_delete_not_exists(self):
        _, err, code = self._run('branch', 'delete', 'db.t', 'absent')
        self.assertEqual(1, code)
        self.assertIn("does not exist", err)

    # -- rename --------------------------------------------------------------

    def test_rename(self):
        self._run('branch', 'create', 'db.t', 'b1')
        out, _, code = self._run('branch', 'rename', 'db.t', 'b1', 'b2')
        self.assertEqual(0, code)
        self.assertIn("renamed", out)
        out, _, _ = self._run('branch', 'list', 'db.t')
        self.assertIn("b2", out)
        self.assertNotIn("b1", out)

    def test_rename_from_missing(self):
        _, err, code = self._run('branch', 'rename', 'db.t', 'absent', 'b2')
        self.assertEqual(1, code)
        self.assertIn("does not exist", err)

    def test_rename_to_existing(self):
        self._run('branch', 'create', 'db.t', 'b1')
        self._run('branch', 'create', 'db.t', 'b2')
        _, err, code = self._run('branch', 'rename', 'db.t', 'b1', 'b2')
        self.assertEqual(1, code)
        self.assertIn("already exists", err)

    # -- fast-forward --------------------------------------------------------

    def test_fast_forward(self):
        self._run('branch', 'create', 'db.t', 'b1', '--tag', 'v1')
        out, _, code = self._run('branch', 'fast-forward', 'db.t', 'b1')
        self.assertEqual(0, code)
        self.assertIn("Fast-forwarded", out)

    def test_fast_forward_missing(self):
        _, err, code = self._run('branch', 'fast-forward', 'db.t', 'absent')
        self.assertEqual(1, code)
        self.assertIn("does not exist", err)

    # -- bad input -----------------------------------------------------------

    def test_invalid_identifier(self):
        _, err, code = self._run('branch', 'create', 'nodot', 'b1')
        self.assertEqual(1, code)
        self.assertIn("Invalid table identifier", err)


if __name__ == "__main__":
    unittest.main()
