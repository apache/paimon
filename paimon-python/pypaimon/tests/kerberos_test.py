# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from pypaimon.common.options import Options
from pypaimon.common.options.config import SecurityOptions


class SecurityOptionsTest(unittest.TestCase):

    def test_parse_principal_and_keytab(self):
        opts = Options({
            "security.kerberos.login.principal": "user@REALM",
            "security.kerberos.login.keytab": "/path/to/user.keytab",
        })
        self.assertEqual(opts.get(SecurityOptions.KERBEROS_PRINCIPAL), "user@REALM")
        self.assertEqual(opts.get(SecurityOptions.KERBEROS_KEYTAB), "/path/to/user.keytab")

    def test_use_ticket_cache_default_true(self):
        opts = Options({})
        self.assertTrue(opts.get(SecurityOptions.KERBEROS_USE_TICKET_CACHE))

    def test_use_ticket_cache_explicit_false(self):
        opts = Options({"security.kerberos.login.use-ticket-cache": "false"})
        self.assertFalse(opts.get(SecurityOptions.KERBEROS_USE_TICKET_CACHE))

    def test_principal_and_keytab_default_none(self):
        opts = Options({})
        self.assertIsNone(opts.get(SecurityOptions.KERBEROS_PRINCIPAL))
        self.assertIsNone(opts.get(SecurityOptions.KERBEROS_KEYTAB))


class KerberosHdfsTest(unittest.TestCase):

    @patch("pypaimon.filesystem.pyarrow_file_io.subprocess.run")
    @patch("pypaimon.filesystem.pyarrow_file_io.pafs.HadoopFileSystem")
    def test_hdfs_with_keytab_calls_kinit(self, mock_hdfs_fs, mock_subprocess_run):
        mock_subprocess_run.return_value = MagicMock(stdout="/some/classpath")

        with tempfile.NamedTemporaryFile(suffix=".keytab") as keytab_file:
            from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO

            with patch.dict(os.environ, {
                "HADOOP_HOME": "/opt/hadoop",
                "HADOOP_CONF_DIR": "/opt/hadoop/etc/hadoop",
                "KRB5CCNAME": "/tmp/krb5cc_test",
            }):
                opts = Options({
                    "security.kerberos.login.principal": "hdfs/nn@REALM",
                    "security.kerberos.login.keytab": keytab_file.name,
                })

                with patch.object(PyArrowFileIO, '__init__', lambda self, *a, **kw: None):
                    file_io = PyArrowFileIO.__new__(PyArrowFileIO)
                    file_io.properties = opts
                    file_io.logger = MagicMock()

                    file_io._initialize_hdfs_fs("hdfs", "namenode:8020")

            kinit_calls = [
                c for c in mock_subprocess_run.call_args_list
                if c[0][0][0] == 'kinit'
            ]
            self.assertEqual(len(kinit_calls), 1)
            self.assertEqual(kinit_calls[0][0][0], ['kinit', '-kt', keytab_file.name, 'hdfs/nn@REALM'])

            hdfs_kwargs = mock_hdfs_fs.call_args[1]
            self.assertEqual(hdfs_kwargs["host"], "namenode")
            self.assertEqual(hdfs_kwargs["port"], 8020)
            self.assertEqual(hdfs_kwargs["kerb_ticket"], "/tmp/krb5cc_test")
            self.assertNotIn("user", hdfs_kwargs)

    @patch("pypaimon.filesystem.pyarrow_file_io.subprocess.run")
    @patch("pypaimon.filesystem.pyarrow_file_io.pafs.HadoopFileSystem")
    def test_hdfs_with_ticket_cache(self, mock_hdfs_fs, mock_subprocess_run):
        mock_subprocess_run.return_value = MagicMock(stdout="/some/classpath")

        from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO

        with patch.dict(os.environ, {
            "HADOOP_HOME": "/opt/hadoop",
            "HADOOP_CONF_DIR": "/opt/hadoop/etc/hadoop",
            "KRB5CCNAME": "/tmp/krb5cc_existing",
        }):
            with patch("os.path.exists", return_value=True):
                opts = Options({})

                with patch.object(PyArrowFileIO, '__init__', lambda self, *a, **kw: None):
                    file_io = PyArrowFileIO.__new__(PyArrowFileIO)
                    file_io.properties = opts
                    file_io.logger = MagicMock()

                    file_io._initialize_hdfs_fs("hdfs", "namenode:8020")

        hdfs_kwargs = mock_hdfs_fs.call_args[1]
        self.assertEqual(hdfs_kwargs["kerb_ticket"], "/tmp/krb5cc_existing")
        self.assertNotIn("user", hdfs_kwargs)

    @patch("pypaimon.filesystem.pyarrow_file_io.subprocess.run")
    @patch("pypaimon.filesystem.pyarrow_file_io.pafs.HadoopFileSystem")
    def test_hdfs_without_kerberos_uses_simple_auth(self, mock_hdfs_fs, mock_subprocess_run):
        mock_subprocess_run.return_value = MagicMock(stdout="/some/classpath")

        from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO

        env = {
            "HADOOP_HOME": "/opt/hadoop",
            "HADOOP_CONF_DIR": "/opt/hadoop/etc/hadoop",
            "HADOOP_USER_NAME": "testuser",
        }
        env_remove = {k: v for k, v in os.environ.items()}
        env_remove.pop("KRB5CCNAME", None)
        env_remove.update(env)

        with patch.dict(os.environ, env_remove, clear=True):
            with patch("os.path.exists", return_value=False):
                opts = Options({"security.kerberos.login.use-ticket-cache": "false"})

                with patch.object(PyArrowFileIO, '__init__', lambda self, *a, **kw: None):
                    file_io = PyArrowFileIO.__new__(PyArrowFileIO)
                    file_io.properties = opts
                    file_io.logger = MagicMock()

                    file_io._initialize_hdfs_fs("hdfs", "namenode:8020")

        hdfs_kwargs = mock_hdfs_fs.call_args[1]
        self.assertEqual(hdfs_kwargs["user"], "testuser")
        self.assertNotIn("kerb_ticket", hdfs_kwargs)

    def test_keytab_not_found_raises_error(self):
        from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO
        with self.assertRaises(FileNotFoundError):
            PyArrowFileIO._kerberos_login_from_keytab("user@REALM", "/nonexistent/path.keytab")

    @patch("pypaimon.filesystem.pyarrow_file_io.subprocess.run")
    def test_principal_without_keytab_raises_error(self, mock_subprocess_run):
        mock_subprocess_run.return_value = MagicMock(stdout="/some/classpath")

        from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO

        with patch.dict(os.environ, {
            "HADOOP_HOME": "/opt/hadoop",
            "HADOOP_CONF_DIR": "/opt/hadoop/etc/hadoop",
        }):
            opts = Options({
                "security.kerberos.login.principal": "user@REALM",
            })

            with patch.object(PyArrowFileIO, '__init__', lambda self, *a, **kw: None):
                file_io = PyArrowFileIO.__new__(PyArrowFileIO)
                file_io.properties = opts
                file_io.logger = MagicMock()

                with self.assertRaises(ValueError) as ctx:
                    file_io._initialize_hdfs_fs("hdfs", "namenode:8020")
                self.assertIn("must be both set or both unset", str(ctx.exception))

    @patch("pypaimon.filesystem.pyarrow_file_io.subprocess.run")
    def test_keytab_without_principal_raises_error(self, mock_subprocess_run):
        mock_subprocess_run.return_value = MagicMock(stdout="/some/classpath")

        from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO

        with tempfile.NamedTemporaryFile(suffix=".keytab") as keytab_file:
            with patch.dict(os.environ, {
                "HADOOP_HOME": "/opt/hadoop",
                "HADOOP_CONF_DIR": "/opt/hadoop/etc/hadoop",
            }):
                opts = Options({
                    "security.kerberos.login.keytab": keytab_file.name,
                })

                with patch.object(PyArrowFileIO, '__init__', lambda self, *a, **kw: None):
                    file_io = PyArrowFileIO.__new__(PyArrowFileIO)
                    file_io.properties = opts
                    file_io.logger = MagicMock()

                    with self.assertRaises(ValueError) as ctx:
                        file_io._initialize_hdfs_fs("hdfs", "namenode:8020")
                    self.assertIn("must be both set or both unset", str(ctx.exception))

    def test_get_ticket_cache_from_krb5ccname(self):
        from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO

        with patch.dict(os.environ, {"KRB5CCNAME": "/tmp/custom_cc"}):
            path = PyArrowFileIO._get_ticket_cache_path()
            self.assertEqual(path, "/tmp/custom_cc")

    def test_get_ticket_cache_from_krb5ccname_file_prefix(self):
        from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO

        with patch.dict(os.environ, {"KRB5CCNAME": "FILE:/tmp/custom_cc"}):
            path = PyArrowFileIO._get_ticket_cache_path()
            self.assertEqual(path, "/tmp/custom_cc")

    def test_get_ticket_cache_default_path(self):
        from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO

        env = dict(os.environ)
        env.pop("KRB5CCNAME", None)
        with patch.dict(os.environ, env, clear=True):
            with patch("os.path.exists", return_value=True):
                with patch("os.getuid", return_value=1000):
                    path = PyArrowFileIO._get_ticket_cache_path()
                    self.assertEqual(path, "/tmp/krb5cc_1000")

    def test_get_ticket_cache_no_cache(self):
        from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO

        env = dict(os.environ)
        env.pop("KRB5CCNAME", None)
        with patch.dict(os.environ, env, clear=True):
            with patch("os.path.exists", return_value=False):
                path = PyArrowFileIO._get_ticket_cache_path()
                self.assertIsNone(path)

    @patch("pypaimon.filesystem.pyarrow_file_io.subprocess.run")
    @patch("pypaimon.filesystem.pyarrow_file_io.pafs.HadoopFileSystem")
    def test_hdfs_with_fallback_keys(self, mock_hdfs_fs, mock_subprocess_run):
        """Verify that Java-compatible fallback keys security.principal / security.keytab work."""
        mock_subprocess_run.return_value = MagicMock(stdout="/some/classpath")

        with tempfile.NamedTemporaryFile(suffix=".keytab") as keytab_file:
            from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO

            with patch.dict(os.environ, {
                "HADOOP_HOME": "/opt/hadoop",
                "HADOOP_CONF_DIR": "/opt/hadoop/etc/hadoop",
                "KRB5CCNAME": "/tmp/krb5cc_test",
            }):
                opts = Options({
                    "security.principal": "hdfs/nn@REALM",
                    "security.keytab": keytab_file.name,
                })

                with patch.object(PyArrowFileIO, '__init__', lambda self, *a, **kw: None):
                    file_io = PyArrowFileIO.__new__(PyArrowFileIO)
                    file_io.properties = opts
                    file_io.logger = MagicMock()

                    file_io._initialize_hdfs_fs("hdfs", "namenode:8020")

            kinit_calls = [
                c for c in mock_subprocess_run.call_args_list
                if c[0][0][0] == 'kinit'
            ]
            self.assertEqual(len(kinit_calls), 1)
            self.assertEqual(kinit_calls[0][0][0],
                             ['kinit', '-kt', keytab_file.name, 'hdfs/nn@REALM'])

            hdfs_kwargs = mock_hdfs_fs.call_args[1]
            self.assertEqual(hdfs_kwargs["kerb_ticket"], "/tmp/krb5cc_test")
            self.assertNotIn("user", hdfs_kwargs)

    def test_keytab_not_readable_raises_error(self):
        from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO

        with tempfile.NamedTemporaryFile(suffix=".keytab") as keytab_file:
            with patch("os.access", return_value=False):
                with self.assertRaises(PermissionError) as ctx:
                    PyArrowFileIO._kerberos_login_from_keytab("user@REALM", keytab_file.name)
                self.assertIn("not readable", str(ctx.exception))

    @patch("pypaimon.filesystem.pyarrow_file_io.subprocess.run")
    def test_kinit_success_but_no_cache_raises_error(self, mock_subprocess_run):
        mock_subprocess_run.return_value = MagicMock(stdout="/some/classpath")

        from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO

        with tempfile.NamedTemporaryFile(suffix=".keytab") as keytab_file:
            with patch.dict(os.environ, {
                "HADOOP_HOME": "/opt/hadoop",
                "HADOOP_CONF_DIR": "/opt/hadoop/etc/hadoop",
            }):
                opts = Options({
                    "security.kerberos.login.principal": "user@REALM",
                    "security.kerberos.login.keytab": keytab_file.name,
                })

                with patch.object(PyArrowFileIO, '__init__',
                                  lambda self, *a, **kw: None):
                    file_io = PyArrowFileIO.__new__(PyArrowFileIO)
                    file_io.properties = opts
                    file_io.logger = MagicMock()

                    with patch.object(PyArrowFileIO,
                                      '_get_ticket_cache_path',
                                      return_value=None):
                        with self.assertRaises(RuntimeError) as ctx:
                            file_io._initialize_hdfs_fs("hdfs", "namenode:8020")
                        self.assertIn("no ticket cache path",
                                      str(ctx.exception))


if __name__ == '__main__':
    unittest.main()
