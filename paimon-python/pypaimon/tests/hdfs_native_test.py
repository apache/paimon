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

import os
import sys
import tempfile
import types
import unittest
from unittest.mock import MagicMock, patch

import pyarrow.fs as pafs

from pypaimon.common.file_io import FileIO
from pypaimon.common.options import Options
from pypaimon.common.options.config import HdfsOptions


def _install_fake_hdfs_native():
    """Install a fake hdfs_native module (with .fsspec submodule) into
    sys.modules.

    Returns (fake_module, mock_client_cls, mock_write_options_cls).
    """
    fake = types.ModuleType("hdfs_native")
    fake.Client = MagicMock(name="Client")
    fake.WriteOptions = MagicMock(name="WriteOptions")
    fsspec_mod = types.ModuleType("hdfs_native.fsspec")
    fsspec_mod.HdfsFileSystem = MagicMock(name="HdfsFileSystem")
    fsspec_mod.ViewfsFileSystem = MagicMock(name="ViewfsFileSystem")
    fake.fsspec = fsspec_mod
    sys.modules["hdfs_native"] = fake
    sys.modules["hdfs_native.fsspec"] = fsspec_mod
    return fake, fake.Client, fake.WriteOptions


def _uninstall_fake_hdfs_native():
    sys.modules.pop("hdfs_native", None)
    sys.modules.pop("hdfs_native.fsspec", None)
    # Also drop the cached HdfsNativeFileIO so a re-import sees the new fake
    sys.modules.pop(
        "pypaimon.filesystem.hdfs_native_file_io", None)


class HdfsOptionsTest(unittest.TestCase):

    def test_defaults(self):
        opts = Options({})
        self.assertEqual(opts.get(HdfsOptions.HDFS_CLIENT_IMPL), "native")
        self.assertTrue(opts.get(HdfsOptions.HDFS_CLIENT_FALLBACK_TO_PYARROW))
        self.assertIsNone(opts.get(HdfsOptions.HDFS_CONF_DIR))

    def test_explicit_pyarrow(self):
        opts = Options({"hdfs.client.impl": "pyarrow"})
        self.assertEqual(opts.get(HdfsOptions.HDFS_CLIENT_IMPL), "pyarrow")

    def test_explicit_fallback_false(self):
        opts = Options({"hdfs.client.fallback-to-pyarrow": "false"})
        self.assertFalse(opts.get(HdfsOptions.HDFS_CLIENT_FALLBACK_TO_PYARROW))


class HdfsNativeFileIORoutingTest(unittest.TestCase):

    def setUp(self):
        _uninstall_fake_hdfs_native()

    def tearDown(self):
        _uninstall_fake_hdfs_native()

    def test_local_paths_unaffected(self):
        fio = FileIO.get("file:///tmp/foo")
        self.assertEqual(type(fio).__name__, "LocalFileIO")

    def test_default_hdfs_routes_to_native(self):
        _install_fake_hdfs_native()
        fio = FileIO.get("hdfs://ns/foo", Options({}))
        self.assertEqual(type(fio).__name__, "HdfsNativeFileIO")

    def test_explicit_pyarrow_routes_to_pyarrow(self):
        # No hdfs-native module needed; should go straight to pyarrow.
        with patch(
            "pypaimon.filesystem.pyarrow_file_io.PyArrowFileIO.__init__",
            return_value=None,
        ):
            fio = FileIO.get(
                "hdfs://ns/foo",
                Options({"hdfs.client.impl": "pyarrow"}),
            )
        self.assertEqual(type(fio).__name__, "PyArrowFileIO")

    def test_native_init_failure_falls_back_to_pyarrow(self):
        # hdfs_native not installed; default fallback enabled.
        _uninstall_fake_hdfs_native()
        with patch(
            "pypaimon.filesystem.pyarrow_file_io.PyArrowFileIO.__init__",
            return_value=None,
        ):
            fio = FileIO.get("hdfs://ns/foo", Options({}))
        self.assertEqual(type(fio).__name__, "PyArrowFileIO")

    def test_native_init_failure_no_fallback_raises(self):
        _uninstall_fake_hdfs_native()
        with self.assertRaises(ImportError):
            FileIO.get(
                "hdfs://ns/foo",
                Options({"hdfs.client.fallback-to-pyarrow": "false"}),
            )

    def test_unsupported_impl_raises(self):
        with self.assertRaises(ValueError) as ctx:
            FileIO.get(
                "hdfs://ns/foo",
                Options({"hdfs.client.impl": "bogus"}),
            )
        self.assertIn("Unsupported hdfs.client.impl", str(ctx.exception))

    def test_env_var_override_when_option_absent(self):
        _install_fake_hdfs_native()
        with patch(
            "pypaimon.filesystem.pyarrow_file_io.PyArrowFileIO.__init__",
            return_value=None,
        ):
            with patch.dict(os.environ, {"PYPAIMON_HDFS_IMPL": "pyarrow"}):
                fio = FileIO.get("hdfs://ns/foo", Options({}))
        self.assertEqual(type(fio).__name__, "PyArrowFileIO")

    def test_option_wins_over_env_var(self):
        _install_fake_hdfs_native()
        with patch.dict(os.environ, {"PYPAIMON_HDFS_IMPL": "pyarrow"}):
            fio = FileIO.get(
                "hdfs://ns/foo",
                Options({"hdfs.client.impl": "native"}),
            )
        self.assertEqual(type(fio).__name__, "HdfsNativeFileIO")

    def test_viewfs_scheme_routes_to_native(self):
        _install_fake_hdfs_native()
        fio = FileIO.get("viewfs://cluster/foo", Options({}))
        self.assertEqual(type(fio).__name__, "HdfsNativeFileIO")

    def test_empty_impl_option_treated_as_unset(self):
        # Templated configs sometimes blank the option to opt out — that
        # should fall through to the default ("native"), not raise
        # "Unsupported hdfs.client.impl ''".
        _install_fake_hdfs_native()
        fio = FileIO.get("hdfs://ns/foo", Options({"hdfs.client.impl": ""}))
        self.assertEqual(type(fio).__name__, "HdfsNativeFileIO")


class HdfsNativeFileIOInitTest(unittest.TestCase):

    def setUp(self):
        self._fake, self._client_cls, self._wo_cls = _install_fake_hdfs_native()

    def tearDown(self):
        _uninstall_fake_hdfs_native()

    def _make(self, path, props=None):
        from pypaimon.filesystem.hdfs_native_file_io import HdfsNativeFileIO
        return HdfsNativeFileIO(path, Options(props or {}))

    def test_constructs_client_with_url(self):
        self._make("hdfs://ns1/warehouse")
        self._client_cls.assert_called_once()
        _, kwargs = self._client_cls.call_args
        self.assertEqual(kwargs.get("url"), "hdfs://ns1")
        self.assertNotIn("config", kwargs)

    def test_viewfs_scheme_passes_through(self):
        self._make("viewfs://cluster1/")
        _, kwargs = self._client_cls.call_args
        self.assertEqual(kwargs.get("url"), "viewfs://cluster1")

    def test_native_hadoop_keys_forwarded_as_config(self):
        self._make("hdfs://ns1/foo", {
            "dfs.nameservices": "ns1",
            "dfs.ha.namenodes.ns1": "nn1,nn2",
            "dfs.namenode.rpc-address.ns1.nn1": "host1:8020",
            "fs.viewfs.mounttable.cluster.link./prod": "hdfs://ns1/prod",
            "warehouse": "hdfs://ns1/warehouse",  # should NOT be forwarded
        })
        _, kwargs = self._client_cls.call_args
        config = kwargs.get("config", {})
        self.assertEqual(config.get("dfs.nameservices"), "ns1")
        self.assertEqual(config.get("dfs.ha.namenodes.ns1"), "nn1,nn2")
        self.assertEqual(
            config.get("dfs.namenode.rpc-address.ns1.nn1"), "host1:8020")
        self.assertEqual(
            config.get("fs.viewfs.mounttable.cluster.link./prod"),
            "hdfs://ns1/prod")
        self.assertNotIn("warehouse", config)

    def test_namespaced_overrides_forwarded(self):
        self._make("hdfs://ns1/foo", {
            "hdfs.config.dfs.client.read.shortcircuit": "true",
        })
        _, kwargs = self._client_cls.call_args
        config = kwargs.get("config", {})
        self.assertEqual(
            config.get("dfs.client.read.shortcircuit"), "true")

    def test_conf_dir_from_option(self):
        self._make("hdfs://ns1/foo", {
            "hdfs.conf-dir": "/tmp/conf",
        })
        _, kwargs = self._client_cls.call_args
        self.assertEqual(kwargs.get("config_dir"), "/tmp/conf")

    def test_conf_dir_from_env(self):
        env = dict(os.environ)
        env["HADOOP_CONF_DIR"] = "/env/conf"
        with patch.dict(os.environ, env, clear=True):
            self._make("hdfs://ns1/foo")
        _, kwargs = self._client_cls.call_args
        self.assertEqual(kwargs.get("config_dir"), "/env/conf")

    def test_option_conf_dir_overrides_env(self):
        env = dict(os.environ)
        env["HADOOP_CONF_DIR"] = "/env/conf"
        with patch.dict(os.environ, env, clear=True):
            self._make("hdfs://ns1/foo", {"hdfs.conf-dir": "/opt/conf"})
        _, kwargs = self._client_cls.call_args
        self.assertEqual(kwargs.get("config_dir"), "/opt/conf")

    @patch("pypaimon.filesystem._kerberos.subprocess.run")
    def test_kerberos_principal_keytab_triggers_kinit(self, mock_kinit):
        mock_kinit.return_value = MagicMock()
        with tempfile.NamedTemporaryFile(suffix=".keytab") as keytab_file:
            with patch.dict(os.environ, {"KRB5CCNAME": "/tmp/kc_test"}):
                self._make("hdfs://ns1/foo", {
                    "security.kerberos.login.principal": "user@REALM",
                    "security.kerberos.login.keytab": keytab_file.name,
                })
            kinit_calls = [
                c for c in mock_kinit.call_args_list
                if c[0][0][0] == "kinit"
            ]
            self.assertEqual(len(kinit_calls), 1)
            self.assertEqual(
                kinit_calls[0][0][0],
                ["kinit", "-kt", keytab_file.name, "user@REALM"],
            )

    def test_principal_without_keytab_raises(self):
        with self.assertRaises(ValueError) as ctx:
            self._make("hdfs://ns1/foo", {
                "security.kerberos.login.principal": "user@REALM",
            })
        self.assertIn("must be both set or both unset", str(ctx.exception))

    @patch("pypaimon.filesystem._kerberos.subprocess.run")
    def test_kerberos_preserves_FILE_prefix_on_krb5ccname(self, mock_kinit):
        # If KRB5CCNAME had a `FILE:` qualifier, the rewrite after kinit
        # must keep it so GSSAPI cache-type detection isn't perturbed.
        mock_kinit.return_value = MagicMock()
        with tempfile.NamedTemporaryFile(suffix=".keytab") as keytab_file:
            with patch.dict(os.environ,
                            {"KRB5CCNAME": "FILE:/tmp/kc_test"},
                            clear=True):
                self._make("hdfs://ns1/foo", {
                    "security.kerberos.login.principal": "user@REALM",
                    "security.kerberos.login.keytab": keytab_file.name,
                })
                self.assertEqual(
                    os.environ["KRB5CCNAME"], "FILE:/tmp/kc_test")

    @patch("pypaimon.filesystem._kerberos.get_ticket_cache_path",
           return_value="/tmp/freshly_kinit_cache")
    @patch("pypaimon.filesystem._kerberos.subprocess.run")
    def test_kerberos_warns_when_overwriting_different_cache(
        self, mock_kinit, _mock_cache,
    ):
        # Multi-principal in the same process clobbers KRB5CCNAME; we warn
        # so the operator sees the race instead of silently mis-routing
        # earlier instances' RPCs.
        mock_kinit.return_value = MagicMock()
        with tempfile.NamedTemporaryFile(suffix=".keytab") as keytab_file:
            with patch.dict(os.environ,
                            {"KRB5CCNAME": "/tmp/some_other_cache"},
                            clear=True):
                with self.assertLogs(
                    "pypaimon.filesystem.hdfs_native_file_io",
                    level="WARNING",
                ) as log_ctx:
                    self._make("hdfs://ns1/foo", {
                        "security.kerberos.login.principal": "user@REALM",
                        "security.kerberos.login.keytab": keytab_file.name,
                    })
                self.assertTrue(
                    any("Overwriting process-global KRB5CCNAME" in m
                        for m in log_ctx.output),
                    log_ctx.output,
                )

    @patch("pypaimon.filesystem._kerberos.get_ticket_cache_path",
           return_value="/tmp/kc_test")
    @patch("pypaimon.filesystem._kerberos.subprocess.run")
    def test_kerberos_no_warning_when_cache_unchanged(
        self, mock_kinit, _mock_cache,
    ):
        import logging as _logging
        mock_kinit.return_value = MagicMock()
        with tempfile.NamedTemporaryFile(suffix=".keytab") as keytab_file:
            # Pre-existing value matches the kinit-resolved cache → no warn.
            with patch.dict(os.environ,
                            {"KRB5CCNAME": "/tmp/kc_test"},
                            clear=True):
                # assertNoLogs is 3.10+; patch warning explicitly so older
                # interpreters keep working too.
                logger = _logging.getLogger(
                    "pypaimon.filesystem.hdfs_native_file_io")
                with patch.object(logger, "warning") as warn:
                    self._make("hdfs://ns1/foo", {
                        "security.kerberos.login.principal": "user@REALM",
                        "security.kerberos.login.keytab": keytab_file.name,
                    })
                    warn.assert_not_called()

    def test_unsupported_scheme_raises(self):
        with self.assertRaises(ValueError):
            self._make("s3://bucket/key")


class HdfsNativeFileIOOpsTest(unittest.TestCase):

    def setUp(self):
        self._fake, self._client_cls, self._wo_cls = _install_fake_hdfs_native()
        self._mock_client = MagicMock(name="ClientInstance")
        self._client_cls.return_value = self._mock_client
        from pypaimon.filesystem.hdfs_native_file_io import HdfsNativeFileIO
        self.fio = HdfsNativeFileIO("hdfs://ns/", Options({}))

    def tearDown(self):
        _uninstall_fake_hdfs_native()

    def _file_status(self, path, isdir=False, length=0, mtime=0):
        s = MagicMock()
        s.path = path
        s.isdir = isdir
        s.length = length
        s.modification_time = mtime
        return s

    def test_exists_true(self):
        self._mock_client.get_file_info.return_value = self._file_status("/x")
        self.assertTrue(self.fio.exists("/x"))

    def test_exists_false(self):
        self._mock_client.get_file_info.side_effect = FileNotFoundError("nope")
        self.assertFalse(self.fio.exists("/missing"))

    def test_get_file_status_adapts_to_pafs_filetype(self):
        self._mock_client.get_file_info.return_value = self._file_status(
            "/x", isdir=False, length=42, mtime=1700000000000,
        )
        info = self.fio.get_file_status("/x")
        self.assertEqual(info.path, "/x")
        self.assertEqual(info.size, 42)
        self.assertEqual(info.type, pafs.FileType.File)
        self.assertIsNotNone(info.mtime)

    def test_list_status(self):
        self._mock_client.list_status.return_value = iter([
            self._file_status("/x/a", isdir=False, length=10),
            self._file_status("/x/b", isdir=True),
        ])
        infos = self.fio.list_status("/x")
        self.assertEqual(len(infos), 2)
        self.assertEqual(infos[0].type, pafs.FileType.File)
        self.assertEqual(infos[1].type, pafs.FileType.Directory)
        self.assertIsNone(infos[1].size)

    def test_delete_missing_returns_false(self):
        self._mock_client.get_file_info.side_effect = FileNotFoundError("nope")
        self.assertFalse(self.fio.delete("/missing"))
        self._mock_client.delete.assert_not_called()

    def test_delete_file(self):
        self._mock_client.get_file_info.return_value = self._file_status("/x")
        self._mock_client.delete.return_value = True
        self.assertTrue(self.fio.delete("/x"))
        self._mock_client.delete.assert_called_once_with("/x", False)

    def test_delete_nonempty_dir_without_recursive_raises(self):
        self._mock_client.get_file_info.return_value = self._file_status(
            "/x", isdir=True)
        self._mock_client.list_status.return_value = iter([
            self._file_status("/x/a")])
        with self.assertRaises(OSError):
            self.fio.delete("/x", recursive=False)

    def test_mkdirs_creates_when_missing(self):
        self._mock_client.get_file_info.side_effect = FileNotFoundError("nope")
        self.assertTrue(self.fio.mkdirs("/new"))
        self._mock_client.mkdirs.assert_called_once_with(
            "/new", create_parent=True)

    def test_mkdirs_idempotent_on_existing_dir(self):
        self._mock_client.get_file_info.return_value = self._file_status(
            "/x", isdir=True)
        self.assertTrue(self.fio.mkdirs("/x"))
        self._mock_client.mkdirs.assert_not_called()

    def test_mkdirs_existing_file_raises(self):
        self._mock_client.get_file_info.return_value = self._file_status(
            "/x", isdir=False)
        with self.assertRaises(FileExistsError):
            self.fio.mkdirs("/x")


class HdfsNativeAdaptersTest(unittest.TestCase):

    def setUp(self):
        _install_fake_hdfs_native()

    def tearDown(self):
        _uninstall_fake_hdfs_native()

    def test_writer_adapter_tracks_position_and_closes_once(self):
        from pypaimon.filesystem.hdfs_native_file_io import _HdfsWriterAdapter
        fw = MagicMock()
        fw.write.side_effect = lambda buf: len(buf)
        adapter = _HdfsWriterAdapter(fw)
        adapter.write(b"abc")
        adapter.write(b"defg")
        self.assertEqual(adapter.tell(), 7)
        adapter.close()
        adapter.close()  # idempotent
        fw.close.assert_called_once()

    def test_reader_adapter_seek_and_read(self):
        from pypaimon.filesystem.hdfs_native_file_io import _HdfsReaderAdapter
        fr = MagicMock()
        fr.tell.side_effect = [20, 30]
        fr.read.return_value = b"x" * 10
        adapter = _HdfsReaderAdapter(fr)
        self.assertEqual(adapter.seek(20), 20)
        fr.seek.assert_called_once_with(20, 0)
        data = adapter.read(10)
        self.assertEqual(data, b"x" * 10)
        fr.read.assert_called_once_with(10)
        self.assertEqual(adapter.tell(), 30)

    def test_reader_adapter_read_negative_reads_all(self):
        from pypaimon.filesystem.hdfs_native_file_io import _HdfsReaderAdapter
        fr = MagicMock()
        fr.read.return_value = b"all-content"
        adapter = _HdfsReaderAdapter(fr)
        self.assertEqual(adapter.read(), b"all-content")
        fr.read.assert_called_once_with(-1)

    def test_reader_adapter_close_releases_underlying(self):
        from pypaimon.filesystem.hdfs_native_file_io import _HdfsReaderAdapter
        fr = MagicMock()
        adapter = _HdfsReaderAdapter(fr)
        adapter.close()
        adapter.close()  # idempotent
        fr.close.assert_called_once()
        self.assertTrue(adapter.closed)


def _write_hadoop_xml(path, entries):
    """Write a minimal Hadoop-style xml file at `path`."""
    body = ['<?xml version="1.0"?>', "<configuration>"]
    for name, value in entries.items():
        body.append(
            f"  <property><name>{name}</name><value>{value}</value></property>"
        )
    body.append("</configuration>")
    with open(path, "w") as f:
        f.write("\n".join(body))


class ViewFsFallbackTest(unittest.TestCase):
    """Cover _load_hadoop_xml + _maybe_inject_viewfs_fallback polyfill."""

    def setUp(self):
        _install_fake_hdfs_native()
        from pypaimon.filesystem.hdfs_native_file_io import HdfsNativeFileIO
        self.Fio = HdfsNativeFileIO

    def tearDown(self):
        _uninstall_fake_hdfs_native()

    def test_load_hadoop_xml_merges_two_files(self):
        with tempfile.TemporaryDirectory() as d:
            _write_hadoop_xml(os.path.join(d, "core-site.xml"),
                              {"fs.defaultFS": "viewfs://c1"})
            _write_hadoop_xml(os.path.join(d, "hdfs-site.xml"),
                              {"dfs.nameservices": "ns1"})
            cfg = self.Fio._load_hadoop_xml(d)
        self.assertEqual(cfg.get("fs.defaultFS"), "viewfs://c1")
        self.assertEqual(cfg.get("dfs.nameservices"), "ns1")

    def test_load_hadoop_xml_missing_dir_returns_empty(self):
        self.assertEqual(self.Fio._load_hadoop_xml(None), {})
        self.assertEqual(self.Fio._load_hadoop_xml("/no/such/dir/xyz"), {})

    def test_load_hadoop_xml_malformed_file_skipped(self):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "core-site.xml"), "w") as f:
                f.write("<not really xml")
            _write_hadoop_xml(os.path.join(d, "hdfs-site.xml"),
                              {"dfs.nameservices": "ns1"})
            cfg = self.Fio._load_hadoop_xml(d)
        self.assertEqual(cfg, {"dfs.nameservices": "ns1"})

    def test_fallback_from_existing_link_target(self):
        overrides = {}
        xml = {
            "fs.viewfs.mounttable.c1.link./home": "hdfs://ns-prod/home",
            "fs.viewfs.mounttable.c1.link./tmp": "hdfs://ns-tmp/tmp",
        }
        self.Fio._maybe_inject_viewfs_fallback("viewfs", "c1", overrides, xml)
        self.assertEqual(
            overrides.get("fs.viewfs.mounttable.c1.linkFallback"),
            "hdfs://ns-prod/",
        )

    def test_fallback_from_nameservices_when_no_links(self):
        overrides = {}
        xml = {"dfs.nameservices": "nsA,nsB"}
        self.Fio._maybe_inject_viewfs_fallback("viewfs", "c1", overrides, xml)
        self.assertEqual(
            overrides.get("fs.viewfs.mounttable.c1.linkFallback"),
            "hdfs://nsA/",
        )

    def test_fallback_from_link_in_overrides_only(self):
        # Zero-file viewfs setup: link.* arrives via catalog options
        # (overrides), no hadoop xml present. The fallback must still be
        # derived from the merged view.
        overrides = {
            "fs.viewfs.mounttable.c1.link./home": "hdfs://ns-prod/home",
        }
        xml = {}
        self.Fio._maybe_inject_viewfs_fallback("viewfs", "c1", overrides, xml)
        self.assertEqual(
            overrides.get("fs.viewfs.mounttable.c1.linkFallback"),
            "hdfs://ns-prod/",
        )

    def test_fallback_from_nameservices_in_overrides_only(self):
        overrides = {"dfs.nameservices": "nsA,nsB"}
        xml = {}
        self.Fio._maybe_inject_viewfs_fallback("viewfs", "c1", overrides, xml)
        self.assertEqual(
            overrides.get("fs.viewfs.mounttable.c1.linkFallback"),
            "hdfs://nsA/",
        )

    def test_fallback_already_in_xml_not_overridden(self):
        overrides = {}
        xml = {
            "fs.viewfs.mounttable.c1.linkFallback": "hdfs://ns-existing/",
            "dfs.nameservices": "nsA",
        }
        self.Fio._maybe_inject_viewfs_fallback("viewfs", "c1", overrides, xml)
        self.assertNotIn("fs.viewfs.mounttable.c1.linkFallback", overrides)

    def test_fallback_already_in_overrides_kept(self):
        overrides = {"fs.viewfs.mounttable.c1.linkFallback": "hdfs://manual/"}
        xml = {"dfs.nameservices": "nsA"}
        self.Fio._maybe_inject_viewfs_fallback("viewfs", "c1", overrides, xml)
        self.assertEqual(
            overrides["fs.viewfs.mounttable.c1.linkFallback"],
            "hdfs://manual/",
        )

    def test_hdfs_scheme_does_not_inject(self):
        overrides = {}
        xml = {"dfs.nameservices": "nsA"}
        self.Fio._maybe_inject_viewfs_fallback("hdfs", "ns1", overrides, xml)
        self.assertEqual(overrides, {})

    def test_no_signal_no_inject(self):
        overrides = {}
        xml = {"unrelated.key": "v"}
        self.Fio._maybe_inject_viewfs_fallback("viewfs", "c1", overrides, xml)
        self.assertEqual(overrides, {})

    def test_init_auto_injects_for_viewfs_uri(self):
        with tempfile.TemporaryDirectory() as d:
            _write_hadoop_xml(
                os.path.join(d, "hdfs-site.xml"),
                {
                    "dfs.nameservices": "ns1",
                    "fs.viewfs.mounttable.hadoop-lt-cluster.link./home":
                        "hdfs://ns1/home",
                },
            )
            opts = Options({"hdfs.conf-dir": d})
            self.Fio("viewfs://hadoop-lt-cluster/home/x", opts)
        client_cls = sys.modules["hdfs_native"].Client
        _, kwargs = client_cls.call_args
        config = kwargs.get("config", {})
        self.assertEqual(
            config.get("fs.viewfs.mounttable.hadoop-lt-cluster.linkFallback"),
            "hdfs://ns1/",
        )


class ToFilesystemPathTest(unittest.TestCase):
    """Cover URI -> absolute-path normalisation for hdfs-native."""

    def setUp(self):
        _install_fake_hdfs_native()

    def tearDown(self):
        _uninstall_fake_hdfs_native()

    def _make(self, root):
        from pypaimon.filesystem.hdfs_native_file_io import HdfsNativeFileIO
        return HdfsNativeFileIO(root, Options({}))

    def test_viewfs_uri_same_cluster_returns_path(self):
        fio = self._make("viewfs://cluster1/")
        self.assertEqual(
            fio.to_filesystem_path("viewfs://cluster1/home/hudi/x"),
            "/home/hudi/x",
        )

    def test_viewfs_uri_no_path_returns_root(self):
        fio = self._make("viewfs://cluster1/")
        self.assertEqual(fio.to_filesystem_path("viewfs://cluster1"), "/")

    def test_viewfs_absolute_path_unchanged(self):
        fio = self._make("viewfs://cluster1/")
        self.assertEqual(fio.to_filesystem_path("/foo/bar"), "/foo/bar")

    def test_hdfs_uri_same_ns_returns_path(self):
        fio = self._make("hdfs://ns1/")
        self.assertEqual(
            fio.to_filesystem_path("hdfs://ns1/foo/bar"),
            "/foo/bar",
        )

    def test_hdfs_uri_different_ns_unchanged(self):
        fio = self._make("hdfs://ns1/")
        self.assertEqual(
            fio.to_filesystem_path("hdfs://nsX/foo"),
            "hdfs://nsX/foo",
        )

    def test_hdfs_client_with_viewfs_uri_unchanged(self):
        fio = self._make("hdfs://ns1/")
        # Different scheme; let hdfs-native error rather than silently rewrite.
        self.assertEqual(
            fio.to_filesystem_path("viewfs://cluster1/foo"),
            "viewfs://cluster1/foo",
        )

    def test_exists_passes_path_only_to_client(self):
        from pypaimon.filesystem.hdfs_native_file_io import HdfsNativeFileIO
        fio = HdfsNativeFileIO("viewfs://cluster1/", Options({}))
        client = sys.modules["hdfs_native"].Client.return_value
        client.get_file_info.return_value = MagicMock(
            path="/home/hudi/x", isdir=False, length=0, modification_time=0)
        fio.exists("viewfs://cluster1/home/hudi/x")
        client.get_file_info.assert_called_once_with("/home/hudi/x")


class FilesystemPropertyTest(unittest.TestCase):
    """Cover the lazy pyarrow.fs facade backed by hdfs_native.fsspec."""

    def setUp(self):
        _install_fake_hdfs_native()
        self._patcher = patch("pyarrow.fs.PyFileSystem")
        self._handler_patcher = patch("pyarrow.fs.FSSpecHandler")
        self.MockPyFs = self._patcher.start()
        self.MockHandler = self._handler_patcher.start()

    def tearDown(self):
        self._patcher.stop()
        self._handler_patcher.stop()
        _uninstall_fake_hdfs_native()

    def _make(self, root, props=None, xml_entries=None):
        from pypaimon.filesystem.hdfs_native_file_io import HdfsNativeFileIO
        if xml_entries:
            d = tempfile.mkdtemp()
            self.addCleanup(lambda: __import__("shutil").rmtree(d, ignore_errors=True))
            _write_hadoop_xml(os.path.join(d, "hdfs-site.xml"), xml_entries)
            base_props = {"hdfs.conf-dir": d}
            base_props.update(props or {})
            props = base_props
        return HdfsNativeFileIO(root, Options(props or {}))

    def test_viewfs_uses_viewfs_fsspec_class(self):
        fio = self._make("viewfs://cluster1/")
        fs_instance = fio.filesystem  # trigger lazy
        VFs = sys.modules["hdfs_native.fsspec"].ViewfsFileSystem
        HFs = sys.modules["hdfs_native.fsspec"].HdfsFileSystem
        VFs.assert_called_once()
        HFs.assert_not_called()
        _, kwargs = VFs.call_args
        self.assertEqual(kwargs.get("host"), "cluster1")
        self.assertIs(fs_instance, self.MockPyFs.return_value)

    def test_hdfs_uses_hdfs_fsspec_class(self):
        self._make("hdfs://ns1/").filesystem
        HFs = sys.modules["hdfs_native.fsspec"].HdfsFileSystem
        VFs = sys.modules["hdfs_native.fsspec"].ViewfsFileSystem
        HFs.assert_called_once()
        VFs.assert_not_called()
        _, kwargs = HFs.call_args
        self.assertEqual(kwargs.get("host"), "ns1")

    def test_lazy_caches_after_first_access(self):
        fio = self._make("hdfs://ns1/")
        first = fio.filesystem
        second = fio.filesystem
        self.assertIs(first, second)
        HFs = sys.modules["hdfs_native.fsspec"].HdfsFileSystem
        self.assertEqual(HFs.call_count, 1)

    def test_xml_and_catalog_options_merged_into_fsspec_storage_options(self):
        fio = self._make(
            "hdfs://ns1/",
            props={"dfs.client.read.shortcircuit": "true"},
            xml_entries={"dfs.nameservices": "ns1"},
        )
        fio.filesystem  # trigger
        HFs = sys.modules["hdfs_native.fsspec"].HdfsFileSystem
        _, kwargs = HFs.call_args
        # Both xml and option keys should land in the fsspec kwargs.
        self.assertEqual(kwargs.get("dfs.nameservices"), "ns1")
        self.assertEqual(kwargs.get("dfs.client.read.shortcircuit"), "true")

    def test_catalog_option_overrides_xml(self):
        fio = self._make(
            "hdfs://ns1/",
            props={"dfs.foo": "v_user"},
            xml_entries={"dfs.foo": "v_xml"},
        )
        fio.filesystem
        HFs = sys.modules["hdfs_native.fsspec"].HdfsFileSystem
        _, kwargs = HFs.call_args
        self.assertEqual(kwargs.get("dfs.foo"), "v_user")

    def test_missing_fsspec_raises_clear_error(self):
        fio = self._make("hdfs://ns1/")
        # Remove the fsspec submodule but keep hdfs_native itself, to
        # simulate an old/partial install.
        sys.modules.pop("hdfs_native.fsspec", None)
        sys.modules["hdfs_native"].fsspec = None
        with self.assertRaises(RuntimeError) as ctx:
            fio.filesystem
        self.assertIn("hdfs-native fsspec adapter", str(ctx.exception))


class PickleTest(unittest.TestCase):
    """Cover __reduce__ so Ray / multiprocessing can ship FileIO."""

    def setUp(self):
        _install_fake_hdfs_native()
        # Isolate from any HADOOP_CONF_DIR on the host so __reduce__'s
        # env-derived config_dir pinning is deterministic across machines.
        self._env_patcher = patch.dict(os.environ, {}, clear=True)
        self._env_patcher.start()

    def tearDown(self):
        self._env_patcher.stop()
        _uninstall_fake_hdfs_native()

    def _make(self, path, props=None):
        from pypaimon.filesystem.hdfs_native_file_io import HdfsNativeFileIO
        return HdfsNativeFileIO(path, Options(props or {}))

    def test_reduce_returns_class_and_args(self):
        from pypaimon.filesystem.hdfs_native_file_io import HdfsNativeFileIO
        fio = self._make("viewfs://cluster1/some/sub/path",
                         {"dfs.nameservices": "ns1"})
        cls, args = fio.__reduce__()
        self.assertIs(cls, HdfsNativeFileIO)
        path, options = args
        # Path is rebuilt from scheme+netloc (path segment dropped) — that
        # is intentional because __init__ ignores path beyond scheme+netloc.
        self.assertEqual(path, "viewfs://cluster1")
        self.assertEqual(options.to_map(), {"dfs.nameservices": "ns1"})

    def test_reduce_for_empty_netloc(self):
        fio = self._make("hdfs://")
        _, (path, _) = fio.__reduce__()
        self.assertEqual(path, "hdfs://")

    def test_reduce_pins_env_resolved_config_dir_into_options(self):
        # config_dir resolved from $HADOOP_CONF_DIR should be carried into
        # the pickled options so a worker on a host with a different env
        # value still uses the driver's resolved directory.
        with tempfile.TemporaryDirectory() as d:
            with patch.dict(os.environ, {"HADOOP_CONF_DIR": d}, clear=True):
                fio = self._make("hdfs://ns1/foo")
            _, (_, options) = fio.__reduce__()
        self.assertEqual(options.to_map().get("hdfs.conf-dir"), d)

    def test_reduce_does_not_override_explicit_conf_dir_option(self):
        with tempfile.TemporaryDirectory() as opt_dir:
            with patch.dict(os.environ,
                            {"HADOOP_CONF_DIR": "/env/dir"}, clear=True):
                fio = self._make("hdfs://ns1/foo",
                                 {"hdfs.conf-dir": opt_dir})
            _, (_, options) = fio.__reduce__()
        self.assertEqual(options.to_map().get("hdfs.conf-dir"), opt_dir)

    def test_pickle_roundtrip_preserves_type_and_options(self):
        import pickle
        fio = self._make("hdfs://ns1/foo",
                         {"dfs.foo": "bar", "fs.viewfs.x": "y"})
        client_cls = sys.modules["hdfs_native"].Client
        client_cls.reset_mock()
        # Roundtrip via the highest pickle protocol.
        blob = pickle.dumps(fio, protocol=pickle.HIGHEST_PROTOCOL)
        restored = pickle.loads(blob)
        from pypaimon.filesystem.hdfs_native_file_io import HdfsNativeFileIO
        self.assertIsInstance(restored, HdfsNativeFileIO)
        self.assertEqual(restored.properties.to_map(),
                         {"dfs.foo": "bar", "fs.viewfs.x": "y"})
        # The original __init__ ran once; the unpickle ran __init__ again.
        self.assertEqual(client_cls.call_count, 1)

    def test_pickle_with_viewfs_scheme(self):
        import pickle
        fio = self._make("viewfs://cluster1/")
        restored = pickle.loads(pickle.dumps(fio))
        self.assertEqual(restored._scheme, "viewfs")
        self.assertEqual(restored._netloc, "cluster1")

    def test_pickle_does_not_serialise_live_client(self):
        # If the live _client were pickled, the call would fail (MagicMocks
        # are picklable but the real RawClient would not be). This test
        # documents the contract: __reduce__ MUST sidestep _client.
        import pickle
        fio = self._make("hdfs://ns1/")
        blob = pickle.dumps(fio)
        # The pickled blob should reference the constructor inputs only;
        # specifically it should not embed the literal mock _client.
        self.assertNotIn(b"_client", blob)


class HdfsNativeWriteFormatTest(unittest.TestCase):
    """HdfsNativeFileIO is the default hdfs:// backend, so it must keep the
    same write surface as the pyarrow backend it replaces — including
    lance/vortex, which otherwise fall through to FileIO's NotImplementedError.
    """

    def setUp(self):
        self._fake, self._client_cls, _ = _install_fake_hdfs_native()
        self._client_cls.return_value = MagicMock(name="ClientInstance")
        from pypaimon.filesystem.hdfs_native_file_io import HdfsNativeFileIO
        self.fio = HdfsNativeFileIO("hdfs://ns/", Options({}))

    def tearDown(self):
        _uninstall_fake_hdfs_native()

    def test_lance_and_vortex_are_overridden(self):
        # The regression this guards: with these unimplemented, an HDFS table
        # using file.format=lance/vortex would hit FileIO's NotImplementedError.
        from pypaimon.common.file_io import FileIO
        self.assertIsNot(
            type(self.fio).write_lance, FileIO.write_lance)
        self.assertIsNot(
            type(self.fio).write_vortex, FileIO.write_vortex)

    def test_write_lance_delegates_to_lance_specified(self):
        import pyarrow
        table = pyarrow.table({"a": [1, 2]})
        writer = MagicMock(name="LanceFileWriter")
        fake_lance = types.ModuleType("lance")
        fake_lance.file = types.SimpleNamespace(
            LanceFileWriter=MagicMock(return_value=writer))
        with patch.dict(sys.modules, {"lance": fake_lance}), \
                patch("pypaimon.read.reader.lance_utils.to_lance_specified",
                      return_value=("hdfs://ns/x.lance", {"opt": "v"})) as spec:
            self.fio.write_lance("hdfs://ns/x.lance", table)
        spec.assert_called_once()
        _, kwargs = fake_lance.file.LanceFileWriter.call_args
        self.assertEqual(kwargs.get("storage_options"), {"opt": "v"})
        writer.close.assert_called_once()

    def test_write_vortex_delegates_to_vortex_specified(self):
        import pyarrow
        table = pyarrow.table({"a": [1, 2]})
        fake_vortex = types.ModuleType("vortex")
        fake_vortex.array = MagicMock(return_value="varr")
        fake_vortex.store = types.SimpleNamespace(from_url=MagicMock())
        fake_io = types.ModuleType("vortex._lib.io")
        fake_io.write = MagicMock()
        fake_lib = types.ModuleType("vortex._lib")
        fake_lib.io = fake_io
        fake_modules = {
            "vortex": fake_vortex,
            "vortex._lib": fake_lib,
            "vortex._lib.io": fake_io,
        }
        with patch.dict(sys.modules, fake_modules), \
                patch("pypaimon.read.reader.vortex_utils.to_vortex_specified",
                      return_value=("hdfs://ns/x.vortex", None)):
            self.fio.write_vortex("hdfs://ns/x.vortex", table)
        fake_io.write.assert_called_once_with("varr", "hdfs://ns/x.vortex")


if __name__ == "__main__":
    unittest.main()
