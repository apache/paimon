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
import shutil
import subprocess
import sys
import tempfile
import threading
import unittest

import pyarrow as pa

from pypaimon.catalog.filesystem_catalog import FileSystemCatalog
from pypaimon.common.identifier import Identifier
from pypaimon.common.options import Options
from pypaimon.common.options.config import CatalogOptions
from pypaimon.data.timestamp import Timestamp
from pypaimon.manifest.manifest_file_manager import ManifestFileManager
from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.manifest.schema.manifest_file_meta import ManifestFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.schema.schema import Schema
from pypaimon.table.row.generic_row import GenericRow

_EMPTY_ROW = GenericRow([], [])
_EMPTY_STATS = SimpleStats(min_values=_EMPTY_ROW, max_values=_EMPTY_ROW, null_counts=[])


def _paimon_python_root():
    return os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))


def _runner_can_write_zstandard_avro():
    """fastavro uses ``backports.zstd`` (Py < 3.14) to *write* zstandard Avro blocks."""
    try:
        from io import BytesIO
        import fastavro
        buf = BytesIO()
        fastavro.writer(buf, {'type': 'string'}, ['x'], codec='zstandard')
        return len(buf.getvalue()) > 0
    except Exception:
        return False


def _venv_python_executable(venv_dir):
    for rel in (('bin', 'python'), ('bin', 'python3'), ('Scripts', 'python.exe')):
        path = os.path.join(venv_dir, *rel)
        if os.path.isfile(path):
            return path
    raise FileNotFoundError('no interpreter under venv: ' + venv_dir)


def _subprocess_env_for_pip():
    """Drop proxy variables for venv pip calls (avoids SOCKS optional dependency errors)."""
    env = os.environ.copy()
    for key in list(env):
        if 'PROXY' in key.upper():
            env.pop(key, None)
    env.setdefault('PIP_DISABLE_PIP_VERSION_CHECK', '1')
    return env


_MANIFEST_ZSTD_READ_SUBPROC_VENV_LOCK = threading.Lock()
_MANIFEST_ZSTD_READ_SUBPROC_VENV_DIR = None
_MANIFEST_ZSTD_READ_SUBPROC_VENV_PYTHON = None


def _manifest_zstd_read_subprocess_venv_python():
    """Disposable venv with editable pypaimon for ``manifest_list_zstd_read_subprocess.py``.

    Does not install ``backports.zstd`` so the first worker run can hit fastavro's missing zstd
    codec path when reading zstandard-compressed manifest lists.
    """
    global _MANIFEST_ZSTD_READ_SUBPROC_VENV_DIR, _MANIFEST_ZSTD_READ_SUBPROC_VENV_PYTHON
    with _MANIFEST_ZSTD_READ_SUBPROC_VENV_LOCK:
        if _MANIFEST_ZSTD_READ_SUBPROC_VENV_PYTHON is not None:
            return _MANIFEST_ZSTD_READ_SUBPROC_VENV_PYTHON
        repo = _paimon_python_root()
        venv_dir = tempfile.mkdtemp(prefix='paimon-zstd-read-subprocess-')
        pip_install_env = _subprocess_env_for_pip()
        uv_bin = shutil.which('uv')
        try:
            if uv_bin:
                subprocess.check_call(
                    [uv_bin, 'venv', venv_dir],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    env=pip_install_env,
                )
                isolated_venv_python = _venv_python_executable(venv_dir)
                subprocess.check_call(
                    [uv_bin, 'pip', 'install', '-q', '--python', isolated_venv_python, '-e', repo, 'requests'],
                    env=pip_install_env,
                )
            else:
                subprocess.check_call(
                    [sys.executable, '-m', 'venv', venv_dir],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
                isolated_venv_python = _venv_python_executable(venv_dir)
                subprocess.check_call(
                    [isolated_venv_python, '-m', 'pip', 'install', '-q', '-e', repo, 'requests'],
                    env=pip_install_env,
                )
        except Exception:
            shutil.rmtree(venv_dir, ignore_errors=True)
            raise
        _MANIFEST_ZSTD_READ_SUBPROC_VENV_DIR = venv_dir
        _MANIFEST_ZSTD_READ_SUBPROC_VENV_PYTHON = isolated_venv_python
        return isolated_venv_python


def _tear_down_manifest_zstd_read_subprocess_venv():
    global _MANIFEST_ZSTD_READ_SUBPROC_VENV_DIR, _MANIFEST_ZSTD_READ_SUBPROC_VENV_PYTHON
    with _MANIFEST_ZSTD_READ_SUBPROC_VENV_LOCK:
        if _MANIFEST_ZSTD_READ_SUBPROC_VENV_DIR:
            shutil.rmtree(_MANIFEST_ZSTD_READ_SUBPROC_VENV_DIR, ignore_errors=True)
        _MANIFEST_ZSTD_READ_SUBPROC_VENV_DIR = None
        _MANIFEST_ZSTD_READ_SUBPROC_VENV_PYTHON = None


def tearDownModule():
    _tear_down_manifest_zstd_read_subprocess_venv()


def _write_zstandard_manifest_list_file(table, list_name):
    """Write a manifest list Avro object file using the zstandard codec (same as Java may emit)."""
    from io import BytesIO
    import fastavro
    from pypaimon.manifest.manifest_list_manager import ManifestListManager
    from pypaimon.manifest.schema.manifest_file_meta import (
        MANIFEST_FILE_META_SCHEMA, ManifestFileMeta)
    from pypaimon.table.row.generic_row import GenericRowSerializer

    meta = ManifestFileMeta(
        file_name='manifest.avro', file_size=1024,
        num_added_files=1, num_deleted_files=0,
        partition_stats=SimpleStats.empty_stats(), schema_id=0,
    )
    avro_record = {
        '_VERSION': 2,
        '_FILE_NAME': meta.file_name,
        '_FILE_SIZE': meta.file_size,
        '_NUM_ADDED_FILES': meta.num_added_files,
        '_NUM_DELETED_FILES': meta.num_deleted_files,
        '_PARTITION_STATS': {
            '_MIN_VALUES': GenericRowSerializer.to_bytes(meta.partition_stats.min_values),
            '_MAX_VALUES': GenericRowSerializer.to_bytes(meta.partition_stats.max_values),
            '_NULL_COUNTS': meta.partition_stats.null_counts,
        },
        '_SCHEMA_ID': meta.schema_id,
        '_MIN_ROW_ID': meta.min_row_id,
        '_MAX_ROW_ID': meta.max_row_id,
    }
    buf = BytesIO()
    fastavro.writer(buf, MANIFEST_FILE_META_SCHEMA, [avro_record], codec='zstandard')
    mlm = ManifestListManager(table)
    list_path = '{}/{}'.format(mlm.manifest_path, list_name)
    with table.file_io.new_output_stream(list_path) as out:
        out.write(buf.getvalue())


class _ManifestManagerSetup(unittest.TestCase):
    """Shared setup for manifest manager tests.

    Subclasses must set _table_name and implement _make_manager / _write_one.
    """

    _table_name: str

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog = FileSystemCatalog(
            Options({CatalogOptions.WAREHOUSE.key(): cls.tempdir})
        )
        cls.catalog.create_database('default', False)

    def setUp(self):
        table_id = f'default.{self._table_name}'
        try:
            table_identifier = Identifier.from_string(table_id)
            table_path = self.catalog.get_table_path(table_identifier)
            if self.catalog.file_io.exists(table_path):
                self.catalog.file_io.delete(table_path, recursive=True)
        except Exception:
            pass

        pa_schema = pa.schema([('id', pa.int32()), ('value', pa.string())])
        schema = Schema.from_pyarrow_schema(pa_schema)
        self.catalog.create_table(table_id, schema, False)
        self.table = self.catalog.get_table(table_id)

    def _make_manager(self):
        raise NotImplementedError

    def _write_one(self, manager, name):
        """Write a single item that can be read back by manager.read(name)."""
        raise NotImplementedError


class ManifestFileManagerTest(_ManifestManagerSetup):
    """Tests for ManifestFileManager."""

    _table_name = 'manager_test'

    def _make_manager(self):
        return ManifestFileManager(self.table)

    def _write_one(self, manager, name):
        entry = ManifestEntry(
            kind=0,
            partition=_EMPTY_ROW,
            bucket=0,
            total_buckets=1,
            file=DataFileMeta(
                file_name="data.parquet", file_size=1024, row_count=100,
                min_key=_EMPTY_ROW, max_key=_EMPTY_ROW,
                key_stats=_EMPTY_STATS, value_stats=_EMPTY_STATS,
                min_sequence_number=1, max_sequence_number=100,
                schema_id=0, level=0, extra_files=[],
                creation_time=Timestamp.from_epoch_millis(0),
                delete_row_count=0, embedded_index=None, file_source=None,
                value_stats_cols=None, external_path=None,
                first_row_id=None, write_cols=None,
            ),
        )
        manager.write(name, [entry])

    def _create_manifest_entry(self, file_name, bucket=0):
        entry = ManifestEntry(
            kind=0,
            partition=_EMPTY_ROW,
            bucket=bucket,
            total_buckets=1,
            file=DataFileMeta(
                file_name=file_name, file_size=1024, row_count=100,
                min_key=_EMPTY_ROW, max_key=_EMPTY_ROW,
                key_stats=_EMPTY_STATS, value_stats=_EMPTY_STATS,
                min_sequence_number=1, max_sequence_number=100,
                schema_id=0, level=0, extra_files=[],
                creation_time=Timestamp.from_epoch_millis(0),
                delete_row_count=0, embedded_index=None, file_source=None,
                value_stats_cols=None, external_path=None,
                first_row_id=None, write_cols=None,
            ),
        )
        return entry

    def test_filter_applied_after_read(self):
        manager = self._make_manager()

        entries = [
            self._create_manifest_entry("data-1.parquet", bucket=0),
            self._create_manifest_entry("data-2.parquet", bucket=1),
            self._create_manifest_entry("data-3.parquet", bucket=0),
        ]
        manager.write("test-manifest.avro", entries)

        result_all = manager.read("test-manifest.avro")
        self.assertEqual(len(result_all), 3)

        result_filtered = manager.read(
            "test-manifest.avro", manifest_entry_filter=lambda e: e.bucket == 0)
        self.assertEqual(len(result_filtered), 2)


class ManifestListManagerTest(_ManifestManagerSetup):
    """Tests for ManifestListManager."""

    _table_name = 'list_manager_test'

    def _make_manager(self):
        return ManifestListManager(self.table)

    def _write_one(self, manager, name):
        meta = ManifestFileMeta(
            file_name="manifest.avro", file_size=1024,
            num_added_files=1, num_deleted_files=0,
            partition_stats=SimpleStats.empty_stats(), schema_id=0,
        )
        manager.write(name, [meta])

    def _make_snapshot(self, base_manifest_list, delta_manifest_list="delta-manifest-list"):
        from pypaimon.snapshot.snapshot import Snapshot
        return Snapshot(
            version=3, id=1, schema_id=0,
            base_manifest_list=base_manifest_list,
            delta_manifest_list=delta_manifest_list,
            commit_user="test", commit_identifier=1, commit_kind="APPEND",
            time_millis=1234567890, total_record_count=100, delta_record_count=10,
        )

    def test_read_base_returns_only_base_manifest(self):
        manager = self._make_manager()

        base_meta = ManifestFileMeta(
            file_name="manifest-base.avro", file_size=1024,
            num_added_files=1, num_deleted_files=0,
            partition_stats=SimpleStats.empty_stats(), schema_id=0,
        )
        delta_meta = ManifestFileMeta(
            file_name="manifest-delta.avro", file_size=1024,
            num_added_files=1, num_deleted_files=0,
            partition_stats=SimpleStats.empty_stats(), schema_id=0,
        )
        manager.write("base-manifest-list", [base_meta])
        manager.write("delta-manifest-list", [delta_meta])

        snapshot = self._make_snapshot("base-manifest-list", "delta-manifest-list")
        result = manager.read_base(snapshot)

        self.assertEqual(len(result), 1)
        self.assertEqual(result[0].file_name, "manifest-base.avro")

    @unittest.skipIf(
        sys.version_info < (3, 9),
        'PyPI backports.zstd only supports Python 3.9–3.13',
    )
    @unittest.skipIf(
        sys.version_info >= (3, 14),
        'fastavro uses stdlib compression.zstd on Python 3.14+, not backports.zstd',
    )
    def test_zstd_manifest_list_fastavro_requires_backports_zstd(self):
        """Child venv runs ``manifest_list_zstd_read_subprocess`` (argv: warehouse, table id, list file name).

        No ``backports.zstd`` in the venv → read fails; after ``pip install`` → read succeeds.
        """
        if not _runner_can_write_zstandard_avro():
            self.skipTest('runner cannot write zstandard Avro')

        zstd_manifest_list_file_name = 'zstd-manifest-list-backports-integration'
        _write_zstandard_manifest_list_file(self.table, zstd_manifest_list_file_name)

        manifest_list_manager = ManifestListManager(self.table)
        self.assertEqual(len(manifest_list_manager.read(zstd_manifest_list_file_name)), 1)

        zstd_manifest_list_abspath = os.path.join(
            manifest_list_manager.manifest_path, zstd_manifest_list_file_name)
        self.assertTrue(os.path.isfile(zstd_manifest_list_abspath), msg=zstd_manifest_list_abspath)

        zstd_read_subprocess_script = os.path.join(
            os.path.dirname(__file__), 'manifest_list_zstd_read_subprocess.py')
        catalog_table_id = 'default.{}'.format(self._table_name)
        isolated_venv_python = _manifest_zstd_read_subprocess_venv_python()
        pip_install_env = _subprocess_env_for_pip()
        subprocess.run(
            [isolated_venv_python, '-m', 'pip', 'uninstall', '-y', 'backports.zstd'],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=pip_install_env,
        )

        read_without_zstd_backend = subprocess.run(
            [
                isolated_venv_python,
                zstd_read_subprocess_script,
                self.catalog.warehouse,
                catalog_table_id,
                zstd_manifest_list_file_name,
            ],
            capture_output=True,
            text=True,
        )
        self.assertNotEqual(
            read_without_zstd_backend.returncode, 0,
            msg=read_without_zstd_backend.stdout + read_without_zstd_backend.stderr)
        stderr_and_stdout = (
            read_without_zstd_backend.stdout + read_without_zstd_backend.stderr)
        self.assertIn('zstandard codec is supported but you need to install', stderr_and_stdout)
        self.assertIn('backports.zstd', stderr_and_stdout)

        subprocess.check_call(
            [isolated_venv_python, '-m', 'pip', 'install', '-q', 'backports.zstd'],
            env=pip_install_env,
        )
        read_with_zstd_backend = subprocess.run(
            [
                isolated_venv_python,
                zstd_read_subprocess_script,
                self.catalog.warehouse,
                catalog_table_id,
                zstd_manifest_list_file_name,
            ],
            capture_output=True,
            text=True,
        )
        self.assertEqual(
            read_with_zstd_backend.returncode, 0,
            msg=read_with_zstd_backend.stdout + read_with_zstd_backend.stderr)
        self.assertEqual(read_with_zstd_backend.stdout.strip(), '1')


if __name__ == '__main__':
    unittest.main()
