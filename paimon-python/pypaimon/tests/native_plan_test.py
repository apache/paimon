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

import sys
import unittest
from types import ModuleType
from unittest.mock import Mock, patch

from pypaimon.catalog.catalog_context import CatalogContext
from pypaimon.catalog.filesystem_catalog_loader import FileSystemCatalogLoader
from pypaimon.catalog.jdbc_catalog_loader import JdbcCatalogLoader
from pypaimon.catalog.rest.rest_catalog_loader import RESTCatalogLoader
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.native_plan import (
    _catalog_options,
    _predicate_to_native,
    _read_options,
    _restore_python_partition_paths,
    native_plan,
)
from pypaimon.read.scan_stats import ScanStats
from pypaimon.read.table_scan import TableScan
from pypaimon.table.bucket_mode import BucketMode


def _scan(native_enabled, file_scanner):
    """Build a TableScan without running its heavy __init__."""
    scan = TableScan.__new__(TableScan)
    scan.table = Mock()
    scan.table.options.native_plan_enabled.return_value = native_enabled
    scan.table.options.options.contains_key.return_value = False   # no time-travel
    scan.table.options.options.contains.return_value = False       # no incremental
    scan.table.options.merge_engine.return_value = None            # not first-row
    scan.table.options.query_auth_enabled = False
    scan.table.current_branch.return_value = 'main'
    scan.table.is_primary_key_table = False        # not a pk table
    scan.table.trimmed_primary_keys = ['k']        # non-empty trimmed pk
    scan.table.bucket_mode.return_value = BucketMode.HASH_FIXED
    scan.table._applied_dynamic_options = {}       # no copy() overrides
    scan.table.partition_keys = []                 # not partitioned
    scan.table.table_schema.id = 1
    scan.table.schema_manager.latest.return_value.id = 1   # loaded schema is latest
    scan.table.identifier.get_database_name.return_value = 'default'
    scan.table.catalog_environment.catalog_loader = FileSystemCatalogLoader(
        CatalogContext.create_from_options(Options({})))           # filesystem catalog
    file_scanner.idx_of_this_subtask = None       # no shard
    file_scanner.start_pos_of_this_subtask = None  # no slice
    file_scanner.chunk_shuffle = None              # no chunk-shuffle
    file_scanner._global_index_result = None       # no global-index result
    file_scanner._row_ranges = None                # no explicit row ranges
    file_scanner.deletion_vectors_enabled = False  # no deletion vectors
    file_scanner.data_evolution = False            # no data evolution
    file_scanner.only_read_real_buckets = False    # not postpone bucket
    scan.file_scanner = file_scanner
    scan.predicate = None
    scan.partition_predicate = None
    scan._query_auth_fn = None      # no query-auth restrictions
    scan._read_type = None
    scan.limit = None               # no row limit
    return scan


class NativePlanTest(unittest.TestCase):

    def setUp(self):
        # Make the real capability probe see a split-API-capable pypaimon-rust so
        # gate tests route natively. Tests that call native_plan() directly override
        # sys.modules within their own block.
        fake_df = ModuleType('pypaimon_rust.datafusion')
        fake_df.PaimonCatalog = type(
            'PaimonCatalog', (), {'get_table': lambda self, name: None})
        fake_df.Split = type('Split', (), {'serialize': lambda self: b''})
        fake_mod = ModuleType('pypaimon_rust')
        fake_mod.datafusion = fake_df
        patcher = patch.dict(
            sys.modules,
            {'pypaimon_rust': fake_mod, 'pypaimon_rust.datafusion': fake_df})
        patcher.start()
        self.addCleanup(patcher.stop)

    def test_switch_defaults_off(self):
        self.assertFalse(CoreOptions(Options({})).native_plan_enabled())
        self.assertTrue(
            CoreOptions(Options({"scan.native-plan.enabled": "true"})).native_plan_enabled())

    def test_plan_uses_file_scanner_when_switch_off(self):
        fs = Mock()
        sentinel = object()
        fs.scan.return_value = sentinel
        scan = _scan(native_enabled=False, file_scanner=fs)
        self.assertIs(scan.plan(), sentinel)
        fs.scan.assert_called_once_with()

    def test_plan_routes_to_native_and_prunes_partitions(self):
        # Native planner returns every partition; the predicate keeps only [2026, 7].
        keep = Mock(partition=Mock(values=[2026, 7]))
        drop = Mock(partition=Mock(values=[2025, 1]))
        pred = Mock()
        pred.test.side_effect = lambda part: part.values == [2026, 7]
        fs = Mock(partition_key_predicate=pred)
        scan = _scan(native_enabled=True, file_scanner=fs)

        with patch('pypaimon.read.native_plan.native_plan', return_value=[keep, drop]) as np:
            plan = scan.plan()

        np.assert_called_once_with(
            scan.table, predicate=None, limit=None, projection=None)
        fs.scan.assert_not_called()
        self.assertEqual(plan.splits(), [keep])

    def test_plan_falls_back_when_partition_prune_raises(self):
        pred = Mock()
        pred.test.side_effect = RuntimeError('predicate boom')
        fs = Mock(partition_key_predicate=pred)
        sentinel = object()
        fs.scan.return_value = sentinel
        scan = _scan(native_enabled=True, file_scanner=fs)
        split = Mock(partition=Mock(values=[2026, 7]), snapshot_id=1)
        with patch('pypaimon.read.native_plan.native_plan', return_value=[split]):
            self.assertIs(scan.plan(), sentinel)
        fs.scan.assert_called_once_with()

    def test_plan_native_no_partition_predicate_keeps_all(self):
        splits = [Mock(partition=Mock(values=[1])), Mock(partition=Mock(values=[2]))]
        fs = Mock(partition_key_predicate=None)
        scan = _scan(native_enabled=True, file_scanner=fs)
        with patch('pypaimon.read.native_plan.native_plan', return_value=splits):
            self.assertEqual(scan.plan().splits(), splits)

    def test_plan_forwards_filter_limit_partition_and_time_travel(self):
        fs = Mock(partition_key_predicate=None)
        scan = _scan(native_enabled=True, file_scanner=fs)
        scan.table.partition_keys = ['dt']
        scan.limit = 5
        scan._read_type = [Mock(name='k'), Mock(name='dt')]
        scan._read_type[0].name = 'k'
        scan._read_type[1].name = 'dt'
        scan.predicate = PredicateBuilder.and_predicates([
            Predicate('equal', 0, 'k', [7]),
            Predicate('equal', 1, 'dt', ['2026-08-02']),
        ])
        scan.table._applied_dynamic_options = {'scan.snapshot-id': '3'}
        scan.table.options.options.contains_key.side_effect = (
            lambda key: key == 'scan.snapshot-id')
        scan.table.table_schema.id = 2
        scan.table.schema_manager.latest.return_value.id = 3
        split = Mock(partition=Mock(values=['2026-08-02']), snapshot_id=3)

        with patch('pypaimon.read.native_plan.native_plan', return_value=[split]) as np:
            plan = scan.plan()

        self.assertEqual(plan.snapshot_id, 3)
        np.assert_called_once_with(
            scan.table,
            predicate=scan.predicate,
            limit=5,
            projection=['k', 'dt'],
        )

    def test_plan_falls_back_when_scan_is_not_plain(self):
        # Native planning does not carry shard/slice, global-index, row ranges,
        # or incremental scans -> must fall back to the file scanner.
        def check(setup):
            fs = Mock(partition_key_predicate=None)
            sentinel = object()
            fs.scan.return_value = sentinel
            scan = _scan(native_enabled=True, file_scanner=fs)
            setup(scan, fs)
            with patch('pypaimon.read.native_plan.native_plan') as np:
                self.assertIs(scan.plan(), sentinel)
            np.assert_not_called()
            fs.scan.assert_called_once_with()

        check(lambda s, fs: setattr(fs, 'idx_of_this_subtask', 0))
        check(lambda s, fs: setattr(fs, 'start_pos_of_this_subtask', 0))
        check(lambda s, fs: setattr(fs, 'chunk_shuffle', (1, 100)))
        check(lambda s, fs: setattr(fs, '_global_index_result', object()))
        check(lambda s, fs: setattr(fs, '_row_ranges', [object()]))
        check(lambda s, fs: setattr(fs, 'deletion_vectors_enabled', True))
        check(lambda s, fs: setattr(fs, 'only_read_real_buckets', True))
        check(lambda s, fs: (setattr(s.table, 'is_primary_key_table', True),
                             setattr(s.table, 'trimmed_primary_keys', [])))
        check(lambda s, fs: s.table.bucket_mode.__setattr__(
            'return_value', BucketMode.HASH_DYNAMIC))
        check(lambda s, fs: s.table.bucket_mode.__setattr__(
            'return_value', BucketMode.CROSS_PARTITION))
        check(lambda s, fs: setattr(
            s.table, '_applied_dynamic_options', {'scan.snapshot-id': None}))
        check(lambda s, fs: setattr(s.table.schema_manager.latest.return_value, 'id', 2))
        check(lambda s, fs: s.table.schema_manager.latest.__setattr__(
            'side_effect', RuntimeError('metadata read failed')))
        check(lambda s, fs: s.table.options.options.contains_key.__setattr__(
            'side_effect', lambda k: k == 'scan.version'))
        check(lambda s, fs: s.table.options.merge_engine.__setattr__(
            'return_value', 'first-row'))
        check(lambda s, fs: setattr(s.table.options, 'query_auth_enabled', True))
        check(lambda s, fs: s.table.current_branch.__setattr__('return_value', 'b1'))
        check(lambda s, fs: s.table.identifier.get_database_name.__setattr__(
            'return_value', 'db.name'))
        check(lambda s, fs: s.table.identifier.get_database_name.__setattr__(
            'return_value', 'unknown'))
        check(lambda s, fs: setattr(
            s.table.catalog_environment, 'catalog_loader', object()))   # no context()
        for attr in ('hadoop_conf', 'prefer_io_loader', 'fallback_io_loader'):
            check(lambda s, fs, attr=attr: setattr(
                s.table.catalog_environment.catalog_loader.context(), attr, object()))
        check(lambda s, fs: s.table.options.options.contains.__setattr__(
            'return_value', True))          # incremental

    def test_plan_native_empty_falls_back(self):
        # Empty native result -> fall back for an atomic snapshot id.
        fs = Mock(partition_key_predicate=None)
        sentinel = object()
        fs.scan.return_value = sentinel
        scan = _scan(native_enabled=True, file_scanner=fs)
        with patch('pypaimon.read.native_plan.native_plan', return_value=[]):
            self.assertIs(scan.plan(), sentinel)
        fs.scan.assert_called_once_with()

    def test_plan_falls_back_when_rust_unavailable(self):
        # scan.native-plan.enabled but pypaimon-rust missing/old -> fall back,
        # not crash.
        fs = Mock(partition_key_predicate=None)
        sentinel = object()
        fs.scan.return_value = sentinel
        scan = _scan(native_enabled=True, file_scanner=fs)
        with patch('pypaimon.read.native_plan.native_runtime_available',
                   return_value=False), \
                patch('pypaimon.read.native_plan.native_plan') as np:
            self.assertIs(scan.plan(), sentinel)
        np.assert_not_called()
        fs.scan.assert_called_once_with()

    def test_plan_falls_back_when_native_plan_raises(self):
        # A native planning failure (e.g. unsupported scheme) must fall back, not crash.
        fs = Mock(partition_key_predicate=None)
        sentinel = object()
        fs.scan.return_value = sentinel
        scan = _scan(native_enabled=True, file_scanner=fs)
        with patch('pypaimon.read.native_plan.native_plan',
                   side_effect=RuntimeError('unsupported scheme viewfs://')):
            self.assertIs(scan.plan(), sentinel)
        fs.scan.assert_called_once_with()

    def test_plan_falls_back_for_jdbc_catalog_loader(self):
        fs = Mock(partition_key_predicate=None)
        sentinel = object()
        fs.scan.return_value = sentinel
        scan = _scan(native_enabled=True, file_scanner=fs)
        scan.table.catalog_environment.catalog_loader = JdbcCatalogLoader(
            CatalogContext.create_from_options(Options({})))

        with patch('pypaimon.read.native_plan.native_plan') as np:
            self.assertIs(scan.plan(), sentinel)

        np.assert_not_called()
        fs.scan.assert_called_once_with()

    def test_plan_falls_back_for_builtin_catalog_loader_subclasses(self):
        class RoutedFileSystemLoader(FileSystemCatalogLoader):
            def load(self):
                return object()

        class RoutedRESTLoader(RESTCatalogLoader):
            def load(self):
                return object()

        for loader_class in (RoutedFileSystemLoader, RoutedRESTLoader):
            with self.subTest(loader_class=loader_class.__name__):
                fs = Mock(partition_key_predicate=None)
                sentinel = object()
                fs.scan.return_value = sentinel
                scan = _scan(native_enabled=True, file_scanner=fs)
                scan.table.catalog_environment.catalog_loader = loader_class(
                    CatalogContext.create_from_options(Options({})))

                with patch('pypaimon.read.native_plan.native_plan') as np:
                    self.assertIs(scan.plan(), sentinel)

                np.assert_not_called()
                fs.scan.assert_called_once_with()

    def test_scan_with_stats_native_empty_uses_fallback_stats(self):
        fs = Mock(partition_key_predicate=None)
        fallback_plan = object()
        fallback_stats = ScanStats(manifest_files_total=7)
        fs.scan_with_stats.return_value = (fallback_plan, fallback_stats)
        scan = _scan(native_enabled=True, file_scanner=fs)

        with patch('pypaimon.read.native_plan.native_plan', return_value=[]) as np:
            plan, stats = scan.scan_with_stats()

        self.assertIs(plan, fallback_plan)
        self.assertIs(stats, fallback_stats)
        np.assert_called_once_with(
            scan.table, predicate=None, limit=None, projection=None)
        fs.scan_with_stats.assert_called_once_with()
        fs.scan.assert_not_called()

    def test_catalog_options_are_normalized_for_rust(self):
        table = Mock()
        table.catalog_environment.catalog_loader = FileSystemCatalogLoader(
            CatalogContext.create_from_options(Options({
                'warehouse': '/tmp/warehouse',
                'data-token.enabled': True,
                'retry-count': 3,
                'unset': None,
            })))

        self.assertEqual(_catalog_options(table), {
            'warehouse': '/tmp/warehouse',
            # Lowercase so Rust's case-sensitive bool parser accepts it.
            'data-token.enabled': 'true',
            'retry-count': '3',
            'metastore': 'filesystem',
        })

    def test_catalog_options_use_actual_rest_loader_type(self):
        table = Mock()
        table.catalog_environment.catalog_loader = RESTCatalogLoader(
            CatalogContext.create_from_options(Options({
                'uri': 'http://localhost:8181',
            })))

        self.assertEqual(_catalog_options(table), {
            'uri': 'http://localhost:8181',
            'metastore': 'rest',
        })

    def test_catalog_options_reject_loader_subclass(self):
        class RoutedFileSystemLoader(FileSystemCatalogLoader):
            pass

        table = Mock()
        table.catalog_environment.catalog_loader = RoutedFileSystemLoader(
            CatalogContext.create_from_options(Options({})))

        with self.assertRaisesRegex(ValueError, 'exact built-in catalog loader'):
            _catalog_options(table)

    def test_predicate_and_time_travel_are_converted_for_rust(self):
        predicate = PredicateBuilder.and_predicates([
            Predicate('greaterOrEqual', 0, 'k', [10]),
            Predicate('in', 1, 'v', ['a', 'b']),
        ])
        self.assertEqual(_predicate_to_native(predicate), {
            'method': 'and',
            'children': [
                {'method': 'greaterOrEqual', 'field': 'k', 'literals': [10]},
                {'method': 'in', 'field': 'v', 'literals': ['a', 'b']},
            ],
        })

        table = Mock()
        table.options.source_split_target_size.return_value = 1024
        table.options.source_split_open_file_cost.return_value = 128
        table.options.options = Options({
            'scan.snapshot-id': '9',
        })
        self.assertEqual(_read_options(table), {
            'source.split.target-size': '1024',
            'source.split.open-file-cost': '128',
            'scan.snapshot-id': '9',
        })

    def test_partition_path_prefers_existing_python_legacy_path(self):
        table = Mock(partition_keys=['p'])
        table.path_factory.return_value.bucket_path.return_value = (
            '/warehouse/t/p=a/b/bucket-0')
        table.file_io.list_status.return_value = [Mock(base_name='data.parquet')]
        data_file = Mock(
            external_path=None,
            file_name='data.parquet',
            file_path='/warehouse/t/p=a%2Fb/bucket-0/data.parquet',
        )
        split = Mock(
            partition=Mock(values=['a/b']), bucket=0, files=[data_file])

        _restore_python_partition_paths(table, [split])

        self.assertEqual(
            data_file.file_path,
            '/warehouse/t/p=a/b/bucket-0/data.parquet',
        )

    def test_partition_path_keeps_existing_rust_path(self):
        table = Mock(partition_keys=['p'])
        table.path_factory.return_value.bucket_path.return_value = (
            '/warehouse/t/p=a/b/bucket-0')
        table.file_io.list_status.return_value = []
        rust_path = '/warehouse/t/p=a%2Fb/bucket-0/data.parquet'
        data_file = Mock(
            external_path=None,
            file_name='data.parquet',
            file_path=rust_path,
        )
        split = Mock(
            partition=Mock(values=['a/b']), bucket=0, files=[data_file])

        _restore_python_partition_paths(table, [split])

        self.assertEqual(data_file.file_path, rust_path)

    def test_partition_path_lists_each_bucket_once(self):
        table = Mock(partition_keys=['p'])
        table.path_factory.return_value.bucket_path.return_value = (
            '/warehouse/t/p=a/b/bucket-0')
        table.file_io.list_status.return_value = [
            Mock(base_name='a.parquet'), Mock(base_name='b.parquet')]
        splits = [
            Mock(partition=Mock(values=['a/b']), bucket=0, files=[Mock(
                external_path=None,
                file_name=name,
                file_path='/warehouse/t/p=a%%2Fb/bucket-0/%s' % name,
            )])
            for name in ('a.parquet', 'b.parquet')
        ]

        _restore_python_partition_paths(table, splits)

        table.file_io.list_status.assert_called_once_with(
            '/warehouse/t/p=a/b/bucket-0')
        self.assertEqual(
            [split.files[0].file_path for split in splits],
            [
                '/warehouse/t/p=a/b/bucket-0/a.parquet',
                '/warehouse/t/p=a/b/bucket-0/b.parquet',
            ],
        )

    def test_partition_path_listing_failure_is_not_hidden(self):
        table = Mock(partition_keys=['p'])
        table.path_factory.return_value.bucket_path.return_value = (
            '/warehouse/t/p=a/b/bucket-0')
        table.file_io.list_status.side_effect = PermissionError('denied')
        split = Mock(partition=Mock(values=['a/b']), bucket=0, files=[Mock(
            external_path=None,
            file_name='data.parquet',
            file_path='/warehouse/t/p=a%2Fb/bucket-0/data.parquet',
        )])

        with self.assertRaises(PermissionError):
            _restore_python_partition_paths(table, [split])

    def test_native_plan_threads_trimmed_keys_to_deserializer(self):
        # PK tables route through: the trimmed primary keys must reach the
        # deserializer so per-file min/max keys are decoded for merge-on-read.
        kfields = [object()]
        table = Mock(trimmed_primary_keys_fields=kfields)
        table.table_schema = Mock(fields=[], partition_keys=[])
        table.partition_keys = []
        table.options.source_split_target_size.return_value = 1024
        table.options.source_split_open_file_cost.return_value = 128
        table.options.options.contains_key.return_value = False
        split = Mock()
        split.serialize.return_value = b'bytes'
        rt = Mock()
        rt.new_read_builder.return_value.new_scan.return_value.plan.return_value \
            .splits.return_value = [split]
        catalog = Mock()
        catalog.get_table.return_value = rt

        fake_df = ModuleType('pypaimon_rust.datafusion')
        fake_df.PaimonCatalog = Mock(return_value=catalog)
        fake_df.Split = type('Split', (), {'serialize': lambda self: b''})
        fake_mod = ModuleType('pypaimon_rust')
        fake_mod.datafusion = fake_df

        with patch.dict(sys.modules,
                        {'pypaimon_rust': fake_mod, 'pypaimon_rust.datafusion': fake_df}), \
                patch('pypaimon.read.native_plan._catalog_options', return_value={}), \
                patch('pypaimon.read.native_plan.deserialize_split_v1',
                      return_value='decoded') as des:
            result = native_plan(table)

        self.assertEqual(result, ['decoded'])
        rt.new_read_builder.assert_called_once_with({
            CoreOptions.SOURCE_SPLIT_TARGET_SIZE.key(): '1024',
            CoreOptions.SOURCE_SPLIT_OPEN_FILE_COST.key(): '128',
        })
        des.assert_called_once_with(b'bytes', [], kfields)

    def test_native_plan_requires_split_api(self):
        # An intermediate pypaimon-rust missing either get_table or Split.serialize
        # must raise a clear error, not an AttributeError mid-plan.
        split_ok = type('Split', (), {'serialize': lambda self: b''})
        catalog_ok = type('PaimonCatalog', (), {'get_table': lambda self, name: None})
        cases = {
            'no get_table': (type('PaimonCatalog', (), {}), split_ok),
            'no serialize': (catalog_ok, type('Split', (), {})),
        }
        for label, (catalog_cls, split_cls) in cases.items():
            with self.subTest(case=label):
                fake_df = ModuleType('pypaimon_rust.datafusion')
                fake_df.PaimonCatalog = catalog_cls
                fake_df.Split = split_cls
                fake_mod = ModuleType('pypaimon_rust')
                fake_mod.datafusion = fake_df
                with patch.dict(
                        sys.modules,
                        {'pypaimon_rust': fake_mod, 'pypaimon_rust.datafusion': fake_df}):
                    with self.assertRaisesRegex(RuntimeError, '0.3.0'):
                        native_plan(Mock())


if __name__ == '__main__':
    unittest.main()
