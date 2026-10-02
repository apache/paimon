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

import json
import sys
import unittest
from types import ModuleType, SimpleNamespace
from unittest.mock import Mock, call, patch

from pypaimon.catalog.catalog_context import CatalogContext
from pypaimon.catalog.filesystem_catalog_loader import FileSystemCatalogLoader
from pypaimon.catalog.jdbc_catalog_loader import JdbcCatalogLoader
from pypaimon.catalog.rest.rest_catalog_loader import RESTCatalogLoader
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.vector_search_result import ScoredGlobalIndexResult
from pypaimon.read.native_plan import (
    _catalog_options,
    _native_read_builder,
    _predicate_to_native,
    _resolved_schema_json,
    _restore_python_partition_paths,
    native_family_search_modes_available,
    native_plan,
    native_version_at_least,
)
from pypaimon.read.plan import Plan
from pypaimon.read.table_scan import TableScan
from pypaimon.schema.data_types import AtomicType, DataField, MapType, MultisetType, RowType
from pypaimon.schema.table_schema import TableSchema
from pypaimon.table.bucket_mode import BucketMode
from pypaimon.utils.range import Range


def _scan(native_enabled, file_scanner):
    """Build a TableScan without running its heavy __init__."""
    scan = TableScan.__new__(TableScan)
    scan.table = Mock()
    scan.table.options.native_plan_enabled.return_value = native_enabled
    scan.table.options.native_read_enabled.return_value = False
    scan.table.options.options.contains_key.return_value = False   # no time-travel
    scan.table.options.options.contains.return_value = False       # no incremental
    scan.table.options.merge_engine.return_value = None            # not first-row
    scan.table.options.query_auth_enabled = False
    scan.table.options.data_file_path_directory.return_value = None  # no relocated dir
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
    file_scanner.is_streaming = False
    file_scanner.skip_level0 = False
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
        if sys.version_info >= (3, 8):
            version_patcher = patch('importlib.metadata.version', return_value='0.3.0')
            version_patcher.start()
            self.addCleanup(version_patcher.stop)

    def test_resolved_schema_json_uses_canonical_nested_collection_types(self):
        schema = TableSchema(id=7, highest_field_id=3, time_millis=0, fields=[
            DataField(0, 'attributes', MapType(False, AtomicType('STRING', False), RowType(True, [
                DataField(1, 'counts', MapType(True, AtomicType('STRING', False), AtomicType('INT', False))),
                DataField(2, 'tags', MultisetType(False, AtomicType('STRING', False))),
                DataField(3, 'groups', MultisetType(True, MapType(
                    False, AtomicType('STRING', False), AtomicType('INT'))))
            ])))
        ])
        table = SimpleNamespace(table_schema=schema)
        self.assertEqual(json.loads(_resolved_schema_json(table)), {
            'version': 3, 'id': 7, 'highestFieldId': 3, 'timeMillis': 0,
            'partitionKeys': [], 'primaryKeys': [], 'comment': None,
            'options': {},
            'fields': [{'id': 0, 'name': 'attributes', 'type': {
                'type': 'MAP NOT NULL', 'nullable': False, 'key': 'STRING NOT NULL',
                'value': {'type': 'ROW', 'nullable': True, 'fields': [
                    {'id': 1, 'name': 'counts', 'type': {
                        'type': 'MAP', 'nullable': True,
                        'key': 'STRING NOT NULL', 'value': 'INT NOT NULL'}},
                    {'id': 2, 'name': 'tags', 'type': {
                        'type': 'MULTISET NOT NULL', 'nullable': False, 'element': 'STRING NOT NULL'}},
                    {'id': 3, 'name': 'groups', 'type': {
                        'type': 'MULTISET', 'nullable': True, 'element': {
                            'type': 'MAP NOT NULL', 'nullable': False,
                            'key': 'STRING NOT NULL', 'value': 'INT'}}}]}}}],
        })

    def test_resolved_schema_keeps_custom_io_and_rest_on_catalog_path(self):
        from pypaimon.catalog.catalog_environment import CatalogEnvironment
        from pypaimon.catalog.jdbc_catalog_loader import JdbcCatalogLoader
        from pypaimon.filesystem.local_file_io import LocalFileIO
        from pypaimon.read.native_plan import _resolved_schema_file_io_options

        class CustomIO(LocalFileIO):
            pass

        class CustomEnvironment(CatalogEnvironment):
            pass

        class CustomLoader(FileSystemCatalogLoader):
            pass

        class CustomJdbcLoader(JdbcCatalogLoader):
            pass

        table = Mock(file_io=LocalFileIO(), catalog_environment=CatalogEnvironment.empty())
        self.assertEqual(_resolved_schema_file_io_options(table), {})
        table.file_io = CustomIO()
        self.assertIsNone(_resolved_schema_file_io_options(table))
        table.file_io = LocalFileIO()
        table.catalog_environment = CustomEnvironment()
        self.assertIsNone(_resolved_schema_file_io_options(table))
        table.catalog_environment = CatalogEnvironment.empty()
        for loader_type in (RESTCatalogLoader, CustomLoader, CustomJdbcLoader):
            table.catalog_environment.catalog_loader = loader_type(
                CatalogContext.create_from_options(Options({})))
            self.assertIsNone(_resolved_schema_file_io_options(table))
        for loader_type in (FileSystemCatalogLoader, JdbcCatalogLoader):
            for attr in ('hadoop_conf', 'prefer_io_loader', 'fallback_io_loader'):
                context = CatalogContext.create_from_options(Options({}))
                setattr(context, attr, object())
                table.catalog_environment.catalog_loader = loader_type(context)
                self.assertIsNone(_resolved_schema_file_io_options(table))

    def test_switch_defaults_off(self):
        defaults = CoreOptions(Options({}))
        self.assertFalse(defaults.native_plan_enabled())
        self.assertFalse(defaults.native_read_enabled())
        self.assertTrue(
            CoreOptions(Options({"scan.native-plan.enabled": "true"})).native_plan_enabled())
        self.assertTrue(
            CoreOptions(Options({"read.native.enabled": "true"})).native_read_enabled())

    def test_catalogless_standard_file_io_options_are_preserved(self):
        from pypaimon.catalog.catalog_environment import CatalogEnvironment
        from pypaimon.filesystem.pyarrow_file_io import PyArrowFileIO
        from pypaimon.filesystem.resolving_file_io import ResolvingFileIO
        from pypaimon.read.native_plan import _resolved_schema_file_io_options

        properties = Options({'s3.path-style-access': True, 's3.endpoint': 'http://localhost:9000'})
        # No storage connection is needed to check the resolved context transfer.
        arrow = PyArrowFileIO.__new__(PyArrowFileIO)
        arrow.properties = properties
        for file_io in (arrow, ResolvingFileIO(properties)):
            table = Mock(file_io=file_io, catalog_environment=CatalogEnvironment.empty())
            self.assertEqual(_resolved_schema_file_io_options(table), {
                's3.path-style-access': 'true', 's3.endpoint': 'http://localhost:9000'})

    def test_plan_uses_file_scanner_when_switch_off(self):
        fs = Mock()
        sentinel = object()
        fs.scan.return_value = sentinel
        scan = _scan(native_enabled=False, file_scanner=fs)
        self.assertIs(scan.plan(), sentinel)
        fs.scan.assert_called_once_with()

    def test_native_read_switch_also_requests_native_plan(self):
        fs = Mock(partition_key_predicate=None)
        scan = _scan(native_enabled=False, file_scanner=fs)
        scan.table.options.native_read_enabled.return_value = True
        expected = Plan([], 1)
        with patch('pypaimon.read.native_plan.native_reader_available',
                   return_value=True), \
                patch('pypaimon.read.native_plan.native_plan',
                      return_value=expected) as np:
            self.assertEqual(scan.plan(), expected)
        np.assert_called_once()
        fs.scan.assert_not_called()

    def test_plan_routes_to_native_and_prunes_partitions(self):
        # Native planner returns every partition; the predicate keeps only [2026, 7].
        keep = Mock(partition=Mock(values=[2026, 7]))
        drop = Mock(partition=Mock(values=[2025, 1]))
        pred = Mock()
        pred.test.side_effect = lambda part: part.values == [2026, 7]
        fs = Mock(partition_key_predicate=pred)
        scan = _scan(native_enabled=True, file_scanner=fs)

        with patch('pypaimon.read.native_plan.native_plan', return_value=Plan([keep, drop], 1)) as np:
            plan = scan.plan()

        np.assert_called_once_with(
            scan.table, predicate=None, limit=None, projection=None,
            row_ranges=None)
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
        with patch('pypaimon.read.native_plan.native_plan', return_value=Plan([split], 1)):
            self.assertIs(scan.plan(), sentinel)
        fs.scan.assert_called_once_with()

    def test_plan_native_no_partition_predicate_keeps_all(self):
        splits = [Mock(partition=Mock(values=[1])), Mock(partition=Mock(values=[2]))]
        fs = Mock(partition_key_predicate=None)
        scan = _scan(native_enabled=True, file_scanner=fs)
        with patch('pypaimon.read.native_plan.native_plan', return_value=Plan(splits, 1)):
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

        with patch('pypaimon.read.native_plan.native_plan', return_value=Plan([split], 3)) as np:
            plan = scan.plan()

        self.assertEqual(plan.snapshot_id, 3)
        np.assert_called_once_with(
            scan.table,
            predicate=scan.predicate,
            limit=5,
            projection=['k', 'dt'],
            row_ranges=None,
        )

    def test_plan_forwards_global_index_row_ranges(self):
        fs = Mock(partition_key_predicate=None)
        scan = _scan(native_enabled=True, file_scanner=fs)
        fs.data_evolution = True
        fs._global_index_result = GlobalIndexResult.from_ranges([
            Range(1, 2), Range(5, 5)])
        split = Mock(partition=Mock(values=[]), snapshot_id=3)

        with patch('pypaimon.read.native_plan.native_plan', return_value=Plan([split], 3)) as np:
            plan = scan.plan()

        np.assert_called_once_with(
            scan.table,
            predicate=None,
            limit=None,
            projection=None,
            row_ranges=[(1, 2), (5, 5)],
        )
        fs.scan.assert_not_called()
        self.assertEqual(plan.splits(), [split])

    def test_empty_global_index_result_does_not_fall_back(self):
        fs = Mock(partition_key_predicate=None)
        scan = _scan(native_enabled=True, file_scanner=fs)
        fs.data_evolution = True
        fs._global_index_result = GlobalIndexResult.create_empty()

        with patch('pypaimon.read.native_plan.native_plan', return_value=Plan([], 1)) as np:
            plan = scan.plan()

        np.assert_called_once_with(
            scan.table,
            predicate=None,
            limit=None,
            projection=None,
            row_ranges=[],
        )
        fs.scan.assert_not_called()
        self.assertEqual(plan.splits(), [])

    def test_scored_global_index_result_uses_native_ranges(self):
        from pypaimon.globalindex.indexed_split import IndexedSplit
        from pypaimon.read.split import DataSplit

        fs = Mock(partition_key_predicate=None)
        scan = _scan(native_enabled=True, file_scanner=fs)
        fs.data_evolution = True
        bitmap = GlobalIndexResult.from_range(Range(1, 1)).results()
        fs._global_index_result = ScoredGlobalIndexResult.create(bitmap, lambda _: 0.75)
        split = IndexedSplit(DataSplit([], None, 0), [Range(1, 1)])
        with patch('pypaimon.read.native_plan.native_plan', return_value=Plan([split], 9)) as np:
            plan = scan.plan()
        self.assertEqual(np.call_args[1]['row_ranges'], [(1, 1)])
        self.assertEqual(plan.splits()[0].scores(), [0.75])
        self.assertEqual(plan.snapshot_id, 9)
        fs.scan.assert_not_called()

    def test_global_index_row_ranges_require_data_evolution_append_table(self):
        result = GlobalIndexResult.from_range(Range(1, 1))

        for data_evolution, primary_key in ((False, False), (True, True)):
            with self.subTest(
                    data_evolution=data_evolution, primary_key=primary_key):
                fs = Mock(partition_key_predicate=None)
                fs.scan.return_value = fallback = object()
                scan = _scan(native_enabled=True, file_scanner=fs)
                fs.data_evolution = data_evolution
                fs._global_index_result = result
                scan.table.is_primary_key_table = primary_key

                with patch('pypaimon.read.native_plan.native_plan') as np:
                    self.assertIs(scan.plan(), fallback)

                np.assert_not_called()
                fs.scan.assert_called_once_with()

    def test_plan_falls_back_for_unsupported_scan_context(self):
        # These cases cannot be reconstructed by native planning even with the
        # current Rust bindings.
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

        check(lambda s, fs: setattr(fs, '_global_index_result', object()))
        check(lambda s, fs: (setattr(s.table, 'is_primary_key_table', True),
                             setattr(s.table, 'trimmed_primary_keys', [])))
        check(lambda s, fs: setattr(s.table.options, 'query_auth_enabled', True))

        def unknown_rest_database(scan, fs):
            scan.table.catalog_environment.catalog_loader = RESTCatalogLoader(
                CatalogContext.create_from_options(Options({})))
            scan.table.identifier.get_database_name.return_value = 'unknown'

        check(unknown_rest_database)
        check(lambda s, fs: setattr(
            s.table.catalog_environment, 'catalog_loader', object()))   # no context()
        for attr in ('hadoop_conf', 'prefer_io_loader', 'fallback_io_loader'):
            check(lambda s, fs, attr=attr: setattr(
                s.table.catalog_environment.catalog_loader.context(), attr, object()))

    def test_plan_native_empty_preserves_snapshot_without_fallback(self):
        for snapshot_id in (None, 7):
            with self.subTest(snapshot_id=snapshot_id):
                fs = Mock(partition_key_predicate=None)
                scan = _scan(native_enabled=True, file_scanner=fs)
                with patch('pypaimon.read.native_plan.native_plan',
                           return_value=Plan([], snapshot_id)):
                    result = scan.plan()
                self.assertEqual(result.splits(), [])
                self.assertEqual(result.snapshot_id, snapshot_id)
                fs.scan.assert_not_called()

    def test_plan_falls_back_when_rust_unavailable(self):
        # scan.native-plan.enabled but pypaimon-rust missing -> fall back.
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

    def test_family_search_modes_use_native_plan(self):
        fs = Mock(partition_key_predicate=None)
        scan = _scan(native_enabled=True, file_scanner=fs)
        scan.table.options.options.contains_key.side_effect = (
            lambda key: key == 'scalar-index.search-mode')
        scan.table._applied_dynamic_options = {
            'scalar-index.search-mode': 'full',
        }
        split = Mock(partition=Mock(values=[]), snapshot_id=1)

        with patch('pypaimon.read.native_plan.native_plan',
                   return_value=Plan([split], 1)) as native:
            self.assertEqual(scan.plan().splits(), [split])

        native.assert_called_once()
        fs.scan.assert_not_called()

    def test_removing_search_mode_uses_native_plan(self):
        fs = Mock(partition_key_predicate=None)
        scan = _scan(native_enabled=True, file_scanner=fs)
        scan.table._applied_dynamic_options = {
            'scalar-index.search-mode': None,
        }

        with patch('pypaimon.read.native_plan.native_plan',
                   return_value=Plan([], 1)) as native:
            self.assertEqual(scan.plan().snapshot_id, 1)

        native.assert_called_once()
        fs.scan.assert_not_called()

    def test_dynamic_read_option_uses_native_plan(self):
        fs = Mock(partition_key_predicate=None)
        scan = _scan(native_enabled=True, file_scanner=fs)
        scan.table._applied_dynamic_options = {
            'blob-as-descriptor': 'true',
            'read.batch-size': '2048',
            'read.parallelism': '2',
        }
        split = Mock(partition=Mock(values=[]), snapshot_id=1)

        with patch(
                'pypaimon.read.native_plan.native_plan',
                return_value=Plan([split], 1)) as native:
            self.assertEqual(scan.plan().splits(), [split])

        native.assert_called_once()
        fs.scan.assert_not_called()

    def test_unknown_dynamic_option_uses_native_plan(self):
        fs = Mock(partition_key_predicate=None)
        scan = _scan(native_enabled=True, file_scanner=fs)
        scan.table._applied_dynamic_options = {
            'future.scan-option': 'value',
        }

        with patch('pypaimon.read.native_plan.native_plan',
                   return_value=Plan([], 1)) as native:
            self.assertEqual(scan.plan().snapshot_id, 1)

        native.assert_called_once()
        fs.scan.assert_not_called()

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

    def test_plan_uses_resolved_schema_for_jdbc_catalog_loader(self):
        fs = Mock(partition_key_predicate=None)
        scan = _scan(native_enabled=True, file_scanner=fs)
        scan.table.catalog_environment.catalog_loader = JdbcCatalogLoader(
            CatalogContext.create_from_options(Options({})))

        with patch('pypaimon.read.native_plan.native_plan', return_value=Plan([], 1)) as np:
            self.assertEqual(scan.plan().snapshot_id, 1)

        np.assert_called_once()
        fs.scan.assert_not_called()

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

    def test_scan_with_stats_preserves_native_empty_snapshot(self):
        fs = Mock(partition_key_predicate=None)
        scan = _scan(native_enabled=True, file_scanner=fs)
        native = Plan([], 7)
        with patch('pypaimon.read.native_plan.native_plan', return_value=native) as np:
            plan, stats = scan.scan_with_stats()
        self.assertEqual(plan.snapshot_id, 7)
        self.assertIsNone(stats)
        np.assert_called_once_with(
            scan.table, predicate=None, limit=None, projection=None,
            row_ranges=None)
        fs.scan_with_stats.assert_not_called()
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

    @patch(
        'pypaimon.filesystem.jindo_file_system_handler.JINDO_AVAILABLE', True)
    def test_native_plan_prefers_installed_jindo_for_oss(self):
        table = Mock(table_path='oss://bucket/table')
        table.catalog_environment.catalog_loader = FileSystemCatalogLoader(
            CatalogContext.create_from_options(Options({})))

        self.assertEqual(_catalog_options(table), {
            'metastore': 'filesystem',
            'fs.oss.impl': 'jindo',
        })

    @patch(
        'pypaimon.filesystem.jindo_file_system_handler.JINDO_AVAILABLE', False)
    def test_native_plan_uses_opendal_without_jindo(self):
        table = Mock(table_path='oss://bucket/table')
        table.catalog_environment.catalog_loader = FileSystemCatalogLoader(
            CatalogContext.create_from_options(Options({})))

        self.assertEqual(_catalog_options(table), {
            'metastore': 'filesystem',
        })

    @patch(
        'pypaimon.filesystem.jindo_file_system_handler.JINDO_AVAILABLE', True)
    def test_native_plan_respects_explicit_legacy_oss(self):
        table = Mock(table_path='oss://bucket/table')
        table.catalog_environment.catalog_loader = FileSystemCatalogLoader(
            CatalogContext.create_from_options(Options({
                'fs.oss.impl': 'legacy',
            })))

        self.assertEqual(_catalog_options(table), {
            'fs.oss.impl': 'legacy',
            'metastore': 'filesystem',
        })

    def test_catalog_options_reject_loader_subclass(self):
        class RoutedFileSystemLoader(FileSystemCatalogLoader):
            pass

        table = Mock()
        table.catalog_environment.catalog_loader = RoutedFileSystemLoader(
            CatalogContext.create_from_options(Options({})))

        with self.assertRaisesRegex(ValueError, 'exact built-in catalog loader'):
            _catalog_options(table)

    def test_blob_as_descriptor_is_forwarded_to_rust(self):
        for value in ('true', 'false', True, False):
            with self.subTest(value=value):
                options = {'blob-as-descriptor': value}
                table = SimpleNamespace(
                    table_schema=TableSchema(0, [], options=options))
                self.assertEqual(
                    json.loads(_resolved_schema_json(table))['options']['blob-as-descriptor'],
                    str(value).lower())

    def test_predicate_is_converted_for_rust(self):
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

    def test_resolved_schema_preserves_options_and_stringifies_values(self):
        options = {
            'source.split.target-size': '1 kb',
            'source.split.open-file-cost': '128 b',
            'deletion-vectors.merge-on-read': 'true',
            'scan.snapshot-id': '9',
            'scan.watermark': 200,
            'global-index.search-mode': 'detail',
            'scalar-index.search-mode': 'full',
            'vector-index.search-mode': 'fast',
            'full-text-index.search-mode': 'fast',
            'read.batch-size': 32,
            'custom.read-option': True,
            'removed.option': None,
        }
        table = SimpleNamespace(
            table_schema=TableSchema(0, [], options=options))
        self.assertEqual(json.loads(_resolved_schema_json(table))['options'], {
            'source.split.target-size': '1 kb',
            'source.split.open-file-cost': '128 b',
            'deletion-vectors.merge-on-read': 'true',
            'scan.snapshot-id': '9',
            'scan.watermark': '200',
            'global-index.search-mode': 'detail',
            'scalar-index.search-mode': 'full',
            'vector-index.search-mode': 'fast',
            'full-text-index.search-mode': 'fast',
            'read.batch-size': '32',
            'custom.read-option': 'true',
        })
        self.assertEqual(table.table_schema.options, options)

    def test_resolved_schema_preserves_timestamp_selectors(self):
        for key, value in (('scan.timestamp', '2026-09-22T00:00:00'),
                           ('scan.timestamp-millis', 1790035200000)):
            with self.subTest(key=key):
                options = {key: value}
                table = SimpleNamespace(
                    table_schema=TableSchema(0, [], options=options))
                resolved = json.loads(_resolved_schema_json(table))['options']
                self.assertEqual(resolved, {key: str(value)})
                self.assertEqual(table.table_schema.options, options)

    @unittest.skipIf(sys.version_info < (3, 8),
                     "importlib.metadata requires Python 3.8")
    def test_family_search_mode_version_gate(self):
        cases = {
            '0.3.0': False,
            '0.4.0': True,
            '0.4.0.dev20260808': False,
            '1.0.0': True,
        }
        for version, expected in cases.items():
            with self.subTest(version=version), patch(
                    'importlib.metadata.version', return_value=version):
                self.assertEqual(
                    native_family_search_modes_available(), expected)

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
        split._native_split = object()

        _restore_python_partition_paths(table, [split])

        self.assertEqual(
            data_file.file_path,
            '/warehouse/t/p=a/b/bucket-0/data.parquet',
        )
        self.assertIsNone(split._native_split)

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

    def test_rest_catalog_retains_response_for_native_reads(self):
        from pypaimon.api.api_response import GetTableResponse
        from pypaimon.catalog.rest.rest_catalog import RESTCatalog
        from pypaimon.common.identifier import Identifier
        from pypaimon.schema.schema import Schema

        catalog = RESTCatalog.__new__(RESTCatalog)
        catalog.context = CatalogContext.create_from_options(Options({'warehouse': 'test'}))
        catalog.create = Mock()
        identifier = Identifier.create('db', 't')
        response = GetTableResponse(
            'uuid', 't', '/warehouse/t', True, 3,
            Schema(fields=[DataField(4, 'id', AtomicType('INT'))]))
        metadata = catalog.to_table_metadata('db', response)
        catalog.load_table(identifier, Mock(), Mock(), lambda _: metadata)
        environment = catalog.create.call_args[0][3]
        saved = json.loads(environment.rest_table_response)
        self.assertEqual(saved['id'], 'uuid')
        self.assertTrue(saved['isExternal'])
        self.assertEqual(saved['schemaId'], 3)
        self.assertEqual(saved['schema']['fields'][0]['id'], 4)

    def test_rest_response_requires_native_support_and_matching_path(self):
        from pypaimon.catalog.catalog_environment import CatalogEnvironment
        from pypaimon.read.native_plan import _resolved_rest_table_response

        table = Mock(table_path='/warehouse/t')
        loader = RESTCatalogLoader(CatalogContext.create_from_options(Options({})))
        table.catalog_environment = CatalogEnvironment(
            catalog_loader=loader, rest_table_response='{"path": "/warehouse/t"}')
        with patch('pypaimon.read.native_plan.native_method_available', return_value=False):
            self.assertIsNone(_resolved_rest_table_response(table))
        with patch('pypaimon.read.native_plan.native_method_available', return_value=True):
            table.table_path = '/another/table'
            self.assertIsNone(_resolved_rest_table_response(table))
            table.table_path = '/warehouse/t'
            table.catalog_environment.rest_table_response = None
            self.assertIsNone(_resolved_rest_table_response(table))

    def test_rest_response_requires_matching_identity(self):
        from pypaimon.catalog.catalog_environment import CatalogEnvironment
        from pypaimon.common.identifier import Identifier
        from pypaimon.read.native_plan import _resolved_rest_table_response

        table = Mock(table_path='/warehouse/t', identifier=Identifier('db', 't', branch='dev'))
        loader = RESTCatalogLoader(CatalogContext.create_from_options(Options({})))
        table.catalog_environment = CatalogEnvironment(catalog_loader=loader)
        for identity, matches in [
                ({'name': 't'}, False),
                ({}, False),
                ({'name': 't$branch_dev'}, True),
                ({'name': 't$branch_dev', 'database': 'db'}, True),
                ({'name': 't$branch_dev', 'database': 'other'}, False)]:
            with self.subTest(identity=identity), patch(
                    'pypaimon.read.native_plan.native_method_available', return_value=True):
                response = json.dumps(dict(identity, path=table.table_path))
                table.catalog_environment.rest_table_response = response
                self.assertEqual(_resolved_rest_table_response(table), response if matches else None)

    def test_rest_native_builder_reuses_loaded_metadata(self):
        from pypaimon.catalog.catalog_environment import CatalogEnvironment
        from pypaimon.common.identifier import Identifier
        from pypaimon.read.native_plan import _native_read_builder

        response = json.dumps({'name': 't$branch_dev', 'path': '/warehouse/t',
                               'id': 'uuid', 'isExternal': False})
        loader = RESTCatalogLoader(CatalogContext.create_from_options(Options({
            'uri': 'http://localhost:1', 'warehouse': 'test', 'data-token.enabled': 'true'})))
        table = Mock()
        table.identifier = Identifier('db', 't', branch='dev')
        table.table_path = '/warehouse/t'
        table.current_branch.return_value = 'dev'
        table.catalog_environment = CatalogEnvironment(
            identifier=table.identifier, uuid='uuid', catalog_loader=loader,
            supports_version_management=True, rest_table_response=response)
        self.assertEqual(table.catalog_environment.copy(table.identifier).rest_table_response, response)
        resolved = '{"id": 2, "options": {"blob-as-descriptor": "true"}}'
        native_table = Mock()
        native_table.copy_with_resolved_schema.return_value = native_table
        native_table.branch.return_value = 'dev'
        fake_df = ModuleType('pypaimon_rust.datafusion')
        fake_df.Table = Mock()
        fake_df.Table.from_rest_response.return_value = native_table
        fake_df.PaimonCatalog = Mock()
        fake_module = ModuleType('pypaimon_rust')
        fake_module.datafusion = fake_df
        with patch.dict(sys.modules, {'pypaimon_rust': fake_module,
                                      'pypaimon_rust.datafusion': fake_df}), \
                patch('pypaimon.read.native_plan._resolved_schema_json', return_value=resolved):
            for _ in range(3):
                self.assertIs(_native_read_builder(table), native_table.new_read_builder.return_value)
        fake_df.PaimonCatalog.assert_not_called()
        fake_df.Table.from_rest_response.assert_called_once_with(
            response, database='db', table='t$branch_dev',
            rest_options=_catalog_options(table))
        self.assertEqual(native_table.copy_with_resolved_schema.call_args_list,
                         [call(resolved, branch='dev')] * 3)
        self.assertEqual(native_table.new_read_builder.call_count, 3)

    def test_native_rest_cache_invalidation_and_serialization(self):
        import pickle
        from pypaimon.read.native_plan import _NativeRestTableCache

        fake_df = ModuleType('pypaimon_rust.datafusion')
        fake_df.Table = Mock()
        fake_df.Table.from_rest_response.side_effect = lambda *args, **kwargs: object()
        cache = _NativeRestTableCache()
        with patch.dict(sys.modules, {'pypaimon_rust.datafusion': fake_df}):
            original = cache.get('response', 'db', 't', {'token': 'first'})
            self.assertIs(cache.get('response', 'db', 't', {'token': 'first'}), original)
            for response, db, table, options in [
                    ('response', 'db', 't', {'token': 'second'}),
                    ('new-response', 'db', 't', {'token': 'second'}),
                    ('new-response', 'db', 't$branch_dev', {'token': 'second'}),
                    ('new-response', 'other', 't$branch_dev', {'token': 'second'})]:
                replacement = cache.get(response, db, table, options)
                self.assertIsNot(replacement, original)
                original = replacement
            restored = pickle.loads(pickle.dumps(cache))
            self.assertEqual(restored._states, {})
            self.assertIsNot(restored.get(response, db, table, options), original)
            with patch('pypaimon.read.native_plan.os.getpid', return_value=-1):
                self.assertIsNot(cache.get(response, db, table, options), original)

    def test_native_rest_cache_lifetime_and_concurrent_access(self):
        import gc
        import weakref
        from concurrent.futures import ThreadPoolExecutor
        from pypaimon.read.native_plan import _NativeRestTableCache

        class NativeTable:
            pass

        fake_df = ModuleType('pypaimon_rust.datafusion')
        fake_df.Table = Mock()
        fake_df.Table.from_rest_response.side_effect = lambda *args, **kwargs: NativeTable()
        cache = _NativeRestTableCache()
        with patch.dict(sys.modules, {'pypaimon_rust.datafusion': fake_df}):
            with ThreadPoolExecutor(max_workers=4) as pool:
                tables = list(pool.map(lambda _: cache.get('response', 'db', 't', {}), range(8)))
            self.assertTrue(all(table is tables[0] for table in tables))
            fake_df.Table.from_rest_response.assert_called_once()
            reference = weakref.ref(tables[0])
            del tables
            gc.collect()
            self.assertIsNotNone(reference())
            del cache
            gc.collect()
            self.assertIsNone(reference())

    def test_native_rest_cache_concurrent_first_access_after_fork(self):
        import multiprocessing
        import os
        from threading import Event, Lock, Thread, current_thread
        from pypaimon.read.native_plan import _NativeRestTableCache

        if 'fork' not in multiprocessing.get_all_start_methods():
            self.skipTest('fork required')
        context = multiprocessing.get_context('fork')
        fake_df = ModuleType('pypaimon_rust.datafusion')
        fake_df.Table = Mock()
        fake_df.Table.from_rest_response.side_effect = lambda *args, **kwargs: object()
        cache = _NativeRestTableCache()
        held, release = Event(), Event()

        def hold_parent_lock():
            with cache._states[os.getpid()].lock:
                held.set()
                release.wait()

        def child(connection):
            creating, resume = Event(), Event()
            results = []
            fake_df.Table.from_rest_response.reset_mock()

            def new_lock():
                if current_thread().name == 'first':
                    creating.set()
                    assert resume.wait(5)
                return Lock()

            def access():
                results.append(cache.get('response', 'db', 't', {}))

            with patch('pypaimon.read.native_plan.Lock', side_effect=new_lock):
                first = Thread(target=access, name='first', daemon=True)
                second = Thread(target=access, name='second', daemon=True)
                first.start()
                assert creating.wait(5)
                second.start()
                second.join(2)
                second_completed = not second.is_alive()
                resume.set()
                first.join(2)
                connection.send((second_completed, not first.is_alive(),
                                 len(results) == 2 and results[0] is results[1],
                                 fake_df.Table.from_rest_response.call_count))
            connection.close()

        with patch.dict(sys.modules, {'pypaimon_rust.datafusion': fake_df}):
            cache.get('response', 'db', 't', {})
            holder = Thread(target=hold_parent_lock, daemon=True)
            holder.start()
            self.assertTrue(held.wait(5))
            receiving, sending = context.Pipe(duplex=False)
            process = context.Process(target=child, args=(sending,))
            try:
                process.start()
                sending.close()
                self.assertTrue(receiving.poll(10), 'child deadlocked on inherited lock')
                self.assertEqual(receiving.recv(), (True, True, True, 1))
                process.join(5)
                self.assertEqual(process.exitcode, 0)
            finally:
                if process.is_alive():
                    process.terminate()
                    process.join(5)
                receiving.close()
                sending.close()
                release.set()
                holder.join(5)

    def test_native_rest_cache_retries_failed_construction(self):
        from pypaimon.read.native_plan import _NativeRestTableCache

        fake_df = ModuleType('pypaimon_rust.datafusion')
        fake_df.Table = Mock()
        native_table = object()
        fake_df.Table.from_rest_response.side_effect = [RuntimeError('unavailable'), native_table]
        cache = _NativeRestTableCache()
        with patch.dict(sys.modules, {'pypaimon_rust.datafusion': fake_df}):
            with self.assertRaisesRegex(RuntimeError, 'unavailable'):
                cache.get('response', 'db', 't', {})
            self.assertIs(cache.get('response', 'db', 't', {}), native_table)

    def test_native_plan_threads_trimmed_keys_to_deserializer(self):
        # PK tables route through: the trimmed primary keys must reach the
        # deserializer so per-file min/max keys are decoded for merge-on-read.
        kfields = [object()]
        table = Mock(trimmed_primary_keys_fields=kfields)
        table.current_branch.return_value = 'main'
        table.table_schema = Mock(fields=[], partition_keys=[])
        table.partition_keys = []
        split = Mock()
        split.serialize.return_value = b'bytes'
        builder = Mock()
        builder.with_row_ranges.return_value = builder
        builder.new_scan.return_value.plan.return_value.splits.return_value = [split]
        builder.new_scan.return_value.plan.return_value.snapshot_id.return_value = 3

        with patch('pypaimon.read.native_plan._native_read_builder', return_value=builder), \
                patch('pypaimon.read.native_plan.deserialize_split_v1') as des:
            decoded = Mock()
            des.return_value = decoded
            result = native_plan(table, row_ranges=[(1, 2)])

        self.assertEqual(result.splits(), [decoded])
        self.assertIs(decoded._native_split, split)
        self.assertEqual(result.snapshot_id, 3)
        builder.with_row_ranges.assert_called_once_with([(1, 2)])
        des.assert_called_once_with(b'bytes', [], kfields)

    def test_native_plan_empty_snapshot_and_legacy_runtime(self):
        for snapshot_id, legacy in ((None, False), (7, False), (None, True)):
            with self.subTest(snapshot_id=snapshot_id, legacy=legacy):
                table = _scan(True, Mock()).table
                table.table_schema = Mock(fields=[], partition_keys=[])
                rust_plan = SimpleNamespace(splits=lambda: [])
                if not legacy:
                    rust_plan.snapshot_id = lambda: snapshot_id
                scan = SimpleNamespace(plan=lambda: rust_plan)
                builder = Mock()
                builder.new_scan.return_value = scan
                with patch('pypaimon.read.native_plan._native_read_builder', return_value=builder):
                    if legacy:
                        with self.assertRaisesRegex(RuntimeError, "empty plan's snapshot"):
                            native_plan(table)
                    else:
                        plan = native_plan(table)
                        self.assertEqual(plan.snapshot_id, snapshot_id)
                        self.assertEqual(plan.splits(), [])

    def test_native_plan_branch_resolution(self):
        table = _scan(True, Mock()).table
        table.table_schema = TableSchema(0, [])
        table.options = CoreOptions(Options({}))
        table.current_branch.return_value = 'b1'
        rust_plan = SimpleNamespace(splits=lambda: [], snapshot_id=lambda: 7)
        scan = Mock()
        scan.plan.return_value = rust_plan
        rt = Mock()
        rt.branch.return_value = 'b1'
        rt.new_read_builder.return_value.new_scan.return_value = scan
        with patch('pypaimon_rust.datafusion.Table', create=True) as native_table:
            native_table.from_resolved_schema.return_value = rt
            plan = native_plan(table)
            self.assertEqual(plan.snapshot_id, 7)
            scan.plan.assert_called_once_with()
            rt.branch.return_value = 'main'
            with self.assertRaisesRegex(RuntimeError, 'requested branch'):
                native_plan(table)

    def test_native_read_builder_requires_resolved_schema(self):
        for loader_type in (FileSystemCatalogLoader, RESTCatalogLoader):
            with self.subTest(loader=loader_type):
                table = _scan(True, Mock()).table
                table.table_schema = TableSchema(0, [], options={'read.batch-size': 32})
                table.options = CoreOptions(Options(table.table_schema.options))
                table.catalog_environment.catalog_loader = loader_type(
                    CatalogContext.create_from_options(Options({})))
                legacy_table = SimpleNamespace(
                    location=lambda: table.table_path, new_read_builder=Mock())
                with patch('pypaimon_rust.datafusion.Table', type('Table', (), {}), create=True), \
                        patch('pypaimon_rust.datafusion.PaimonCatalog') as catalog:
                    catalog.return_value.get_table.return_value = legacy_table
                    with self.assertRaises(AttributeError):
                        _native_read_builder(table)
                legacy_table.new_read_builder.assert_not_called()

    def test_explicit_row_ranges_are_forwarded(self):
        for ranges in ([], [Range(1, 2), Range(5, 8)]):
            with self.subTest(ranges=ranges):
                fs = Mock(partition_key_predicate=None)
                scan = _scan(True, fs)
                fs._row_ranges = ranges
                with patch('pypaimon.read.native_plan.native_plan',
                           return_value=Plan([], 3)) as native:
                    result = scan.plan()
                self.assertEqual(result.snapshot_id, 3)
                self.assertEqual(native.call_args[1]['row_ranges'],
                                 [(r.from_, r.to) for r in ranges])
                fs.scan.assert_not_called()

    def test_append_distribution_uses_native_order(self):
        for selection in ('idx_of_this_subtask', 'start_pos_of_this_subtask'):
            with self.subTest(selection=selection):
                fs = Mock(partition_key_predicate=None)
                scan = _scan(True, fs)
                setattr(fs, selection, 0)
                with patch('pypaimon.read.native_plan.native_plan',
                           return_value=Plan([], 3)) as native:
                    self.assertEqual(scan.plan().snapshot_id, 3)
                native.assert_called_once()
                fs.scan.assert_not_called()

    def test_watermark_forwarding_uses_native_plan(self):
        fs = Mock(partition_key_predicate=None)
        scan = _scan(True, fs)
        scan.table.options.options = Options({'scan.watermark': '200'})
        scan.table._applied_dynamic_options = {'scan.watermark': '200'}
        scan.table.schema_manager.latest.return_value.id = 2
        with patch('pypaimon.read.native_plan.native_plan',
                   return_value=Plan([], 1)) as native:
            self.assertEqual(scan.plan().snapshot_id, 1)
        native.assert_called_once()
        fs.scan.assert_not_called()

    def test_deletion_vectors_use_native_plan_for_bucket_layouts(self):
        for bucket_local in (False, True):
            for merge_on_read in (False, True):
                with self.subTest(bucket_local=bucket_local,
                                  merge_on_read=merge_on_read):
                    fs = Mock(partition_key_predicate=None)
                    scan = _scan(True, fs)
                    fs.deletion_vectors_enabled = True
                    scan.table.options.options = Options({
                        'index-file-in-data-file-dir': str(bucket_local).lower(),
                        'deletion-vectors.merge-on-read': str(merge_on_read).lower(),
                    })
                    with patch('pypaimon.read.native_plan.native_plan',
                               return_value=Plan([], 1)) as native:
                        self.assertEqual(scan.plan().snapshot_id, 1)
                    native.assert_called_once()
                    fs.scan.assert_not_called()

    @unittest.skipIf(sys.version_info < (3, 8),
                     "importlib.metadata requires Python 3.8")
    def test_runtime_version_comparison_preserves_patch_and_release_order(self):
        cases = [
            ('0.3.99', (0, 4, 0), False),
            ('0.4.0.dev1', (0, 4, 0), False),
            ('0.4.0rc1', (0, 4, 0), False),
            ('0.4.0', (0, 4, 0), True),
            ('0.4.0+local', (0, 4, 0), True),
            ('0.4.0.post1', (0, 4, 0), True),
            ('0.4.0', (0, 4, 1), False),
            ('0.4.1', (0, 4, 1), True),
            ('1.0.0', (0, 4, 0), True),
            ('unknown', (0, 4, 0), False),
        ]
        for version, minimum, expected in cases:
            with self.subTest(version=version, minimum=minimum), patch(
                    'importlib.metadata.version', return_value=version):
                self.assertEqual(native_version_at_least(*minimum), expected)

    def test_incremental_range_is_forwarded_without_timestamp_reinterpretation(self):
        fs = Mock(partition_key_predicate=None)
        scan = _scan(True, fs)
        scan.table.options.options = Options({'incremental-between-timestamp': '100,200'})
        scan.table._applied_dynamic_options = {'incremental-between-timestamp': '100,200'}
        scan._incremental_snapshot_range = (2, 4)
        with patch('pypaimon.read.native_plan.native_plan',
                   return_value=Plan([], 4)) as native:
            self.assertEqual(scan.plan().snapshot_id, 4)
        self.assertEqual(native.call_args[1]['incremental_range'], (2, 4))
        fs.scan.assert_not_called()

    def test_native_plan_forwards_explicit_incremental_mode(self):
        table = _scan(True, Mock()).table
        table.table_schema = Mock(fields=[], partition_keys=[])
        rust_plan = SimpleNamespace(splits=lambda: [], snapshot_id=lambda: 4)
        rust_scan = Mock()
        rust_scan.plan.return_value = rust_plan
        builder = Mock()
        builder.new_incremental_scan.return_value = rust_scan
        with patch('pypaimon.read.native_plan.native_runtime_available',
                   return_value=True), patch(
                'pypaimon.read.native_plan._native_read_builder',
                return_value=builder):
            plan = native_plan(
                table, incremental_range=(2, 4),
                incremental_mode='changelog')
        self.assertEqual(plan.snapshot_id, 4)
        builder.new_incremental_scan.assert_called_once_with(
            2, 4, 'changelog')

        with self.assertRaisesRegex(ValueError, 'incremental_range'):
            native_plan(table, incremental_mode='changelog')

    def test_incremental_window_outside_snapshots_is_terminal_empty(self):
        fs = Mock(partition_key_predicate=None)
        scan = _scan(True, fs)
        scan.table.options.options = Options({'incremental-between-timestamp': '100,200'})
        scan._incremental_snapshot_range = None
        with patch('pypaimon.read.native_plan.native_plan') as native:
            plan = scan.plan()
        self.assertEqual(plan.splits(), [])
        self.assertIsNone(plan.snapshot_id)
        native.assert_not_called()
        fs.scan.assert_not_called()

    def test_primary_key_shard_defers_limit_until_after_bucket_selection(self):
        fs = Mock(partition_key_predicate=None)
        scan = _scan(True, fs)
        scan.table.is_primary_key_table = True
        scan.limit = 1
        fs.idx_of_this_subtask, fs.number_of_para_subtasks = 1, 2
        splits = [Mock(bucket=0), Mock(bucket=1)]
        fs._apply_push_down_limit.side_effect = lambda selected: selected
        with patch('pypaimon.read.native_plan.native_plan', return_value=Plan(splits, 3)) as native:
            self.assertEqual(scan.plan().splits(), [splits[1]])
        self.assertIsNone(native.call_args[1]['limit'])
        fs._apply_push_down_limit.assert_called_once_with([splits[1]])
        fs.scan.assert_not_called()

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
