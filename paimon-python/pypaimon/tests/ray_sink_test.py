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
import tempfile
import unittest
from unittest.mock import Mock, patch

import pyarrow as pa
import pytest
from ray.data._internal.execution.interfaces import TaskContext

from pypaimon import CatalogFactory, Schema
from pypaimon.write.ray_datasink import (
    PaimonDatasink,
    _consume_write_results,
    _cast_binary_to_table_schema,
    write_paimon_dataset,
)
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.table_write import TableWrite


class RaySinkTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.warehouse_path = os.path.join(self.temp_dir, "warehouse")
        os.makedirs(self.warehouse_path, exist_ok=True)

        catalog_options = {
            "warehouse": self.warehouse_path
        }
        self.catalog = CatalogFactory.create(catalog_options)
        self.catalog.create_database("test_db", ignore_if_exists=True)

        pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('name', pa.string()),
            ('value', pa.float64())
        ])

        schema = Schema.from_pyarrow_schema(
            pa_schema=pa_schema,
            partition_keys=None,
            primary_keys=['id'],
            options={'bucket': '2'},  # Use fixed bucket mode for testing
            comment='test table'
        )

        self.table_identifier = "test_db.test_table"
        self.catalog.create_table(self.table_identifier, schema, ignore_if_exists=False)
        self.table = self.catalog.get_table(self.table_identifier)
        self.pk_pa_schema = pa_schema

    def tearDown(self):
        import shutil
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    @staticmethod
    def _data_files_under(table):
        table_path = table.file_io.to_filesystem_path(table.table_path)
        data_files = []
        for root, _, files in os.walk(table_path):
            for file_name in files:
                if file_name.endswith(('.parquet', '.blob')) or '.vector.' in file_name:
                    data_files.append(os.path.join(root, file_name))
        return data_files

    def test_binary_restoration_uses_table_contract_and_preserves_strings(self):
        batch = pa.table({
            'blob': pa.array([b'x', None], type=pa.binary()),
            'bytes': pa.array([b'y', b'z'], type=pa.large_binary()),
            'text': pa.array(['中文', ''], type=pa.large_string()),
        }).replace_schema_metadata({b'source': b'ray'})
        target = pa.schema([('blob', pa.large_binary()), ('bytes', pa.binary()), ('text', pa.string())])
        result = _cast_binary_to_table_schema(batch, target)
        self.assertEqual(result.to_pydict(), batch.to_pydict())
        self.assertEqual(result.schema.field('blob').type, pa.large_binary())
        self.assertEqual(result.schema.field('bytes').type, pa.binary())
        self.assertEqual(result.schema.field('text').type, pa.large_string())
        self.assertEqual(result.schema.metadata, batch.schema.metadata)

    def test_init_and_serialization(self):
        """Test initialization, serialization, and table name."""
        datasink = PaimonDatasink(self.table, overwrite=False)
        self.assertEqual(datasink.table, self.table)
        self.assertFalse(datasink.overwrite)
        self.assertIsNone(datasink.static_partition)
        self.assertIsNone(datasink._writer_builder)
        self.assertEqual(datasink._table_name, "test_db.test_table")

        datasink_overwrite = PaimonDatasink(self.table, overwrite=True)
        self.assertTrue(datasink_overwrite.overwrite)

        datasink_partition_overwrite = PaimonDatasink(
            self.table, static_partition={'dt': '2024-01-01'})
        self.assertFalse(datasink_partition_overwrite.overwrite)
        self.assertEqual(
            datasink_partition_overwrite.static_partition,
            {'dt': '2024-01-01'},
        )

        # Test serialization
        datasink._writer_builder = Mock()
        state = datasink.__getstate__()
        self.assertIn('table', state)
        self.assertIn('overwrite', state)
        self.assertIn('static_partition', state)
        self.assertIn('_writer_builder', state)

        new_datasink = PaimonDatasink.__new__(PaimonDatasink)
        new_datasink.__setstate__(state)
        self.assertEqual(new_datasink.table, self.table)
        self.assertFalse(new_datasink.overwrite)
        self.assertIsNone(new_datasink.static_partition)

    def test_table_and_writer_builder_serializable(self):
        import pickle
        try:
            pickled_table = pickle.dumps(self.table)
            unpickled_table = pickle.loads(pickled_table)
            self.assertIsNotNone(unpickled_table)
            builder = unpickled_table.new_batch_write_builder()
            self.assertIsNotNone(builder)
        except Exception as e:
            self.fail(f"Table object is not serializable: {e}")
        
        writer_builder = self.table.new_batch_write_builder()
        try:
            pickled_builder = pickle.dumps(writer_builder)
            unpickled_builder = pickle.loads(pickled_builder)
            self.assertIsNotNone(unpickled_builder)
            table_write = unpickled_builder.new_write()
            self.assertIsNotNone(table_write)
            table_write.close()
        except Exception as e:
            self.fail(f"WriterBuilder is not serializable: {e}")
        
        overwrite_builder = self.table.new_batch_write_builder().overwrite()
        try:
            pickled_overwrite = pickle.dumps(overwrite_builder)
            unpickled_overwrite = pickle.loads(pickled_overwrite)
            self.assertIsNotNone(unpickled_overwrite)
            # static_partition is a dict, empty dict {} means overwrite all partitions
            self.assertIsNotNone(unpickled_overwrite.static_partition)
            self.assertIsInstance(unpickled_overwrite.static_partition, dict)
        except Exception as e:
            self.fail(f"Overwrite WriterBuilder is not serializable: {e}")

    def test_write_builder_new_write_carries_static_partition(self):
        batch_write = (
            self.table
            .new_batch_write_builder()
            .overwrite({'dt': '2024-01-01'})
            .new_write()
        )
        try:
            self.assertEqual(batch_write.static_partition, {'dt': '2024-01-01'})
        finally:
            batch_write.close()

        stream_builder = self.table.new_stream_write_builder()
        self.assertFalse(hasattr(stream_builder, 'overwrite'))
        stream_write = stream_builder.new_write()
        try:
            self.assertIsNone(stream_write.static_partition)
        finally:
            stream_write.close()

    def test_write_options_are_applied_before_repartitioning(self):
        table = self.table.copy({
            'commit.metadata.source': 'copied',
            'target-file-size': '8 mb',
        })
        dataset = Mock()
        with patch('pypaimon.ray.shuffle.maybe_apply_repartition',
                   return_value=dataset) as repartition:
            write_paimon_dataset(
                dataset, table, options={'commit.metadata.source': 'explicit'})

        effective_table = repartition.call_args[0][1]
        effective_options = effective_table.options.options.to_map()
        self.assertEqual(effective_options['commit.metadata.source'], 'explicit')
        self.assertEqual(effective_options['target-file-size'], '8 mb')
        self.assertEqual(table.options.options.to_map()['commit.metadata.source'], 'copied')
        sink = dataset.write_datasink.call_args[0][0]
        self.assertIs(sink.table, effective_table)
        self.assertIsNone(sink._options)

    def test_write_options_are_applied_before_postpone_dispatch(self):
        identifier = 'test_db.test_postpone_effective_options'
        schema = Schema.from_pyarrow_schema(
            self.pk_pa_schema, primary_keys=['id'], options={
                'bucket': '-2',
                'postpone.batch-write-fixed-bucket': 'false',
                'postpone.default-bucket-num': '1',
            })
        self.catalog.create_table(identifier, schema, False)
        table = self.catalog.get_table(identifier)
        dataset = Mock()
        with patch('pypaimon.write.backend_dispatch.create_postpone_bucket_planner') as planner, \
                patch('pypaimon.write.ray_datasink._write_primary_key_groups') as grouped:
            planner.return_value.current_plan.return_value.contains.return_value = True
            write_paimon_dataset(dataset, table, options={
                'postpone.batch-write-fixed-bucket': 'true',
                'postpone.default-bucket-num': '3',
            })

        planner.assert_called_once()
        effective_table = planner.call_args[0][0]
        self.assertTrue(effective_table.options.postpone_batch_write_fixed_bucket())
        self.assertEqual(effective_table.options.postpone_default_bucket_num(), 3)
        self.assertFalse(table.options.postpone_batch_write_fixed_bucket())
        self.assertIs(grouped.call_args[0][1], effective_table)
        self.assertIsNone(grouped.call_args[1]['options'])

    def test_actor_branch_options_preserve_reload_baseline(self):
        from pypaimon.schema.schema_change import SchemaChange
        from pypaimon.write.ray_datasink import PaimonCommitterActor

        identifier = 'test_db.test_actor_branch_options'
        schema = Schema.from_pyarrow_schema(self.pk_pa_schema, options={
            'bucket': '1',
            'bucket-key': 'id',
            'write.native.enabled': 'false',
            'commit.native.enabled': 'false',
        })
        self.catalog.create_table(identifier, schema, False)
        self.catalog.create_branch(identifier, 'dev')
        branch_identifier = identifier + '$branch_dev'
        self.catalog.alter_table(
            branch_identifier, [SchemaChange.set_option('bucket', '3')], False)
        table = self.catalog.get_table(identifier).copy({
            'commit.metadata.source': 'copied',
            'commit.metadata.removed': 'copied',
        })
        dataset = Mock()
        dataset.write_datasink.side_effect = lambda sink, **kwargs: sink.on_write_complete([])
        with patch('pypaimon.ray.shuffle.maybe_apply_repartition',
                   return_value=dataset) as repartition:
            write_paimon_dataset(dataset, table, commit_mode='actor', options={
                'branch': 'dev',
                'commit.metadata.source': 'explicit',
                'commit.metadata.removed': None,
            })

        sink = dataset.write_datasink.call_args[0][0]
        self.assertEqual(sink._table_identifier, identifier)
        effective_table = repartition.call_args[0][1]
        self.assertIs(sink.table, effective_table)
        self.assertEqual(effective_table.identifier.get_full_name(), branch_identifier)
        self.assertEqual(effective_table.options.bucket(), 1)
        self.assertEqual(sink._options['commit.metadata.source'], 'explicit')
        self.assertIsNone(sink._options['commit.metadata.removed'])
        with patch('pypaimon.write.ray_datasink._is_first_actor_incarnation', return_value=False):
            committer = PaimonCommitterActor(
                sink._catalog_options, sink._table_identifier, options=sink._options)
        actor_table = committer._target_table()
        self.assertEqual(actor_table.identifier.get_full_name(), branch_identifier)
        self.assertEqual(actor_table.table_schema.id, effective_table.table_schema.id)
        self.assertEqual(actor_table.options.options.to_map(),
                         effective_table.options.options.to_map())
        self.assertNotIn('commit.metadata.removed', actor_table.options.options.to_map())
        self.assertEqual(table.identifier.get_full_name(), identifier)
        self.assertEqual(table.options.options.to_map()['commit.metadata.source'], 'copied')
        self.assertEqual(table.options.options.to_map()['commit.metadata.removed'], 'copied')

    def test_actor_datasink_preserves_pre_copied_table_origin(self):
        from pypaimon.schema.schema_change import SchemaChange
        from pypaimon.write.ray_datasink import PaimonActorDatasink, PaimonCommitterActor

        identifier = 'test_db.test_actor_copied_origin'
        schema = Schema.from_pyarrow_schema(self.pk_pa_schema, options={
            'bucket': '1',
            'bucket-key': 'id',
            'write.native.enabled': 'false',
            'commit.native.enabled': 'false',
        })
        self.catalog.create_table(identifier, schema, False)
        self.catalog.create_branch(identifier, 'dev')
        branch_identifier = identifier + '$branch_dev'
        self.catalog.alter_table(
            branch_identifier, [SchemaChange.set_option('bucket', '3')], False)
        source = self.catalog.get_table(identifier)
        copied = source.copy({'branch': 'dev', 'commit.metadata.removed': 'first'})
        copied = copied.copy({'commit.metadata.source': 'copied'})
        copied = copied.copy_without_time_travel({'commit.metadata.removed': None})
        sink = PaimonActorDatasink(
            copied, {'warehouse': self.warehouse_path}, branch_identifier,
            options={'commit.metadata.source': 'explicit'})

        self.assertEqual(sink._table_identifier, identifier)
        self.assertEqual(source.identifier.get_full_name(), identifier)
        self.assertEqual(copied.identifier.get_full_name(), branch_identifier)
        with patch('pypaimon.write.ray_datasink._is_first_actor_incarnation', return_value=False):
            committer = PaimonCommitterActor(
                sink._catalog_options, sink._table_identifier, options=sink._options)
        actor_table = committer._target_table()
        writer_table = sink._target_table()
        self.assertEqual(actor_table.identifier.get_full_name(), branch_identifier)
        self.assertEqual(actor_table.options.bucket(), 1)
        self.assertEqual(actor_table.table_schema, writer_table.table_schema)
        self.assertEqual(actor_table.options.options.to_map()['commit.metadata.source'], 'explicit')
        self.assertNotIn('commit.metadata.removed', actor_table.options.options.to_map())

    def test_actor_datasink_keeps_explicit_identifier_for_path_table(self):
        from pypaimon.table.file_store_table import FileStoreTable
        from pypaimon.write.ray_datasink import PaimonActorDatasink

        identifier = 'test_db.test_actor_path_table'
        schema = Schema.from_pyarrow_schema(self.pk_pa_schema, options={
            'write.native.enabled': 'false',
            'commit.native.enabled': 'false',
        })
        self.catalog.create_table(identifier, schema, False)
        table = FileStoreTable.from_path(self.catalog.get_table(identifier).table_path)
        copied = table.copy({'commit.metadata.source': 'copied'})
        copied = copied.copy_without_time_travel({'target-file-size': '8 mb'})
        sink = PaimonActorDatasink(copied, {'warehouse': self.warehouse_path}, identifier)

        self.assertEqual(sink._table_identifier, identifier)

    def _new_actor_bucket_sink(self, name, options=None):
        from pypaimon.write.ray_datasink import PaimonActorDatasink

        identifier = 'test_db.' + name
        schema = Schema.from_pyarrow_schema(pa.schema([
            ('id', pa.int32()), ('other_id', pa.int64()), ('value', pa.string()),
        ]), options={
            'bucket': '3',
            'bucket-key': 'id,other_id',
            'write.native.enabled': 'false',
            'commit.native.enabled': 'false',
        })
        self.catalog.create_table(identifier, schema, False)
        table = self.catalog.get_table(identifier)
        sink = PaimonActorDatasink(
            table, {'warehouse': self.warehouse_path}, identifier, options=options)
        return table, sink

    def _start_actor_locally(self, sink):
        from pypaimon.write.ray_datasink import PaimonCommitterActor

        actor_arguments = []

        def create_actor(**kwargs):
            actor_arguments.append(kwargs)
            committer = PaimonCommitterActor(**kwargs)
            handle = Mock()
            handle.dedup_base.remote.side_effect = committer.dedup_base
            return handle

        with patch('pypaimon.write.ray_datasink.ray.remote') as remote, \
                patch('pypaimon.write.ray_datasink.ray.get', side_effect=lambda value: value), \
                patch('pypaimon.write.ray_datasink._is_first_actor_incarnation', return_value=False):
            remote.return_value.options.return_value.remote.side_effect = create_actor
            sink.on_write_start()
        return actor_arguments[0]

    def test_actor_rejects_changed_bucket_configuration(self):
        from pypaimon.schema.data_types import AtomicType
        from pypaimon.schema.schema_change import SchemaChange

        changes = [
            ('count', SchemaChange.set_option('bucket', '5')),
            ('keys', SchemaChange.set_option('bucket-key', 'value')),
            ('order', SchemaChange.set_option('bucket-key', 'other_id,id')),
            ('type', SchemaChange.update_column_type('id', AtomicType('BIGINT'))),
            ('function', SchemaChange.set_option('bucket-function.type', 'hive')),
        ]
        for label, change in changes:
            with self.subTest(change=label):
                table, sink = self._new_actor_bucket_sink('test_actor_bucket_drift_' + label)
                self.catalog.alter_table(table.identifier.get_full_name(), [change], False)
                with self.assertRaisesRegex(ValueError, 'bucket configuration.*writer'):
                    self._start_actor_locally(sink)
                self.assertIsNone(table.snapshot_manager().get_latest_snapshot())

    def test_actor_rechecks_bucket_configuration_on_reconstruction(self):
        from pypaimon.schema.schema_change import SchemaChange
        from pypaimon.write.ray_datasink import PaimonCommitterActor

        table, sink = self._new_actor_bucket_sink('test_actor_reconstructed_bucket_drift')
        actor_arguments = self._start_actor_locally(sink)
        self.catalog.alter_table(
            table.identifier.get_full_name(), [SchemaChange.set_option('bucket', '5')], False)
        with patch('pypaimon.write.ray_datasink._is_first_actor_incarnation', return_value=False):
            with self.assertRaisesRegex(ValueError, 'bucket configuration.*writer'):
                PaimonCommitterActor(**actor_arguments)
        self.assertIsNone(table.snapshot_manager().get_latest_snapshot())

    def test_actor_serialization_keeps_writer_bucket_configuration(self):
        import pickle

        from pypaimon.schema.schema_change import SchemaChange
        from pypaimon.write.ray_datasink import PaimonActorDatasink

        for legacy in (False, True):
            with self.subTest(legacy=legacy):
                table, sink = self._new_actor_bucket_sink(
                    'test_actor_serialized_bucket_config_' + str(legacy))
                state = sink.__getstate__()
                if legacy:
                    state.pop('_expected_bucket_config', None)
                serialized = pickle.dumps(state)
                self.catalog.alter_table(
                    table.identifier.get_full_name(), [SchemaChange.set_option('bucket', '5')], False)
                restored = PaimonActorDatasink.__new__(PaimonActorDatasink)
                restored.__setstate__(pickle.loads(serialized))
                with self.assertRaisesRegex(ValueError, 'bucket configuration.*writer'):
                    self._start_actor_locally(restored)
                self.assertIsNone(table.snapshot_manager().get_latest_snapshot())

    def test_actor_bucket_check_uses_effective_options(self):
        from pypaimon.schema.schema_change import SchemaChange

        table, sink = self._new_actor_bucket_sink(
            'test_actor_effective_bucket_config', options={'bucket-key': 'value'})
        self.catalog.alter_table(
            table.identifier.get_full_name(), [SchemaChange.set_option('bucket-key', 'other_id')], False)
        self._start_actor_locally(sink)

    def test_actor_bucket_check_allows_unrelated_metadata_changes(self):
        from pypaimon.schema.schema_change import SchemaChange

        table, sink = self._new_actor_bucket_sink('test_actor_unrelated_metadata')
        self.catalog.alter_table(
            table.identifier.get_full_name(), [SchemaChange.set_option('target-file-size', '8 mb')], False)
        self._start_actor_locally(sink)

    def test_actor_bucket_check_allows_equivalent_layout(self):
        from pypaimon.schema.schema_change import Move, SchemaChange

        table, sink = self._new_actor_bucket_sink('test_actor_equivalent_layout')
        self.catalog.alter_table(table.identifier.get_full_name(), [
            SchemaChange.update_column_position(Move.first('other_id')),
            SchemaChange.update_column_comment('id', 'updated comment'),
            SchemaChange.set_option('bucket-function.type', ' DEFAULT '),
        ], False)
        self._start_actor_locally(sink)

    def test_actor_bucket_check_ignores_nested_field_metadata(self):
        from pypaimon.schema.schema_change import SchemaChange
        from pypaimon.write.ray_datasink import PaimonActorDatasink

        identifier = 'test_db.test_actor_nested_metadata'
        schema = Schema.from_pyarrow_schema(pa.schema([
            ('id', pa.struct([('inner_id', pa.int32())])), ('value', pa.string()),
        ]), options={
            'bucket': '3', 'bucket-key': 'id',
            'write.native.enabled': 'false', 'commit.native.enabled': 'false',
        })
        self.catalog.create_table(identifier, schema, False)
        table = self.catalog.get_table(identifier)
        sink = PaimonActorDatasink(table, {'warehouse': self.warehouse_path}, identifier)
        self.catalog.alter_table(identifier, [
            SchemaChange.update_column_comment(['id', 'inner_id'], 'updated comment'),
        ], False)
        self._start_actor_locally(sink)

    def test_actor_failed_startup_kills_handle_and_preserves_error(self):
        for kill_fails in (False, True):
            with self.subTest(kill_fails=kill_fails):
                _, sink = self._new_actor_bucket_sink('test_actor_failed_startup_' + str(kill_fails))
                error = ValueError('bucket configuration does not match writer')
                handle = Mock()
                with patch('pypaimon.write.ray_datasink.ray.remote') as remote, \
                        patch('pypaimon.write.ray_datasink.ray.get', side_effect=error), \
                        patch('pypaimon.write.ray_datasink.ray.kill') as kill:
                    remote.return_value.options.return_value.remote.return_value = handle
                    if kill_fails:
                        kill.side_effect = RuntimeError('kill failed')
                    with self.assertRaises(ValueError) as raised:
                        sink.on_write_start()
                    self.assertIs(raised.exception, error)
                    kill.assert_called_once_with(handle)
                    self.assertIsNone(sink._actor)

    def test_on_write_start(self):
        """Test on_write_start with normal and overwrite modes."""
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()
        self.assertIsNotNone(datasink._writer_builder)
        self.assertFalse(datasink._writer_builder.static_partition)

        datasink_overwrite = PaimonDatasink(self.table, overwrite=True)
        datasink_overwrite.on_write_start()
        self.assertIsNotNone(datasink_overwrite._writer_builder.static_partition)

        datasink_partition_overwrite = PaimonDatasink(
            self.table, static_partition={'dt': '2024-01-01'})
        datasink_partition_overwrite.on_write_start()
        self.assertEqual(
            datasink_partition_overwrite._writer_builder.static_partition,
            {'dt': '2024-01-01'},
        )

    def test_write(self):
        """Test write method: empty blocks, multiple blocks, error handling, and resource cleanup."""
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()
        ctx = Mock(spec=TaskContext)

        # Test empty block
        empty_table = pa.table({
            'id': pa.array([], type=pa.int64()),
            'name': pa.array([], type=pa.string()),
            'value': pa.array([], type=pa.float64())
        }, schema=self.pk_pa_schema)
        result = datasink.write([empty_table], ctx)
        self.assertEqual(result, [])

        # Test single and multiple blocks
        single_block = pa.table({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'value': [1.1, 2.2, 3.3]
        }, schema=self.pk_pa_schema)
        result = datasink.write([single_block], ctx)
        self.assertIsInstance(result, list)
        if result:
            self.assertTrue(all(isinstance(msg, CommitMessage) for msg in result))

        block1 = pa.table({
            'id': [4, 5],
            'name': ['David', 'Eve'],
            'value': [4.4, 5.5]
        }, schema=self.pk_pa_schema)
        block2 = pa.table({
            'id': [6, 7],
            'name': ['Frank', 'Grace'],
            'value': [6.6, 7.7]
        }, schema=self.pk_pa_schema)
        result = datasink.write([block1, block2], ctx)
        self.assertIsInstance(result, list)
        if result:
            self.assertTrue(all(isinstance(msg, CommitMessage) for msg in result))

        # Test that write creates WriteBuilder on worker (not using driver's builder)
        with patch.object(self.table, 'new_batch_write_builder') as mock_builder:
            mock_write_builder = Mock()
            mock_write_builder.overwrite.return_value = mock_write_builder
            mock_write = Mock()
            mock_write.prepare_commit.return_value = []
            mock_write_builder.new_write.return_value = mock_write
            mock_builder.return_value = mock_write_builder

            data_table = pa.table({
                'id': [1],
                'name': ['Alice'],
                'value': [1.1]
            })
            datasink.write([data_table], ctx)
            mock_builder.assert_called_once()

        partition_datasink = PaimonDatasink(
            self.table, static_partition={'dt': '2024-01-01'})
        with patch.object(self.table, 'new_batch_write_builder') as mock_builder:
            mock_write_builder = Mock()
            mock_write_builder.overwrite.return_value = mock_write_builder
            mock_write = Mock()
            mock_write.prepare_commit.return_value = []
            mock_write_builder.new_write.return_value = mock_write
            mock_builder.return_value = mock_write_builder

            data_table = pa.table({
                'id': [1],
                'name': ['Alice'],
                'value': [1.1]
            })
            partition_datasink.write([data_table], ctx)
            mock_write_builder.overwrite.assert_called_once_with(
                {'dt': '2024-01-01'})

        invalid_table = pa.table({
            'wrong_column': [1, 2, 3]
        })
        with self.assertRaises(Exception):
            datasink.write([invalid_table], ctx)

        with patch.object(self.table, 'new_batch_write_builder') as mock_builder:
            mock_write_builder = Mock()
            mock_write_builder.overwrite.return_value = mock_write_builder
            mock_write = Mock()
            mock_write.write_arrow.side_effect = Exception("Write error")
            mock_write_builder.new_write.return_value = mock_write
            mock_builder.return_value = mock_write_builder

            data_table = pa.table({
                'id': [1],
                'name': ['Alice'],
                'value': [1.1]
            })
            with self.assertRaises(Exception):
                datasink.write([data_table], ctx)
            mock_write.abort.assert_called_once()
            mock_write.close.assert_not_called()

        with patch.object(self.table, 'new_batch_write_builder') as mock_builder:
            mock_write_builder = Mock()
            mock_write_builder.overwrite.return_value = mock_write_builder
            mock_write = Mock()
            mock_write.prepare_commit.return_value = [Mock(spec=CommitMessage)]
            mock_write.close.side_effect = Exception("Close error")
            mock_write_builder.new_write.return_value = mock_write
            mock_builder.return_value = mock_write_builder

            data_table = pa.table({
                'id': [1],
                'name': ['Alice'],
                'value': [1.1]
            })
            with self.assertRaises(Exception):
                datasink.write([data_table], ctx)
            mock_write.prepare_commit.assert_called_once()
            mock_write.abort.assert_called_once()

    def test_postpone_worker_uses_driver_bucket_plan_without_manifest_scan(self):
        from pypaimon.execution.python.write.postpone_bucket import PythonPostponeBucketPlanner
        from pypaimon.write.postpone_bucket import PostponeBucketPlan

        pa_schema = pa.schema([
            pa.field('id', pa.int64(), nullable=False),
            ('name', pa.string()),
            ('value', pa.float64()),
        ])
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=['id'],
            options={
                'bucket': '-2',
            },
        )
        identifier = 'test_db.test_postpone_worker_plan'
        self.catalog.create_table(identifier, schema, False)
        table = self.catalog.get_table(identifier)
        datasink = PaimonDatasink(
            table,
            postpone_bucket_plan=PostponeBucketPlan({(): 2}),
        )
        data = pa.Table.from_pydict({
            'id': list(range(20)),
            'name': ['name-{}'.format(i) for i in range(20)],
            'value': [float(i) for i in range(20)],
        }, schema=pa_schema)

        with patch.object(
            PythonPostponeBucketPlanner,
            '_load_bucket_metadata',
            side_effect=AssertionError("worker must not scan manifests"),
        ) as load:
            messages = datasink.write([data], Mock(spec=TaskContext))

        load.assert_not_called()
        self.assertEqual({0, 1}, {message.bucket for message in messages})
        self.assertEqual({2}, {message.total_buckets for message in messages})

    @pytest.mark.python_write
    def test_write_failure_preserves_prepared_dedicated_files(self):
        from pypaimon.execution.python.write.writer.dedicated_format_writer import DedicatedFormatWriter

        pa_schema = pa.schema([
            ('id', pa.int32()),
            ('payload', pa.large_binary()),
        ])
        schema = Schema.from_pyarrow_schema(pa_schema, options={
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
        })
        table_identifier = "test_db.test_blob_close_failure"
        self.catalog.create_table(table_identifier, schema, False)
        table = self.catalog.get_table(table_identifier)

        datasink = PaimonDatasink(table, overwrite=False)
        datasink.on_write_start()
        ctx = Mock(spec=TaskContext)
        data_table = pa.Table.from_pydict({
            'id': [1, 2, 3],
            'payload': [b'a', b'b', b'c'],
        }, schema=pa_schema)

        original_close_current_writers = DedicatedFormatWriter._close_current_writers
        close_current_calls = {'count': 0}

        def fail_during_close(writer):
            close_current_calls['count'] += 1
            if close_current_calls['count'] == 1:
                return original_close_current_writers(writer)
            raise RuntimeError("Close error")

        with patch.object(DedicatedFormatWriter, '_close_current_writers', fail_during_close):
            with self.assertRaisesRegex(RuntimeError, "Close error"):
                datasink.write([data_table], ctx)

        self.assertEqual(close_current_calls['count'], 2)
        self.assertEqual({'.parquet', '.blob'},
                         {os.path.splitext(path)[1] for path in self._data_files_under(table)})
        self.assertIsNone(table.snapshot_manager().get_latest_snapshot())

    def test_on_write_complete(self):
        from ray.data.datasource.datasink import WriteResult

        # Test empty messages
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()
        write_result = WriteResult(
            num_rows=0,
            size_bytes=0,
            write_returns=[[], []]
        )
        datasink.on_write_complete(write_result)

        # Empty overwrite must still reach TableCommit so overwrite semantics
        # can delete the target range.
        datasink = PaimonDatasink(self.table, overwrite=True)
        datasink.on_write_start()
        write_result = WriteResult(
            num_rows=0,
            size_bytes=0,
            write_returns=[[], []]
        )
        mock_commit = Mock()
        datasink._writer_builder.new_commit = Mock(return_value=mock_commit)
        datasink.on_write_complete(write_result)

        mock_commit.commit.assert_called_once_with([])
        mock_commit.close.assert_called_once()

        datasink = PaimonDatasink(self.table, static_partition={'dt': '2024-01-01'})
        datasink.on_write_start()
        write_result = WriteResult(
            num_rows=0,
            size_bytes=0,
            write_returns=[[], []]
        )
        mock_commit = Mock()
        datasink._writer_builder.new_commit = Mock(return_value=mock_commit)
        datasink.on_write_complete(write_result)

        mock_commit.commit.assert_called_once_with([])
        mock_commit.close.assert_called_once()

        # Test with messages and filtering empty messages
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()
        commit_msg1 = Mock(spec=CommitMessage)
        commit_msg1.is_empty.return_value = False
        commit_msg2 = Mock(spec=CommitMessage)
        commit_msg2.is_empty.return_value = False
        empty_msg = Mock(spec=CommitMessage)
        empty_msg.is_empty.return_value = True

        write_result = WriteResult(
            num_rows=0,
            size_bytes=0,
            write_returns=[[commit_msg1], [commit_msg2], [empty_msg]]
        )

        mock_commit = Mock()
        datasink._writer_builder.new_commit = Mock(return_value=mock_commit)
        datasink.on_write_complete(write_result)

        mock_commit.commit.assert_called_once()
        commit_args = mock_commit.commit.call_args[0][0]
        self.assertEqual(len(commit_args), 2)  # Empty message filtered out
        mock_commit.close.assert_called_once()

        # Test commit failure: abort should not be called
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()
        commit_msg1 = Mock(spec=CommitMessage)
        commit_msg1.is_empty.return_value = False
        commit_msg2 = Mock(spec=CommitMessage)
        commit_msg2.is_empty.return_value = False

        write_result = WriteResult(
            num_rows=0,
            size_bytes=0,
            write_returns=[[commit_msg1], [commit_msg2]]
        )

        mock_commit = Mock()
        mock_commit.commit.side_effect = Exception("Commit failed")
        datasink._writer_builder.new_commit = Mock(return_value=mock_commit)

        with self.assertRaises(Exception):
            datasink.on_write_complete(write_result)

        mock_commit.abort.assert_not_called()
        mock_commit.close.assert_called_once()

        # Test table_commit creation failure
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()
        commit_msg1 = Mock(spec=CommitMessage)
        commit_msg1.is_empty.return_value = False

        write_result = WriteResult(
            num_rows=0,
            size_bytes=0,
            write_returns=[[commit_msg1]]
        )

        mock_new_commit = Mock(side_effect=Exception("Failed to create table_commit"))
        datasink._writer_builder.new_commit = mock_new_commit
        with self.assertRaises(Exception):
            datasink.on_write_complete(write_result)

    def test_on_write_complete_without_on_write_start(self):
        from ray.data.datasource.datasink import WriteResult

        write_result = WriteResult(
            num_rows=0,
            size_bytes=0,
            write_returns=[],
        )

        for overwrite, static_partition in [
            (True, None),
            (False, {'dt': '2024-01-01'}),
        ]:
            with self.subTest(
                overwrite=overwrite,
                static_partition=static_partition,
            ):
                table = Mock()
                table.identifier.get_full_name.return_value = 'test_db.test_table'
                writer_builder = table.new_batch_write_builder.return_value
                writer_builder.overwrite.return_value = writer_builder
                table_commit = writer_builder.new_commit.return_value

                datasink = PaimonDatasink(
                    table,
                    overwrite=overwrite,
                    static_partition=static_partition,
                )
                datasink.on_write_complete(write_result)

                table.new_batch_write_builder.assert_called_once_with()
                writer_builder.overwrite.assert_called_once_with(static_partition)
                table_commit.commit.assert_called_once_with([])
                table_commit.close.assert_called_once_with()

    def test_table_write_ray_forwards_static_partition(self):
        dataset = Mock()
        table_write = TableWrite.__new__(TableWrite)
        table_write.table = self.table
        table_write.static_partition = {'dt': '2024-01-01'}

        with patch('pypaimon.ray.shuffle.maybe_apply_repartition') as mock_repartition, \
                patch('pypaimon.write.ray_datasink.PaimonDatasink') as mock_datasink_cls:
            mock_repartition.return_value = dataset
            datasink = mock_datasink_cls.return_value

            table_write.write_ray(dataset, concurrency=2)

            mock_repartition.assert_called_once_with(dataset, self.table, 'auto')
            mock_datasink_cls.assert_called_once_with(
                self.table,
                overwrite=False,
                static_partition={'dt': '2024-01-01'},
                options=None,
            )
            dataset.write_datasink.assert_called_once_with(
                datasink,
                concurrency=2,
                ray_remote_args=None,
            )

    def test_table_write_ray_static_partition_argument_overrides_builder(self):
        dataset = Mock()
        table_write = TableWrite.__new__(TableWrite)
        table_write.table = self.table
        table_write.static_partition = {'dt': '2024-01-01'}

        with patch('pypaimon.ray.shuffle.maybe_apply_repartition') as mock_repartition, \
                patch('pypaimon.write.ray_datasink.PaimonDatasink') as mock_datasink_cls:
            mock_repartition.return_value = dataset

            table_write.write_ray(
                dataset,
                static_partition={'dt': '2024-01-02'},
            )

            mock_datasink_cls.assert_called_once_with(
                self.table,
                overwrite=False,
                static_partition={'dt': '2024-01-02'},
                options=None,
            )

    def test_on_write_failed(self):
        datasink = PaimonDatasink(self.table, overwrite=False)
        datasink.on_write_start()
        datasink._writer_builder.new_commit = Mock()
        error = Exception("Write job failed")
        datasink.on_write_failed(error)

        datasink._writer_builder.new_commit.assert_not_called()

    def test_consume_write_results_reports_late_failure(self):
        import pickle

        message_col = '__messages__'

        class FailingResults:
            def iter_batches(self, batch_format):
                if batch_format != 'pyarrow':
                    raise AssertionError(batch_format)
                yield pa.table({
                    message_col: pa.array(
                        [pickle.dumps(['first'])], type=pa.binary()
                    ),
                })
                raise RuntimeError('late failure')

        coordinator = Mock()
        with self.assertRaisesRegex(RuntimeError, 'late failure'):
            _consume_write_results(
                FailingResults(), coordinator, message_col
            )

        coordinator.on_write_complete.assert_not_called()
        coordinator.on_write_failed.assert_called_once()

    def test_consume_write_results_drains_errors_as_data(self):
        import pickle

        message_col = '__messages__'
        error_col = '__errors__'
        results = Mock()
        results.iter_batches.return_value = iter([
            pa.table({
                message_col: pa.array([
                    pickle.dumps(['first']),
                    pickle.dumps([]),
                    pickle.dumps(['last']),
                ], type=pa.binary()),
                error_col: pa.array(
                    [None, 'worker failure', None], type=pa.string()
                ),
            }),
        ])
        coordinator = Mock()

        with self.assertRaisesRegex(RuntimeError, 'worker failure'):
            _consume_write_results(
                results, coordinator, message_col, error_col
            )

        coordinator.on_write_complete.assert_not_called()
        coordinator.on_write_failed.assert_called_once()

    def test_consume_write_results_failure_preserves_completed_files(self):
        import pickle

        writer = self.table.new_batch_write_builder().new_write()
        writer.write_arrow(pa.Table.from_pydict({
            'id': [1],
            'name': ['Alice'],
            'value': [1.1],
        }, schema=self.pk_pa_schema))
        messages = writer.prepare_commit()
        writer.close()
        paths = [
            file.external_path or file.file_path
            for message in messages
            for file in message.new_files
        ]

        message_col = '__messages__'
        error_col = '__errors__'
        results = Mock()
        results.iter_batches.return_value = iter([
            pa.table({
                message_col: pa.array([
                    pickle.dumps(messages),
                    pickle.dumps([]),
                ], type=pa.binary()),
                error_col: pa.array([None, 'worker failure'], type=pa.string()),
            }),
        ])
        coordinator = PaimonDatasink(self.table, overwrite=False)
        coordinator.on_write_start()

        with self.assertRaisesRegex(RuntimeError, 'worker failure'):
            _consume_write_results(
                results, coordinator, message_col, error_col
            )

        self.assertTrue(all(self.table.file_io.exists(path) for path in paths))


if __name__ == '__main__':
    unittest.main()
