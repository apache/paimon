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

from pypaimon.write.backend_dispatch import python_commit
import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.read.datasource.split_provider import CatalogSplitProvider
from pypaimon.schema.data_types import ArrayType, AtomicType, DataField, PyarrowFieldParser
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.table.row.blob import BlobDescriptor, BlobViewStruct


class TestTimeTravelSchemaEvolution:

    @pytest.fixture(autouse=True)
    def setup(self, tmp_path):
        self.root = tmp_path
        self.catalog = CatalogFactory.create({'warehouse': str(tmp_path / 'warehouse')})
        self.catalog.create_database('test', False)

    def _create(self, fields, options=None, name='test.t'):
        table_options = {
            'bucket': '-1',
            'file.format': 'parquet',
            'file.compression': 'none',
            'row-tracking.enabled': 'true',
            'data-evolution.enabled': 'true',
        }
        table_options.update(options or {})
        self.catalog.create_table(name, Schema(fields=fields, options=table_options), False)
        return self.catalog.get_table(name)

    @staticmethod
    def _write(table, row):
        builder = table.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        try:
            writer.write_arrow(pa.Table.from_pylist(
                [row], schema=PyarrowFieldParser.from_paimon_schema(table.fields)))
            messages = writer.prepare_commit()
            commit.commit(messages)
            return messages
        finally:
            writer.close()
            commit.close()

    @staticmethod
    def _read(table):
        builder = table.new_read_builder()
        return builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pylist()

    def _create_branched_table(self, fields, row):
        table = self._create(fields, {
            'row-tracking.enabled': 'false',
            'data-evolution.enabled': 'false',
            'scan.native-plan.enabled': 'false',
            'read.native.enabled': 'false',
            'write.native.enabled': 'false',
        })
        self._write(table, row)
        table.create_tag('base', 1)
        self.catalog.create_branch('test.t', 'dev', tag_name='base')
        return table

    @pytest.mark.parametrize('evolve_main', [False, True])
    @pytest.mark.parametrize('selector', ['scan.snapshot-id', 'scan.tag-name'])
    def test_branch_time_travel_uses_target_schema(self, selector, evolve_main):
        self._create_branched_table([
            DataField(0, 'id', AtomicType('INT')),
            DataField(1, 'payload', AtomicType('STRING')),
        ], {'id': 1, 'payload': 'base'})
        if evolve_main:
            self.catalog.alter_table('test.t', [
                SchemaChange.add_column('source', AtomicType('STRING'))])
        self.catalog.alter_table('test.t$branch_dev', [
            SchemaChange.add_column('quality_score', AtomicType('DOUBLE'))])
        main = self.catalog.get_table('test.t')
        dev = self.catalog.get_table('test.t$branch_dev')
        main_row = {'id': 2, 'payload': 'main'}
        if evolve_main:
            main_row['source'] = 'web'
            assert main.table_schema.id == dev.table_schema.id
        self._write(main, main_row)
        self._write(dev, {'id': 3, 'payload': 'dev', 'quality_score': 0.9})
        dev.create_tag('training_v1', 2)
        self.catalog.alter_table('test.t$branch_dev', [
            SchemaChange.add_column('later_column', AtomicType('STRING'))])
        self._write(self.catalog.get_table('test.t$branch_dev'), {
            'id': 4, 'payload': 'later', 'quality_score': 0.8, 'later_column': 'new'})

        target = {selector: '2' if selector == 'scan.snapshot-id' else 'training_v1'}
        historical = main.copy({'branch': 'dev', **target})
        two_step = main.copy({'branch': 'dev'}).copy(target)
        expected = [
            {'id': 1, 'payload': 'base', 'quality_score': None},
            {'id': 3, 'payload': 'dev', 'quality_score': 0.9},
        ]
        assert historical.field_names == two_step.field_names == ['id', 'payload', 'quality_score']
        assert sorted(self._read(historical), key=lambda row: row['id']) == expected

        provider = CatalogSplitProvider(
            table_identifier='test.t',
            catalog_options={'warehouse': str(self.root / 'warehouse')},
            snapshot_id=2 if selector == 'scan.snapshot-id' else None,
            tag_name='training_v1' if selector == 'scan.tag-name' else None,
            dynamic_options={'branch': 'dev'},
        )
        assert [field.name for field in provider.read_type()] == historical.field_names
        read = provider.table().new_read_builder().new_read()
        assert sorted(read.to_arrow(provider.splits()).to_pylist(), key=lambda row: row['id']) == expected

    def test_branch_time_travel_preserves_bigint_values(self):
        main = self._create_branched_table([
            DataField(0, 'id', AtomicType('INT')),
            DataField(1, 'count', AtomicType('INT')),
        ], {'id': 1, 'count': 1})
        self.catalog.alter_table('test.t$branch_dev', [
            SchemaChange.update_column_type('count', AtomicType('BIGINT'))])
        self._write(main, {'id': 2, 'count': 2})
        self._write(self.catalog.get_table('test.t$branch_dev'), {'id': 3, 'count': 3000000000})

        historical = main.copy({'branch': 'dev', 'scan.snapshot-id': '2'})

        assert sorted(self._read(historical), key=lambda row: row['id']) == [
            {'id': 1, 'count': 1}, {'id': 3, 'count': 3000000000}]

    @pytest.mark.parametrize('branch', ['main', None])
    def test_branch_time_travel_back_to_main(self, branch):
        self._create_branched_table([
            DataField(0, 'id', AtomicType('INT')),
            DataField(1, 'payload', AtomicType('STRING')),
        ], {'id': 1, 'payload': 'base'})
        self.catalog.alter_table('test.t', [
            SchemaChange.add_column('source', AtomicType('STRING'))])
        main = self.catalog.get_table('test.t')
        dev = self.catalog.get_table('test.t$branch_dev')
        self._write(main, {'id': 2, 'payload': 'main', 'source': 'web'})
        self._write(dev, {'id': 3, 'payload': 'dev'})

        historical = dev.copy({'branch': branch, 'scan.snapshot-id': '2'})

        assert historical.current_branch() == 'main'
        assert historical.field_names == ['id', 'payload', 'source']
        assert sorted(self._read(historical), key=lambda row: row['id']) == [
            {'id': 1, 'payload': 'base', 'source': None},
            {'id': 2, 'payload': 'main', 'source': 'web'},
        ]

    def test_commit_retry_refreshes_snapshot_schema(self, monkeypatch):
        original = self._create([DataField(0, 'id', AtomicType('INT'))])
        self._write(original, {'id': 1})
        builder = original.new_batch_write_builder()
        writer, commit = builder.new_write(), builder.new_commit()
        snapshot_commit = python_commit(commit).file_store_commit.snapshot_commit
        publish = snapshot_commit.commit
        attempted_schemas = []

        def publish_after_schema_change(*args):
            attempted_schemas.append(args[1].schema_id)
            if len(attempted_schemas) == 1:
                self.catalog.alter_table('test.t', [
                    SchemaChange.add_column('new_column', AtomicType('STRING'))])
                return False
            return publish(*args)

        monkeypatch.setattr(snapshot_commit, 'commit', publish_after_schema_change)
        try:
            writer.write_arrow(pa.Table.from_pylist([{'id': 2}], schema=pa.schema([('id', pa.int32())])))
            messages = writer.prepare_commit()
            commit.commit(messages)
        finally:
            writer.close()
            commit.close()

        current = self.catalog.get_table('test.t')
        snapshot = current.snapshot_manager().get_latest_snapshot()
        assert attempted_schemas == [0, 1]
        assert snapshot.schema_id == current.table_schema.id == 1
        historical = current.copy({'scan.snapshot-id': str(snapshot.id)})
        assert sorted(self._read(historical), key=lambda row: row['id']) == [
            {'id': 1, 'new_column': None}, {'id': 2, 'new_column': None}]
        assert all(file.schema_id == 0
                   for split in current.new_read_builder().new_scan().plan().splits()
                   for file in split.files)

    @pytest.mark.parametrize('key,directive,field_type', [
        ('vector-field', '__VECTOR_FIELD;3', ArrayType(True, AtomicType('FLOAT'))),
        ('blob-field', '__BLOB_FIELD', AtomicType('BYTES')),
        ('blob-descriptor-field', '__BLOB_DESCRIPTOR_FIELD', AtomicType('BYTES')),
        ('blob-view-field', '__BLOB_VIEW_FIELD', AtomicType('BYTES')),
    ])
    def test_added_field_options_follow_historical_schema(self, key, directive, field_type):
        original = self._create([
            DataField(0, 'id', AtomicType('INT')),
            DataField(1, 'payload', field_type, directive),
        ])
        historical_options = dict(original.table_schema.options)
        payload = [1.0, 0.0, 0.0] if key == 'vector-field' else None
        self._write(original, {'id': 1, 'payload': payload})
        original.create_tag('before_change', 1)
        self.catalog.alter_table('test.t', [
            SchemaChange.add_column('payload_v2', field_type, directive)])
        current = self.catalog.get_table('test.t')
        current_options = dict(current.table_schema.options)
        assert current_options[key] == 'payload,payload_v2'
        self._write(current, {'id': 2, 'payload': payload, 'payload_v2': payload})

        historical = current.copy({'read.batch-size': '7'}).copy({
            'scan.tag-name': 'before_change'})

        assert historical.field_names == ['id', 'payload']
        assert historical.table_schema.options[key] == 'payload'
        assert historical.table_schema.options['read.batch-size'] == '7'
        assert self._read(historical) == [{'id': 1, 'payload': payload}]
        assert current.table_schema.options == current_options
        assert current.schema_manager.get_schema(original.table_schema.id).options == historical_options

    @pytest.mark.parametrize('key,directive', [
        ('blob-descriptor-field', '__BLOB_DESCRIPTOR_FIELD'),
        ('blob-view-field', '__BLOB_VIEW_FIELD'),
    ])
    def test_dropped_reference_field_reads_original_payload(self, key, directive):
        payload = b'original-payload'
        if key == 'blob-descriptor-field':
            path = self.root / 'payload.bin'
            path.write_bytes(payload)
            reference = BlobDescriptor(str(path), 0, len(payload)).serialize()
        else:
            upstream = self._create([
                DataField(0, 'id', AtomicType('INT')),
                DataField(1, 'payload', AtomicType('BYTES'), '__BLOB_FIELD'),
            ], name='test.upstream')
            self._write(upstream, {'id': 1, 'payload': payload})
            reference = BlobViewStruct('test.upstream', 1, 0).serialize()

        original = self._create([
            DataField(0, 'id', AtomicType('INT')),
            DataField(1, 'payload', AtomicType('BYTES'), directive),
        ])
        self._write(original, {'id': 1, 'payload': reference})
        assert self._read(original) == [{'id': 1, 'payload': payload}]
        self.catalog.alter_table('test.t', [SchemaChange.drop_column('payload')])
        current = self.catalog.get_table('test.t')
        assert key not in current.table_schema.options
        self._write(current, {'id': 2})

        historical = current.copy({'scan.snapshot-id': '1'})

        # Non-null references catch a missing decoder that would otherwise return raw bytes.
        assert self._read(historical) == [{'id': 1, 'payload': payload}]
        assert historical.table_schema.options[key] == 'payload'

    def test_first_vector_field_is_absent_from_historical_options(self):
        original = self._create([DataField(0, 'id', AtomicType('INT'))])
        self._write(original, {'id': 1})
        self.catalog.alter_table('test.t', [SchemaChange.add_column(
            'embedding', ArrayType(True, AtomicType('FLOAT')), '__VECTOR_FIELD;3')])
        current = self.catalog.get_table('test.t')

        historical = current.copy({'scan.snapshot-id': '1'})

        assert historical.field_names == ['id']
        assert 'vector-field' not in historical.table_schema.options
        assert self._read(historical) == [{'id': 1}]

    def test_descriptor_fallback_keeps_historical_inline_layout(self):
        legacy_key = 'blob.stored-descriptor-fields'
        original = self._create([
            DataField(0, 'id', AtomicType('INT')),
            DataField(1, 'payload', AtomicType('BLOB')),
        ], {legacy_key: 'payload'})
        source = self.root / 'blob-source'
        payload = b'blob-payload'
        source.write_bytes(payload)
        messages = self._write(original, {'id': 1, 'payload': BlobDescriptor(
            source.as_uri(), 0, len(payload)).serialize()})
        assert all(f.file_name.endswith('.parquet') for msg in messages for f in msg.new_files)
        self.catalog.alter_table('test.t', [
            SchemaChange.drop_column('payload'),
            SchemaChange.add_column('reference', AtomicType('BYTES'), '__BLOB_DESCRIPTOR_FIELD'),
        ])
        current = self.catalog.get_table('test.t')

        historical = current.copy({'scan.snapshot-id': '1'})

        assert historical.table_schema.options[legacy_key] == 'payload'
        assert 'blob-descriptor-field' not in historical.table_schema.options
        assert historical.options.blob_descriptor_fields() == {'payload'}
        assert self._read(historical) == [{'id': 1, 'payload': payload}]

    def test_explicit_field_overrides_survive_repeated_copies(self):
        original = self._create([
            DataField(0, 'id', AtomicType('INT')),
            DataField(1, 'payload', ArrayType(True, AtomicType('FLOAT')), '__VECTOR_FIELD;3'),
        ])
        self._write(original, {'id': 1, 'payload': [1.0, 0.0, 0.0]})
        self.catalog.alter_table('test.t', [SchemaChange.add_column(
            'payload_v2', ArrayType(True, AtomicType('FLOAT')), '__VECTOR_FIELD;3')])
        current = self.catalog.get_table('test.t')

        for value in ('payload_v2', None):
            historical = current.copy({'vector-field': value}).copy({'scan.snapshot-id': '1'})
            repeated = historical.copy({'read.batch-size': '7'})
            assert repeated.table_schema.options.get('vector-field') == value
            assert repeated._applied_dynamic_options == {
                'vector-field': value, 'scan.snapshot-id': '1', 'read.batch-size': '7'}

        historical = current.copy({'scan.snapshot-id': '1'})
        assert historical._applied_dynamic_options == {'scan.snapshot-id': '1'}

    def test_descriptor_fallback_override_keeps_canonical_group(self):
        original = self._create([
            DataField(0, 'id', AtomicType('INT')), DataField(1, 'payload', AtomicType('BLOB')),
        ], {'blob.stored-descriptor-fields': 'payload'})
        self._write(original, {'id': 1, 'payload': None})
        self.catalog.alter_table('test.t', [SchemaChange.add_column(
            'reference', AtomicType('BYTES'), '__BLOB_DESCRIPTOR_FIELD')])
        current = self.catalog.get_table('test.t')
        historical = current.copy({'blob.stored-descriptor-fields': None}).copy({'scan.snapshot-id': '1'})
        repeated = historical.copy({'read.batch-size': '1'})
        for table in (historical, repeated):
            assert table.table_schema.options['blob-descriptor-field'] == 'payload,reference'
            assert 'blob.stored-descriptor-fields' not in table.table_schema.options
