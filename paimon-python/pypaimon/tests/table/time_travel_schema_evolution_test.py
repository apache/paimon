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

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
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

    def test_legacy_descriptor_option_keeps_historical_blob_layout(self):
        legacy_key = 'blob.stored-descriptor-fields'
        original = self._create([
            DataField(0, 'id', AtomicType('INT')),
            DataField(1, 'payload', AtomicType('BLOB')),
        ], {legacy_key: 'payload'})
        payload = b'legacy-blob-payload'
        messages = self._write(original, {'id': 1, 'payload': payload})
        assert any(f.file_name.endswith('.blob') for msg in messages for f in msg.new_files)
        self.catalog.alter_table('test.t', [
            SchemaChange.drop_column('payload'),
            SchemaChange.add_column('reference', AtomicType('BYTES'), '__BLOB_DESCRIPTOR_FIELD'),
        ])
        current = self.catalog.get_table('test.t')

        historical = current.copy({'scan.snapshot-id': '1'})

        # Restore the original key, without turning it into an inline-descriptor layout switch.
        assert historical.table_schema.options[legacy_key] == 'payload'
        assert 'blob-descriptor-field' not in historical.table_schema.options
        assert not historical.options.blob_descriptor_fields()
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
