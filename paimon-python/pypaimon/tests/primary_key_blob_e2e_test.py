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

"""End-to-end tests for primary-key tables with managed BLOB columns."""

import glob
import io
import os
import shutil
import tempfile
import unittest
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq

from pypaimon import CatalogFactory, Schema
from pypaimon.blob.managed_blob_reference_file import ManagedBlobReferenceFile
from pypaimon.table.row.blob import BlobDescriptor


class PrimaryKeyBlobE2ETest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.warehouse = os.path.join(cls.tempdir, 'warehouse')
        cls.catalog = CatalogFactory.create({'warehouse': cls.warehouse})
        cls.catalog.create_database('default', True)

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_pk_blob_table(
            self, table_name, pa_schema, primary_keys=None, extra_options=None):
        options = {
            'bucket': '1',
            'merge-engine': 'deduplicate',
            'changelog-producer': 'none',
            'file.format': 'parquet',
        }
        options.update(extra_options or {})
        schema = Schema.from_pyarrow_schema(
            pa_schema,
            primary_keys=primary_keys or ['id'],
            options=options,
        )
        full_name = 'default.{}'.format(table_name)
        self.catalog.create_table(full_name, schema, False)
        return self.catalog.get_table(full_name), pa_schema

    @staticmethod
    def _write_arrow(table, pa_table):
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        committer = write_builder.new_commit()
        try:
            writer.write_arrow(pa_table)
            commit_messages = writer.prepare_commit()
            committer.commit(commit_messages)
            return commit_messages
        finally:
            writer.close()
            committer.close()

    @staticmethod
    def _read_table(table):
        read_builder = table.new_read_builder()
        return read_builder.new_read().to_arrow(
            read_builder.new_scan().plan().splits())

    @staticmethod
    def _table_data_path(table):
        return table.table_path

    def _assert_managed_collection_storage(
            self, table, field_name, field_kind, expected_descriptor_count):
        data_files = {
            data_file.file_path: data_file
            for split in table.new_read_builder().new_scan().plan().splits()
            for data_file in split.files
        }
        self.assertTrue(data_files)

        all_descriptor_uris = set()
        descriptor_count = 0
        for data_file_path, data_file in data_files.items():
            physical_values = pq.read_table(
                data_file_path, columns=[field_name]).column(field_name).to_pylist()
            descriptor_values = []
            for value in physical_values:
                if value is None:
                    continue
                if field_kind == 'array':
                    descriptor_values.extend(
                        element for element in value if element is not None)
                else:
                    descriptor_values.extend(
                        map_value
                        for _, map_value in value
                        if map_value is not None)

            descriptor_uris = set()
            for raw in descriptor_values:
                self.assertTrue(
                    BlobDescriptor.is_blob_descriptor(raw),
                    'Managed collection value was stored inline instead of as a descriptor.',
                )
                descriptor = BlobDescriptor.deserialize(raw)
                self.assertTrue(descriptor.uri.endswith('.managed.blob'))
                self.assertGreaterEqual(descriptor.offset, 0)
                self.assertGreaterEqual(descriptor.length, 0)
                descriptor_uris.add(descriptor.uri)

            descriptor_count += len(descriptor_values)
            all_descriptor_uris.update(descriptor_uris)

            sidecar_names = [
                extra_file for extra_file in data_file.extra_files
                if extra_file.endswith('.blobref')
            ]
            self.assertEqual(len(sidecar_names), 1)
            sidecar_path = os.path.join(
                os.path.dirname(data_file_path), sidecar_names[0])
            references = ManagedBlobReferenceFile.read(table.file_io, sidecar_path)
            referenced_uris = {
                os.path.join(ref.storage_root_id, ref.relative_path)
                for ref in references
            }
            self.assertEqual(referenced_uris, descriptor_uris)

        managed_paths = set(glob.glob(
            os.path.join(
                self._table_data_path(table), '**', '*.managed.blob'),
            recursive=True,
        ))
        self.assertEqual(descriptor_count, expected_descriptor_count)
        self.assertEqual(all_descriptor_uris, managed_paths)

    def test_scalar_blob_write_read_round_trip(self):
        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            pa.field('payload', pa.large_binary()),
        ])
        table, schema = self._create_pk_blob_table('pk_scalar_blob', pa_schema)

        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2, 3],
            'payload': [b'alpha', b'beta', b'gamma'],
        }, schema=schema))

        result = self._read_table(table).sort_by('id')
        self.assertEqual(
            {row['id']: row['payload'] for row in result.to_pylist()},
            {1: b'alpha', 2: b'beta', 3: b'gamma'},
        )

        data_path = self._table_data_path(table)
        self.assertTrue(glob.glob(
            os.path.join(data_path, '**', '*.managed.blob'), recursive=True))
        self.assertTrue(glob.glob(
            os.path.join(data_path, '**', '*.blobref'), recursive=True))

    def test_scalar_blob_deduplicate_last_write_wins(self):
        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            pa.field('payload', pa.large_binary()),
        ])
        table, schema = self._create_pk_blob_table('pk_scalar_blob_dedup', pa_schema)

        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2],
            'payload': [b'first-1', b'first-2'],
        }, schema=schema))
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1],
            'payload': [b'second-1'],
        }, schema=schema))

        result = self._read_table(table).sort_by('id')
        self.assertEqual(
            {row['id']: row['payload'] for row in result.to_pylist()},
            {1: b'second-1', 2: b'first-2'},
        )

    def test_close_without_prepare_aborts_managed_blob_packs(self):
        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            pa.field('payload', pa.large_binary()),
        ])
        table, schema = self._create_pk_blob_table('pk_blob_close_abort', pa_schema)
        writer = table.new_batch_write_builder().new_write()
        data_path = self._table_data_path(table)
        try:
            writer.write_arrow(pa.Table.from_pydict({
                'id': [1],
                'payload': [b'uncommitted'],
            }, schema=schema))
            self.assertTrue(glob.glob(
                os.path.join(data_path, '**', '*.managed.blob'), recursive=True))
        finally:
            writer.close()

        self.assertFalse(glob.glob(
            os.path.join(data_path, '**', '*.managed.blob'), recursive=True))

    def test_close_after_prepare_keeps_managed_blob_packs(self):
        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            pa.field('payload', pa.large_binary()),
        ])
        table, schema = self._create_pk_blob_table('pk_blob_close_prepared', pa_schema)
        writer = table.new_batch_write_builder().new_write()
        data_path = self._table_data_path(table)
        writer.write_arrow(pa.Table.from_pydict({
            'id': [1],
            'payload': [b'prepared'],
        }, schema=schema))
        writer.prepare_commit()
        packs_before_close = set(glob.glob(
            os.path.join(data_path, '**', '*.managed.blob'), recursive=True))
        self.assertTrue(packs_before_close)

        writer.close()

        self.assertEqual(
            set(glob.glob(
                os.path.join(data_path, '**', '*.managed.blob'), recursive=True)),
            packs_before_close,
        )

    def test_array_blob_write_read_round_trip(self):
        array_type = pa.list_(pa.large_binary())
        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            pa.field('payloads', array_type),
        ])
        table, schema = self._create_pk_blob_table('pk_array_blob', pa_schema)

        expected = {
            1: [b'a1', None, b'a3'],
            2: None,
            3: [b'c1'],
            4: [],
        }
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2, 3, 4],
            'payloads': pa.array(
                [expected[1], expected[2], expected[3], expected[4]],
                type=array_type,
            ),
        }, schema=schema))

        result = self._read_table(table).sort_by('id')
        actual = {
            row['id']: row['payloads']
            for row in result.to_pylist()
        }
        self.assertEqual(actual, expected)
        self._assert_managed_collection_storage(
            table, 'payloads', 'array', expected_descriptor_count=3)

    def test_map_blob_write_read_round_trip(self):
        map_type = pa.map_(pa.string(), pa.large_binary())
        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            pa.field('payload', map_type),
        ])
        table, schema = self._create_pk_blob_table('pk_map_blob', pa_schema)

        expected = {
            1: [('k1', b'v1'), ('k2', b'v2')],
            2: None,
            3: [],
            4: [('null-value', None)],
        }
        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2, 3, 4],
            'payload': pa.array(
                [dict(expected[1]), expected[2], dict(expected[3]), dict(expected[4])],
                type=map_type,
            ),
        }, schema=schema))

        result = self._read_table(table).sort_by('id')
        actual = {}
        for row in result.to_pylist():
            value = row['payload']
            if value is None:
                actual[row['id']] = None
            elif isinstance(value, dict):
                actual[row['id']] = sorted(value.items())
            else:
                actual[row['id']] = sorted(value)
        self.assertEqual(actual, {
            1: sorted(expected[1]),
            2: None,
            3: [],
            4: expected[4],
        })
        self._assert_managed_collection_storage(
            table, 'payload', 'map', expected_descriptor_count=2)

    def test_blobref_sidecar_lists_managed_blob_packs(self):
        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            pa.field('payload', pa.large_binary()),
        ])
        table, schema = self._create_pk_blob_table('pk_blob_sidecar', pa_schema)

        self._write_arrow(table, pa.Table.from_pydict({
            'id': [1, 2],
            'payload': [b'one', b'two'],
        }, schema=schema))

        data_path = self._table_data_path(table)
        sidecar_paths = glob.glob(
            os.path.join(data_path, '**', '*.blobref'), recursive=True)
        managed_paths = glob.glob(
            os.path.join(data_path, '**', '*.managed.blob'), recursive=True)
        self.assertEqual(len(sidecar_paths), 1)
        self.assertGreaterEqual(len(managed_paths), 1)

        references = ManagedBlobReferenceFile.read(table.file_io, sidecar_paths[0])
        referenced = {
            os.path.join(ref.storage_root_id, ref.relative_path)
            for ref in references
        }
        self.assertTrue(referenced)
        self.assertEqual(referenced, set(managed_paths))

    def test_source_table_descriptor_uses_production_writer_chain(self):
        pa_schema = pa.schema([
            pa.field('id', pa.int32(), nullable=False),
            pa.field('payload', pa.large_binary()),
        ])
        source_identifier = 'default.credential_source$branch_cred'
        table, schema = self._create_pk_blob_table(
            'pk_source_table_descriptor',
            pa_schema,
            extra_options={'blob-descriptor.source-table': source_identifier},
        )

        payload = b'payload-readable-only-with-source-credentials'
        source_file_io = _CredentialScopedFileIO(payload)
        source_catalog = _RoutingSourceCatalog(self.catalog, source_file_io)
        original_loader = table.catalog_environment.catalog_loader
        table.catalog_environment.catalog_loader = _RoutingCatalogLoader(
            source_catalog, original_loader.context())

        source_uri = 'credential://isolated-bucket/object'
        source_descriptor = BlobDescriptor(
            source_uri, 0, len(payload)).serialize()
        commit_messages = self._write_arrow(
            table,
            pa.Table.from_pydict({
                'id': [1],
                'payload': [source_descriptor],
            }, schema=schema),
        )

        self.assertEqual(source_file_io.opened, [source_uri])
        self.assertIsNotNone(source_catalog.source_identifier)
        self.assertEqual(
            source_catalog.source_identifier.get_branch_name(), 'cred')

        data_files = [
            data_file
            for message in commit_messages
            for data_file in message.new_files
        ]
        self.assertEqual(len(data_files), 1)
        data_file = data_files[0]
        physical = pq.read_table(
            data_file.file_path, columns=['payload']).column('payload')[0].as_py()
        target_descriptor = BlobDescriptor.deserialize(physical)
        self.assertNotEqual(target_descriptor.uri, source_uri)
        self.assertTrue(target_descriptor.uri.endswith('.managed.blob'))
        self.assertTrue(table.file_io.exists(target_descriptor.uri))

        sidecars = [
            extra for extra in data_file.extra_files if extra.endswith('.blobref')
        ]
        self.assertEqual(len(sidecars), 1)
        references = ManagedBlobReferenceFile.read(
            table.file_io,
            os.path.join(os.path.dirname(data_file.file_path), sidecars[0]),
        )
        self.assertEqual(len(references), 1)
        self.assertEqual(
            os.path.join(
                references[0].storage_root_id,
                references[0].relative_path,
            ),
            target_descriptor.uri,
        )
        self.assertEqual(
            self._read_table(table).column('payload').to_pylist(), [payload])


class _CredentialScopedFileIO:

    def __init__(self, payload):
        self.payload = payload
        self.opened = []

    def new_input_stream(self, uri):
        self.opened.append(uri)
        return io.BytesIO(self.payload)


class _RoutingSourceCatalog:

    def __init__(self, delegate, source_file_io):
        self.delegate = delegate
        self.source_file_io = source_file_io
        self.source_identifier = None

    def get_table(self, identifier):
        if identifier.get_table_name() == 'credential_source':
            self.source_identifier = identifier
            return SimpleNamespace(file_io=self.source_file_io)
        return self.delegate.get_table(identifier)

    def __getattr__(self, name):
        return getattr(self.delegate, name)

    def close(self):
        pass


class _RoutingCatalogLoader:

    def __init__(self, catalog, context):
        self.catalog = catalog
        self._context = context

    def context(self):
        return self._context

    def load(self):
        return self.catalog


if __name__ == '__main__':
    unittest.main()
