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

import io
import os
import shutil
import tempfile
import unittest
from unittest.mock import Mock, patch

import pyarrow as pa
import pyarrow.parquet as pq

from pypaimon import CatalogFactory, Schema
from pypaimon.blob.managed_blob_payload import (
    materialize_managed_blob, resolve_table_blob_uri)
from pypaimon.blob.managed_blob_reference_file import ManagedBlobReferenceFile
from pypaimon.catalog.table_query_auth import TableQueryAuthResult
from pypaimon.read.query_auth_split import QueryAuthSplit
from pypaimon.schema.data_types import ArrayType, AtomicType, DataField, MapType, PyarrowFieldParser
from pypaimon.table.row.blob import BlobDescriptor, BlobViewStruct
from pypaimon.write.commit_message import CommitMessage


class ManagedBlobPrimaryKeyWriteTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.catalog = CatalogFactory.create({'warehouse': self.temp_dir})
        self.catalog.create_database('default', False)

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_pk_write_uses_custom_blob_uri_reader_factory(self):
        for streaming in (False, True):
            with self.subTest(streaming=streaming):
                table = self._create_table('custom_uri_%s' % streaming, {
                    'bucket': '1', 'write.native.enabled': 'false',
                    'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'})
                builder = (table.new_stream_write_builder() if streaming else table.new_batch_write_builder())
                writer, commit = builder.new_write(), builder.new_commit()
                factory = Mock()
                factory.create.return_value.new_input_stream.side_effect = (
                    lambda uri: io.BytesIO(b'prefixPAYLOADsuffix'))
                writer.with_blob_uri_reader_factory(factory)
                descriptor = BlobDescriptor('custom://source', 6, 7).serialize()
                try:
                    writer.write_arrow(pa.Table.from_pydict({
                        'id': [1], 'payload': [descriptor], 'items': [None], 'mapping': [None],
                    }, schema=PyarrowFieldParser.from_paimon_schema(table.fields)))
                    messages = writer.prepare_commit(1) if streaming else writer.prepare_commit()
                    factory.create.assert_called_once_with('custom://source')
                    raw = self._parquet_rows(table, messages[0].new_files[0])[0]['payload']
                    self.assertEqual(self._written_payload(table, raw), b'PAYLOAD')
                    commit.abort(messages)
                    self.assertFalse(any(name.endswith('.managed.blob') for name in self._names_under(table)))
                finally:
                    writer.close()
                    commit.close()

    def test_merge_read_preserves_user_row_kind_columns(self):
        fields = [DataField(0, 'id', AtomicType('INT', False)),
                  DataField(1, '_row_kind', AtomicType('STRING')),
                  DataField(2, '__row_kind', AtomicType('STRING')),
                  DataField(3, 'payload', AtomicType('BLOB'))]
        self.catalog.create_table('default.row_kind_columns', Schema(
            fields=fields, primary_keys=['id'], options={
                'bucket': '1', 'write.native.enabled': 'false',
                'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false'}), False)
        table = self.catalog.get_table('default.row_kind_columns')
        data = {'id': [1, 2], '_row_kind': ['hello', 'world'],
                '__row_kind': ['first', 'second'], 'payload': [b'visible', b'other']}
        expected = [dict(zip(data, values)) for values in zip(*data.values())]
        for generation in range(2):
            self._write_and_commit(table, pa.Table.from_pydict(
                data, schema=PyarrowFieldParser.from_paimon_schema(fields)))
            for projection in (list(data), ['payload', '_row_kind'], ['__row_kind', '_row_kind']):
                for filtered in (False, True):
                    with self.subTest(generation=generation, projection=projection, filtered=filtered):
                        builder = table.new_read_builder()
                        if filtered:
                            builder.with_filter(builder.new_predicate_builder().equal('payload', b'other'))
                            builder.with_limit(1)
                        builder.with_projection(projection)
                        splits = builder.new_scan().plan().splits()
                        self.assertTrue(all(split.raw_convertible == (generation == 0) for split in splits))
                        wanted = [{name: row[name] for name in projection}
                                  for row in (expected[1:] if filtered else expected)]
                        read = builder.new_read()
                        self.assertEqual(read.to_arrow(splits).to_pylist(), wanted)
                        batches = read.to_arrow_batch_reader(splits)
                        self.assertEqual([row for batch in batches for row in batch.to_pylist()], wanted)
                        actual = []
                        for row in read.to_iterator(splits):
                            actual.append({name: row.get_blob(i).to_data() if name == 'payload' else row.get_field(i)
                                           for i, name in enumerate(projection)})
                        self.assertEqual(actual, wanted)
                        if not filtered:
                            authorized = [QueryAuthSplit(split, TableQueryAuthResult(
                                None, {'payload': '{"name":"NULL"}'})) for split in splits]
                            masked = [{name: None if name == 'payload' else value for name, value in row.items()}
                                      for row in wanted]
                            self.assertEqual(read.to_arrow(authorized).to_pylist(), masked)
                            actual = []
                            for row in read.to_iterator(authorized):
                                actual.append({name: row.get_field(i) for i, name in enumerate(projection)})
                            self.assertEqual(actual, masked)

    def test_python_pk_write_registers_blobref_and_reads_payload(self):
        table = self._create_table('pk_blob', {
            'bucket': '1',
            'write.native.enabled': 'false',
        })
        arrow_schema = PyarrowFieldParser.from_paimon_schema(table.fields)
        data = pa.Table.from_pydict({
            'id': [1, 1, 2],
            'payload': [b'old', b'new', b'other'],
            'items': [[b'a', None], [b'a', b'b'], []],
            'mapping': [[('k', b'x')], [('k', b'y'), ('m', None)], []],
        }, schema=arrow_schema)

        writer, commit = self._write(table)
        try:
            writer.write_arrow(data)
            messages = writer.prepare_commit()
            files = [meta for message in messages for meta in message.new_files]
            self.assertTrue(files)
            self.assertTrue(all(
                any(name.endswith('.blobref') for name in meta.extra_files)
                and not any(name.endswith('.managed.blob') for name in meta.extra_files)
                for meta in files))

            payloads = []
            referenced = []
            for meta in files:
                payloads.extend(self._parquet_rows(table, meta))
                for name in meta.extra_files:
                    if not name.endswith('.blobref'):
                        continue
                    sidecar = meta.aligned_file_path(name)
                    references = ManagedBlobReferenceFile.read(table.file_io, sidecar)
                    referenced.extend(references)
                    for reference in references:
                        pack = self._pack_path(table, reference)
                        self.assertTrue(table.file_io.exists(pack))
                        self.assertTrue(pack.endswith('.managed.blob'))
                        self.assertNotIn(reference.relative_path, meta.extra_files)

            kept = next(row for row in payloads if row['id'] == 1)
            descriptor = BlobDescriptor.deserialize(kept['payload'])
            self.assertTrue(descriptor.uri.startswith(str(table.table_path) + '/'))
            self.assertTrue(table.file_io.exists(descriptor.uri))
            self.assertIn(ManagedBlobReferenceFile.from_descriptor_uri(descriptor.uri), referenced)
            self.assertEqual(self._written_payload(table, kept['payload']), b'new')
            self.assertEqual(self._written_payload(table, kept['items'][0]), b'a')
            self.assertEqual(self._written_payload(table, kept['items'][1]), b'b')
            map_values = dict(kept['mapping'])
            self.assertEqual(self._written_payload(table, map_values['k']), b'y')
            self.assertIsNone(map_values['m'])
            self.assertTrue(referenced)
            self.assertNotIn(b'old', [row['payload'] for row in payloads])

            commit.abort(messages)
            for meta in files:
                self.assertFalse(table.file_io.exists(meta.physical_path()))
                for name in meta.extra_files:
                    self.assertFalse(table.file_io.exists(meta.aligned_file_path(name)))
            for reference in referenced:
                self.assertFalse(table.file_io.exists(self._pack_path(table, reference)))
        finally:
            writer.close()
            commit.close()

    def test_retract_does_not_record_managed_payload(self):
        table = self._create_table('pk_blob_retract', {
            'bucket': '1',
            'write.native.enabled': 'false',
            'rowkind.field': 'op',
        })
        arrow_schema = PyarrowFieldParser.from_paimon_schema(table.fields)
        data = pa.Table.from_pydict({
            'id': [1, 2],
            'payload': [b'keep', b'drop'],
            'items': [[b'keep-item'], [b'drop-item']],
            'mapping': [[('k', b'keep-map')], [('k', b'drop-map')]],
            'op': ['+I', '-D'],
        }, schema=arrow_schema)

        writer, commit = self._write(table)
        try:
            writer.write_arrow(data)
            messages = writer.prepare_commit()
            files = [meta for message in messages for meta in message.new_files]
            rows = []
            blobrefs = []
            for meta in files:
                rows.extend(self._parquet_rows(table, meta))
                for name in meta.extra_files:
                    if name.endswith('.blobref'):
                        blobrefs.extend(ManagedBlobReferenceFile.read(
                            table.file_io, meta.aligned_file_path(name)))
            deleted = next(row for row in rows if row['id'] == 2)
            self.assertIsNone(deleted['payload'])
            self.assertIsNone(deleted['items'])
            self.assertIsNone(deleted['mapping'])
            kept = next(row for row in rows if row['id'] == 1)
            self.assertEqual(self._written_payload(table, kept['payload']), b'keep')
            pack_names = {reference.relative_path for reference in blobrefs}
            self.assertTrue(pack_names)
            for reference in blobrefs:
                pack = self._pack_path(table, reference)
                with table.file_io.new_input_stream(pack) as stream:
                    contents = stream.read()
                self.assertNotIn(b'drop', contents)
        finally:
            writer.close()
            commit.close()

    def test_writer_abort_deletes_uncommitted_pack_and_sidecar(self):
        table = self._create_table('pk_blob_abort', {
            'bucket': '1',
            'write.native.enabled': 'false',
            'target-file-size': '1 B',
        })
        arrow_schema = PyarrowFieldParser.from_paimon_schema(table.fields)
        data = pa.Table.from_pydict({
            'id': [1],
            'payload': [b'payload'],
            'items': [[b'item']],
            'mapping': [[('k', b'map')]],
        }, schema=arrow_schema)

        writer = table.new_batch_write_builder().new_write()
        try:
            writer.write_arrow(data)
            written = self._names_under(table)
            self.assertTrue(any(name.endswith('.parquet') for name in written))
            self.assertTrue(any(name.endswith('.blobref') for name in written))
            self.assertTrue(any(name.endswith('.managed.blob') for name in written))
            writer.abort()
            self.assertFalse(any(
                name.endswith(('.parquet', '.blobref', '.managed.blob'))
                for name in self._names_under(table)))
        finally:
            writer.close()

    def test_partial_abort_keeps_pack_used_by_committed_files(self):
        self._check_shared_pack_cleanup(False)

    def test_relative_warehouse_abort_cleans_shared_packs(self):
        previous = os.getcwd()
        os.chdir(self.temp_dir)
        try:
            catalog = CatalogFactory.create({'warehouse': './wh %?#'})
            catalog.create_database('default', False)
            with patch.object(self, 'catalog', catalog):
                for action in ('writer', 'commit', 'shared'):
                    with self.subTest(action=action):
                        table = self._create_table('abort_' + action, {
                            'bucket': '1', 'write.native.enabled': 'false',
                            'target-file-size': '1 B', 'blob.target-file-size': '1 GB',
                        })
                        writer, commit = self._write(table)
                        try:
                            for row_id in range(2):
                                writer.write_arrow(pa.Table.from_pydict({
                                    'id': [row_id], 'payload': [b'payload'],
                                    'items': [[b'item']], 'mapping': [[('k', b'map')]],
                                }, schema=PyarrowFieldParser.from_paimon_schema(table.fields)))
                            packs = {name for name in self._names_under(table)
                                     if name.endswith('.managed.blob')}
                            self.assertEqual(len(packs), 3)
                            if action == 'writer':
                                writer.abort()
                            else:
                                messages = writer.prepare_commit()
                                if action == 'shared':
                                    files = [meta for message in messages for meta in message.new_files]
                                    self.assertEqual(len(files), 2)
                                    commit.abort([CommitMessage((), 0, new_files=files[:1])])
                                    self.assertTrue(packs.issubset(set(self._names_under(table))))
                                    remaining = self._parquet_rows(table, files[1])[0]
                                    self.assertEqual(self._written_payload(table, remaining['payload']), b'payload')
                                    commit.abort([CommitMessage((), 0, new_files=files[1:])])
                                else:
                                    commit.abort(messages)
                            self.assertFalse(any(name.endswith(('.managed.blob', '.parquet', '.blobref'))
                                                 for name in self._names_under(table)))
                        finally:
                            writer.close()
                            commit.close()
                self._check_shared_pack_cleanup(False)
        finally:
            os.chdir(previous)

    def test_iterator_preserves_payloads_with_reference_headers(self):
        target = os.path.join(self.temp_dir, 'reference-target')
        with open(target, 'wb') as stream:
            stream.write(b'redirected')
        payloads = [BlobViewStruct('db.user_payload', 7, 42).serialize(),
                    BlobDescriptor(target, 0, 10).serialize(), b'plain', None]
        inputs = []
        for index, payload in enumerate(payloads):
            if payload is None:
                inputs.append(None)
                continue
            # A source file makes even descriptor-shaped input an opaque payload.
            source = os.path.join(self.temp_dir, 'payload-%d' % index)
            with open(source, 'wb') as stream:
                stream.write(payload)
            inputs.append(BlobDescriptor(source, 0, len(payload)).serialize())
        table = self._create_table('opaque_blobs', {
            'bucket': '1', 'write.native.enabled': 'false',
            'read.native.enabled': 'false', 'scan.native-plan.enabled': 'false',
        })
        for merged in (False, True):
            self._write_and_commit(table, pa.Table.from_pydict({
                'id': list(range(4)), 'payload': inputs,
                'items': [[value] for value in inputs],
                'mapping': [[('k', value)] for value in inputs],
            }, schema=PyarrowFieldParser.from_paimon_schema(table.fields)))
            for projection in (['id', 'payload'], ['payload', 'id'], ['payload'],
                               ["mapping['k']"], ['payload', "mapping['k']"],
                               {'alias': 'payload'}, {'alias': "mapping['k']"}):
                with self.subTest(merged=merged, projection=projection):
                    builder = table.new_read_builder().with_projection(projection)
                    splits = builder.new_scan().plan().splits()
                    self.assertTrue(all(split.raw_convertible != merged for split in splits))
                    read = builder.new_read()
                    result = read.to_arrow(splits)
                    blob_indices = [i for i, name in enumerate(result.schema.names) if name != 'id']
                    for index in blob_indices:
                        self.assertEqual(result.column(index).to_pylist(), payloads)
                    with read.to_arrow_batch_reader(splits) as batches:
                        self.assertEqual(batches.read_all().to_pydict(), result.to_pydict())
                    if isinstance(projection, dict):
                        continue  # Named expressions are only supported by Arrow read APIs.
                    rows = []
                    for row in read.to_iterator(splits):
                        rows.append([None if row.get_blob(i) is None else row.get_blob(i).to_data()
                                     for i in blob_indices])
                    self.assertEqual(rows, [[payload] * len(blob_indices) for payload in payloads])

            builder = table.new_read_builder()
            builder.with_filter(builder.new_predicate_builder().equal('payload', payloads[0]))
            builder.with_projection(['payload']).with_limit(1)
            splits = builder.new_scan().plan().splits()
            for auth in (None, TableQueryAuthResult(None, {})):
                selected = splits if auth is None else [QueryAuthSplit(split, auth) for split in splits]
                self.assertEqual([row.get_blob(0).to_data() for row in builder.new_read().to_iterator(selected)],
                                 [payloads[0]])

            builder = table.copy({'blob-as-descriptor': 'true'}).new_read_builder()
            builder.with_projection(['payload'])
            splits = builder.new_scan().plan().splits()
            self.assertEqual([None if row.get_blob(0) is None else row.get_blob(0).to_data()
                              for row in builder.new_read().to_iterator(splits)], payloads)

    def test_exclusive_file_cleanup_keeps_pack_used_by_committed_files(self):
        self._check_shared_pack_cleanup(True)

    def _check_shared_pack_cleanup(self, exclusive_cleanup):
        table = self._create_table('pk_blob_partial_abort', {
            'bucket': '1',
            'write.native.enabled': 'false',
            'read.native.enabled': 'false',
            'scan.native-plan.enabled': 'false',
            'target-file-size': '1 B',
            'blob.target-file-size': '1 GB',
        })
        arrow_schema = PyarrowFieldParser.from_paimon_schema(table.fields)
        writer, commit = self._write(table)
        try:
            for row_id in range(6):
                writer.write_arrow(pa.Table.from_pydict({
                    'id': [row_id],
                    'payload': [b'p%d' % row_id],
                    'items': [[b'i%d' % row_id]],
                    'mapping': [[('k', b'm%d' % row_id)]],
                }, schema=arrow_schema))
            messages = writer.prepare_commit()
            files = [meta for message in messages for meta in message.new_files]
            self.assertGreaterEqual(len(files), 2)
            pack_lists = [
                reference.relative_path
                for meta in files for reference in ManagedBlobReferenceFile.read(
                    table.file_io, meta.aligned_file_path(meta.file_name + '.blobref'))
            ]
            self.assertGreater(len(pack_lists), len(set(pack_lists)))

            message = messages[0]
            dropped = files[-1]
            dropped_ids = {row['id'] for row in self._parquet_rows(table, dropped)}
            commit.commit([
                CommitMessage(message.partition, message.bucket, files[:-1])])
            if exclusive_cleanup:
                # Java abort deletes collectFiles() without reference checks.
                # The metadata must be safe for that exclusive-ownership contract.
                for path in dropped.collect_files():
                    table.file_io.delete_quietly(path)
            else:
                commit.abort([
                    CommitMessage(message.partition, message.bucket, [dropped])])

            for name in set(pack_lists):
                self.assertTrue(table.file_io.exists(dropped.aligned_file_path(name)))
            self.assertFalse(table.file_io.exists(dropped.physical_path()))

            builder = table.new_read_builder()
            rows = builder.new_read().to_arrow(
                builder.new_scan().plan().splits()).to_pylist()
            self.assertEqual(
                {row['id'] for row in rows},
                {row_id for row_id in range(6) if row_id not in dropped_ids})
            for row in rows:
                self.assertEqual(row['payload'], b'p%d' % row['id'])
        finally:
            writer.close()
            commit.close()

    def test_read_builder_returns_managed_blob_payload(self):
        table = self._create_table('pk_blob_read', {
            'bucket': '1',
            'write.native.enabled': 'false',
            'read.native.enabled': 'false',
            'scan.native-plan.enabled': 'false',
        })
        arrow_schema = PyarrowFieldParser.from_paimon_schema(table.fields)
        data = pa.Table.from_pydict({
            'id': [1],
            'payload': [b'visible'],
            'items': [[b'item']],
            'mapping': [[('k', b'map')]],
        }, schema=arrow_schema)

        writer, commit = self._write(table)
        try:
            writer.write_arrow(data)
            commit.commit(writer.prepare_commit())
            builder = table.new_read_builder()
            rows = builder.new_read().to_arrow(
                builder.new_scan().plan().splits()).to_pylist()
            self.assertEqual(len(rows), 1)
            row = rows[0]
            self.assertEqual(row['payload'], b'visible')
            self.assertEqual(row['items'], [b'item'])
            self.assertEqual(dict(row['mapping']), {'k': b'map'})
        finally:
            writer.close()
            commit.close()

    def test_payload_predicate_matches_resolved_bytes(self):
        self._check_payload_predicate(False)

    def test_map_key_projection_resolves_payload(self):
        self._check_map_key_projection(False)

    def test_merged_map_key_projection_resolves_payload(self):
        self._check_map_key_projection(True)

    def _check_map_key_projection(self, merged):
        table = self._create_table('pk_blob_map_projection', {
            'bucket': '1',
            'write.native.enabled': 'false',
            'read.native.enabled': 'false',
            'scan.native-plan.enabled': 'false',
        })
        for payload in ([b'old', b'new'] if merged else [b'new']):
            self._write_and_commit(table, pa.Table.from_pydict({
                'id': [1, 2, 3],
                'payload': [b'visible', None, None],
                'items': [None, None, None],
                'mapping': [[('k', payload), ('n', None),
                             ('k.a', b'dot'), ('k_a', b'underscore')], [], None],
            }, schema=PyarrowFieldParser.from_paimon_schema(table.fields)))

        projections = [
            (["mapping['k']"], {'mapping_k': [b'new', None, None]}),
            ({'alias': "mapping['k']"}, {'alias': [b'new', None, None]}),
            (["mapping['k']", 'payload'], {
                'mapping_k': [b'new', None, None], 'payload': [b'visible', None, None]}),
            (["mapping['k.a']", "mapping['k_a']", "mapping['n']", "mapping['missing']"], {
                'mapping_k_a': [b'dot', None, None],
                'mapping_k_a__0': [b'underscore', None, None],
                'mapping_n': [None, None, None], 'mapping_missing': [None, None, None]}),
        ]
        for projection, expected in projections:
            with self.subTest(projection=projection):
                builder = table.new_read_builder().with_projection(projection)
                splits = builder.new_scan().plan().splits()
                self.assertTrue(splits)
                self.assertTrue(all(split.raw_convertible != merged for split in splits))
                read = builder.new_read()
                self.assertEqual(read.to_arrow(splits).to_pydict(), expected)
                with read.to_arrow_batch_reader(splits) as reader:
                    self.assertEqual(reader.read_all().to_pydict(), expected)
                if isinstance(projection, list):
                    rows = [tuple(row.get_field(i) for i in range(len(expected)))
                            for row in read.to_iterator(splits)]
                    self.assertEqual(rows, list(zip(*expected.values())))

        builder = table.new_read_builder()
        builder.with_filter(builder.new_predicate_builder().equal('payload', b'visible'))
        builder.with_projection(["mapping['k']"]).with_limit(1)
        splits = builder.new_scan().plan().splits()
        self.assertEqual(builder.new_read().to_arrow(splits).to_pydict(), {'mapping_k': [b'new']})

        builder = table.copy({'blob-as-descriptor': 'true'}).new_read_builder()
        builder.with_projection(["mapping['k']"])
        rows = builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pylist()
        self.assertEqual(self._written_payload(table, rows[0]['mapping_k']), b'new')
        self.assertEqual(rows[1:], [{'mapping_k': None}, {'mapping_k': None}])

    def test_merged_payload_predicate_matches_resolved_bytes(self):
        self._check_payload_predicate(True)

    def _check_payload_predicate(self, merged):
        table = self._create_table('pk_blob_filter', {
            'bucket': '1',
            'write.native.enabled': 'false',
            'read.native.enabled': 'false',
            'scan.native-plan.enabled': 'false',
        })
        arrow_schema = PyarrowFieldParser.from_paimon_schema(table.fields)
        data = pa.Table.from_pydict({
            'id': [1, 2],
            'payload': [b'visible', b'other'],
            'items': [[b'a'], [b'b']],
            'mapping': [[('k', b'a')], [('k', b'b')]],
        }, schema=arrow_schema)
        self._write_and_commit(table, data)

        if merged:
            self._write_and_commit(table, data)

        for projection in (None, ['id'], ['payload', 'id'], ['id', 'payload'], []):
            for limit in (None, 1):
                for payload, expected_id in ((b'visible', 1), (b'other', 2), (b'missing', None)):
                    with self.subTest(projection=projection, limit=limit, payload=payload):
                        builder = table.new_read_builder()
                        predicate = builder.new_predicate_builder().equal('payload', payload)
                        builder.with_filter(predicate)
                        if projection is not None:
                            builder.with_projection(projection)
                        if limit is not None:
                            builder.with_limit(limit)
                        splits = builder.new_scan().plan().splits()
                        if expected_id is not None:
                            self.assertTrue(splits)
                            self.assertTrue(all(split.raw_convertible != merged for split in splits))
                        read = builder.new_read()
                        expected_count = 0 if expected_id is None else 1
                        result = read.to_arrow(splits)
                        with read.to_arrow_batch_reader(splits) as batch_reader:
                            self.assertEqual(batch_reader.read_all().num_rows, expected_count)
                        self.assertEqual(result.num_rows, expected_count)
                        if expected_count and 'id' in result.schema.names:
                            self.assertEqual(result.column('id').to_pylist(), [expected_id])
                        if expected_count and 'payload' in result.schema.names:
                            self.assertEqual(result.column('payload').to_pylist(), [payload])
                        names = projection if projection is not None else [f.name for f in table.fields]
                        rows = [tuple(row.get_field(i) for i in range(len(names)))
                                for row in read.to_iterator(splits)]
                        self.assertEqual(len(rows), expected_count)
                        if expected_count and 'id' in names:
                            self.assertEqual(rows[0][names.index('id')], expected_id)
                        if expected_count and 'payload' in names:
                            self.assertEqual(rows[0][names.index('payload')], payload)

        # Authorization masks must be applied before the user predicate,
        # including when its payload column is absent from the output.
        builder = table.new_read_builder()
        splits = builder.new_scan().plan().splits()
        builder.with_filter(builder.new_predicate_builder().is_null('payload'))
        builder.with_projection(['id']).with_limit(1)
        auth = TableQueryAuthResult(None, {'payload': '{"name":"NULL"}'})
        splits = [QueryAuthSplit(split, auth) for split in splits]
        read = builder.new_read()
        self.assertEqual(read.to_arrow(splits).num_rows, 1)
        self.assertEqual(len(list(read.to_iterator(splits))), 1)

        # Authorized reads must also defer unmasked managed BLOB predicates
        # until descriptors have been resolved, before applying LIMIT.
        builder = table.new_read_builder()
        builder.with_filter(builder.new_predicate_builder().equal('payload', b'other'))
        builder.with_projection(['id']).with_limit(1)
        auth = TableQueryAuthResult(None, {})
        splits = [QueryAuthSplit(split, auth) for split in builder.new_scan().plan().splits()]
        read = builder.new_read()
        self.assertEqual(read.to_arrow(splits).to_pylist(), [{'id': 2}])
        self.assertEqual([row.get_field(0) for row in read.to_iterator(splits)], [2])

    def test_parallel_payload_filter_applies_limit_to_matches(self):
        table = self._create_table('pk_blob_parallel_filter', {
            'bucket': '1',
            'write.native.enabled': 'false',
            'read.native.enabled': 'false',
            'scan.native-plan.enabled': 'false',
            'target-file-size': '1 B',
            'source.split.target-size': '1 B',
        })
        writer, commit = self._write(table)
        try:
            for row_id, payload in enumerate([b'other', b'visible', b'other', b'visible']):
                writer.write_arrow(pa.Table.from_pydict({
                    'id': [row_id], 'payload': [payload], 'items': [None], 'mapping': [None],
                }, schema=PyarrowFieldParser.from_paimon_schema(table.fields)))
            commit.commit(writer.prepare_commit())
        finally:
            writer.close()
            commit.close()
        for projection in (['id'], ['payload', 'id']):
            builder = table.new_read_builder()
            builder.with_filter(builder.new_predicate_builder().equal('payload', b'visible'))
            builder.with_projection(projection).with_limit(1)
            splits = builder.new_scan().plan().splits()
            self.assertGreater(len(splits), 1)
            result = builder.new_read().to_arrow(splits, parallelism=2)
            self.assertEqual(result.num_rows, 1)
            self.assertIn(result.column('id')[0].as_py(), [1, 3])

    def test_payload_filter_preserves_large_batch(self):
        table = self._create_table('pk_blob_large_filter', {
            'bucket': '1',
            'write.native.enabled': 'false',
            'read.native.enabled': 'false',
            'scan.native-plan.enabled': 'false',
            'read.batch-size': '200000',
        })
        count = 140000
        data = pa.Table.from_pydict({
            'id': list(range(count)),
            'payload': [b'x'] * count,
            'items': [None] * count,
            'mapping': [None] * count,
        }, schema=PyarrowFieldParser.from_paimon_schema(table.fields))
        self._write_and_commit(table, data)
        builder = table.new_read_builder()
        builder.with_filter(builder.new_predicate_builder().equal('payload', b'x'))
        result = builder.new_read().to_arrow(builder.new_scan().plan().splits())
        self.assertEqual(result.num_rows, count)
        self.assertEqual(sorted(result.column('id').to_pylist()), list(range(count)))

    def test_row_iterator_opens_table_relative_blob(self):
        table = self._create_table('pk_blob_iterator', {
            'bucket': '1',
            'write.native.enabled': 'false',
            'read.native.enabled': 'false',
            'scan.native-plan.enabled': 'false',
        })
        arrow_schema = PyarrowFieldParser.from_paimon_schema(table.fields)
        data = pa.Table.from_pydict({
            'id': [1],
            'payload': [b'visible'],
            'items': [[b'item']],
            'mapping': [[('k', b'map')]],
        }, schema=arrow_schema)
        self._write_and_commit(table, data)

        builder = table.new_read_builder()
        read = builder.new_read()
        rows = list(read.to_iterator(builder.new_scan().plan().splits()))
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].get_blob(1).to_data(), b'visible')

    def test_mixed_blob_iterator_preserves_resolved_descriptor_fields(self):
        self._check_mixed_blob_iterator(os.path.join(self.temp_dir, 'external.bin'))

    def test_mixed_blob_iterator_preserves_relative_external_uri(self):
        previous = os.getcwd()
        try:
            os.chdir(self.temp_dir)
            self._check_mixed_blob_iterator('external.bin')
        finally:
            os.chdir(previous)

    def _check_mixed_blob_iterator(self, external_path):
        fields = [
            DataField(0, 'id', AtomicType('INT', False)),
            DataField(1, 'managed', AtomicType('BLOB')),
            DataField(2, 'external', AtomicType('BLOB')),
        ]
        schema = Schema(fields=fields, primary_keys=['id'], options={
            'bucket': '1',
            'blob-descriptor-field': 'external',
            'write.native.enabled': 'false',
            'read.native.enabled': 'false',
            'scan.native-plan.enabled': 'false',
        })
        self.catalog.create_table('default.mixed_blobs', schema, False)
        table = self.catalog.get_table('default.mixed_blobs')
        external_payload = b'external payload'
        with open(external_path, 'wb') as stream:
            stream.write(external_payload)
        descriptor = BlobDescriptor(external_path, 0, len(external_payload)).serialize()
        for generation in range(2):
            managed_payload = b'managed-%d' % generation
            self._write_and_commit(table, pa.Table.from_pydict({
                'id': [1], 'managed': [managed_payload], 'external': [descriptor],
            }, schema=PyarrowFieldParser.from_paimon_schema(fields)))
            for projection in (['id', 'managed', 'external'], ['external', 'managed'], ['id', 'external']):
                with self.subTest(generation=generation, projection=projection):
                    builder = table.new_read_builder().with_projection(projection)
                    splits = builder.new_scan().plan().splits()
                    self.assertTrue(splits)
                    self.assertTrue(all(split.raw_convertible == (generation == 0) for split in splits))
                    read = builder.new_read()
                    expected = {'id': 1, 'managed': managed_payload, 'external': external_payload}
                    self.assertEqual(read.to_arrow(splits).to_pylist(), [
                        {name: expected[name] for name in projection}])
                    rows = list(read.to_iterator(splits))
                    self.assertEqual(len(rows), 1)
                    for index, name in enumerate(projection):
                        if name != 'id':
                            self.assertEqual(rows[0].get_blob(index).to_data(), expected[name])
                    descriptor_builder = table.copy({'blob-as-descriptor': 'true'}).new_read_builder()
                    descriptor_builder.with_projection(projection)
                    descriptor_splits = descriptor_builder.new_scan().plan().splits()
                    rows = list(descriptor_builder.new_read().to_iterator(descriptor_splits))
                    self.assertEqual(len(rows), 1)
                    for index, name in enumerate(projection):
                        if name != 'id':
                            self.assertEqual(rows[0].get_blob(index).to_data(), expected[name])

    def test_failed_prepare_retry_cleans_unreferenced_packs(self):
        self._check_failed_prepare_finalization('prepare')

    def test_failed_prepare_close_cleans_unreferenced_packs(self):
        self._check_failed_prepare_finalization('close')

    def test_failed_prepare_abort_cleans_unreferenced_packs(self):
        self._check_failed_prepare_finalization('abort')

    def _check_failed_prepare_finalization(self, action):
        for stage in ('_data_file_statistics', '_create_data_file_meta'):
            for pack_size in ('1 B', '1 GB'):
                for shared in (False, True):
                    with self.subTest(stage=stage, pack_size=pack_size, shared=shared):
                        name = 'failed_%s_%s_%s_%s' % (action, stage, pack_size.replace(' ', ''), shared)
                        table = self._create_table(name, {
                            'bucket': '1',
                            'write.native.enabled': 'false',
                            'read.native.enabled': 'false',
                            'scan.native-plan.enabled': 'false',
                            'target-file-size': '1 B' if shared else '1 GB',
                            'blob.target-file-size': pack_size,
                        })
                        schema = PyarrowFieldParser.from_paimon_schema(table.fields)
                        builder = table.new_stream_write_builder()
                        writer, commit = builder.new_write(), builder.new_commit()
                        try:
                            if shared:
                                writer.write_arrow(pa.Table.from_pydict({
                                    'id': [0], 'payload': [b'first'], 'items': [None], 'mapping': [None],
                                }, schema=schema))
                                data_writer = next(iter(writer.file_store_write.data_writers.values()))
                                data_writer.target_file_size = 1024 ** 3
                            writer.write_arrow(pa.Table.from_pydict({
                                'id': [1], 'payload': [b'retried'], 'items': [None], 'mapping': [None],
                            }, schema=schema))
                            data_writer = next(iter(writer.file_store_write.data_writers.values()))
                            with patch.object(data_writer, stage, side_effect=OSError('injected')):
                                with self.assertRaisesRegex(OSError, 'injected'):
                                    writer.prepare_commit(1)
                            if shared:
                                first = data_writer.committed_files[0]
                                references = ManagedBlobReferenceFile.read(
                                    table.file_io, first.aligned_file_path(first.file_name + '.blobref'))
                                for reference in references:
                                    self.assertTrue(table.file_io.exists(self._pack_path(table, reference)))
                                if pack_size == '1 GB':
                                    self.assertEqual(sum(name.endswith('.managed.blob')
                                                         for name in self._names_under(table)), 1)
                            if action == 'abort':
                                writer.abort()
                                self.assertFalse(any(
                                    name.endswith(('.parquet', '.blobref', '.managed.blob'))
                                    for name in self._names_under(table)))
                                continue
                            if action == 'prepare':
                                messages = writer.prepare_commit(1)
                                files = [meta for message in messages for meta in message.new_files]
                                commit.commit(messages, 1)
                                writer.close()
                            else:
                                writer.close()
                                files = list(data_writer.committed_files)
                            references = set()
                            rows = []
                            for meta in files:
                                rows.extend(self._parquet_rows(table, meta))
                                for name in meta.extra_files:
                                    if name.endswith('.blobref'):
                                        references.update(reference.relative_path for reference in
                                                          ManagedBlobReferenceFile.read(
                                                              table.file_io, meta.aligned_file_path(name)))
                            packs = {name for name in self._names_under(table)
                                     if name.endswith('.managed.blob')}
                            self.assertTrue(references)
                            self.assertEqual(packs, references)
                            expected = {0: b'first', 1: b'retried'} if shared else {1: b'retried'}
                            self.assertEqual({row['id']: self._written_payload(table, row['payload'])
                                              for row in rows}, expected)
                        finally:
                            writer.close()
                            commit.close()

    def test_later_flush_failure_keeps_completed_pack(self):
        from pypaimon.blob.primary_key_blob_externalizer import PrimaryKeyBlobExternalizer
        table = self._create_table('pk_blob_flush_fail', {
            'bucket': '1',
            'write.native.enabled': 'false',
            'read.native.enabled': 'false',
            'scan.native-plan.enabled': 'false',
            'target-file-size': '1 B',
            'blob.target-file-size': '1 GB',
        })
        arrow_schema = PyarrowFieldParser.from_paimon_schema(table.fields)
        writer, commit = self._write(table)
        original_write = PrimaryKeyBlobExternalizer._write_descriptor
        original_take = PrimaryKeyBlobExternalizer.take_referenced_pack_names

        def take(self):
            names = original_take(self)
            self._files_done = getattr(self, '_files_done', 0) + 1
            return names

        def fail_later(self, field_name, raw):
            if getattr(self, '_files_done', 0) >= 1 and field_name == 'payload':
                raise OSError('injected')
            return original_write(self, field_name, raw)

        try:
            PrimaryKeyBlobExternalizer.take_referenced_pack_names = take
            PrimaryKeyBlobExternalizer._write_descriptor = fail_later
            writer.write_arrow(pa.Table.from_pydict({
                'id': [1],
                'payload': [b'first'],
                'items': [[b'a']],
                'mapping': [[('k', b'a')]],
            }, schema=arrow_schema))
            packs = [name for name in self._names_under(table) if name.endswith('.managed.blob')]
            self.assertTrue(packs)
            data_writer = next(iter(writer.file_store_write.data_writers.values()))
            data_writer.target_file_size = 1024 * 1024 * 1024
            writer.write_arrow(pa.Table.from_pydict({
                'id': [2],
                'payload': [b'second'],
                'items': [[b'b']],
                'mapping': [[('k', b'b')]],
            }, schema=arrow_schema))
            with self.assertRaises(OSError):
                writer.prepare_commit()
            for name in packs:
                self.assertTrue(any(found == name for found in self._names_under(table)))
        finally:
            PrimaryKeyBlobExternalizer.take_referenced_pack_names = original_take
            PrimaryKeyBlobExternalizer._write_descriptor = original_write
            writer.close()
            commit.close()

    def test_relative_warehouse_publishes_absolute_pack_uri(self):
        previous = os.getcwd()
        os.chdir(self.temp_dir)
        try:
            catalog = CatalogFactory.create({'warehouse': './wh'})
            catalog.create_database('default', False)
            fields = [
                DataField(0, 'id', AtomicType('INT', False)),
                DataField(1, 'payload', AtomicType('BLOB')),
            ]
            catalog.create_table('default.rel_blob', Schema(
                fields=fields, primary_keys=['id'], options={
                    'bucket': '1',
                    'write.native.enabled': 'false',
                    'read.native.enabled': 'false',
                    'scan.native-plan.enabled': 'false',
                }), False)
            table = catalog.get_table('default.rel_blob')
            messages = self._write_and_commit(table, pa.Table.from_pydict({
                'id': [1], 'payload': [b'visible'],
            }, schema=PyarrowFieldParser.from_paimon_schema(fields)))
            builder = table.new_read_builder()
            rows = builder.new_read().to_arrow(builder.new_scan().plan().splits()).to_pylist()
            self.assertEqual(rows, [{'id': 1, 'payload': b'visible'}])
            data_file = messages[0].new_files[0]
            descriptor = BlobDescriptor.deserialize(self._parquet_rows(table, data_file)[0]['payload'])
            self.assertTrue(os.path.isabs(descriptor.uri))
            self.assertTrue(descriptor.uri.startswith(os.path.abspath('./wh') + '/'))
            self.assertEqual(descriptor.uri.count('default.db'), 1)
            self.assertTrue(table.file_io.exists(descriptor.uri))
        finally:
            os.chdir(previous)

    def _create_table(self, name, options):
        fields = [
            DataField(0, 'id', AtomicType('INT', False)),
            DataField(1, 'payload', AtomicType('BLOB')),
            DataField(2, 'items', ArrayType(True, AtomicType('BLOB'))),
            DataField(3, 'mapping', MapType(
                True, AtomicType('STRING', False), AtomicType('BLOB'))),
        ]
        if options.get('rowkind.field') == 'op':
            fields.append(DataField(4, 'op', AtomicType('STRING')))
        schema = Schema(fields=fields, primary_keys=['id'], options=options)
        self.catalog.create_table('default.' + name, schema, False)
        return self.catalog.get_table('default.' + name)

    def _write(self, table):
        builder = table.new_batch_write_builder()
        return builder.new_write(), builder.new_commit()

    def _write_and_commit(self, table, data):
        writer, commit = self._write(table)
        try:
            writer.write_arrow(data)
            messages = writer.prepare_commit()
            commit.commit(messages)
            return messages
        finally:
            writer.close()
            commit.close()

    def _parquet_rows(self, table, meta):
        with table.file_io.new_input_stream(meta.physical_path()) as stream:
            return pq.ParquetFile(stream).read().to_pylist()

    def _names_under(self, table):
        names = []
        for dirpath, _, filenames in os.walk(str(table.table_path)):
            names.extend(filenames)
        return names

    def _written_payload(self, table, raw):
        return materialize_managed_blob(
            raw, 'scalar', table.file_io, table.table_path)

    def _pack_path(self, table, reference):
        return resolve_table_blob_uri(
            table.table_path,
            "%s/%s" % (reference.storage_root_id, reference.relative_path))
