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

import glob
import io
import os
import shutil
import tempfile
import unittest
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pyarrow as pa

from pypaimon.blob.primary_key_blob_externalizer import PrimaryKeyBlobExternalizer
from pypaimon.common.file_io import FileIO
from pypaimon.common.identifier import Identifier
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.blob_descriptor_reader_factory import BlobDescriptorReaderFactory
from pypaimon.table.row.blob import Blob, BlobDescriptor
from pypaimon.write.writer.data_writer import DataWriter
from pypaimon.write.file_store_write import FileStoreWrite
from pypaimon.write.table_write import TableWrite
from pypaimon.write.writer.dedicated_format_writer import DedicatedFormatWriter


class _ListOnlyDataWriter(DataWriter):

    def __init__(self):
        self.pending_data = None
        self.committed_files = ["data"]
        self.committed_changelog_files = ["changelog"]

    def _process_data(self, data):
        return data

    def _merge_data(self, existing_data, new_data):
        return new_data


class _TransactionalListWriter(_ListOnlyDataWriter):

    def __init__(self, name, fail_prepare=False):
        super().__init__()
        self.committed_files = [name]
        self.committed_changelog_files = []
        self.fail_prepare = fail_prepare
        self.aborted = []

    def _prepare_commit_data(self):
        if self.fail_prepare:
            raise RuntimeError("forced prepare failure")

    def abort(self):
        self.aborted.extend(self.committed_files)
        self.aborted.extend(self.committed_changelog_files)
        self.committed_files.clear()
        self.committed_changelog_files.clear()
        self._active_prepared_commit = None


class DataWriterOwnershipTest(unittest.TestCase):

    def test_prepare_drains_data_without_draining_changelog(self):
        writer = _ListOnlyDataWriter()

        self.assertEqual(writer.prepare_commit(), ["data"])
        self.assertEqual(writer.committed_files, [])
        self.assertEqual(writer.committed_changelog_files, ["changelog"])
        self.assertEqual(writer.prepare_changelog_commit(), ["changelog"])
        self.assertEqual(writer.committed_changelog_files, [])

    def test_file_store_prepare_failure_aborts_all_staged_buckets(self):
        first = _TransactionalListWriter("first")
        second = _TransactionalListWriter("second", fail_prepare=True)
        file_store = object.__new__(FileStoreWrite)
        file_store.data_writers = {
            (("p1",), 0): first,
            (("p2",), 0): second,
        }
        file_store.commit_identifier = 0
        file_store._active_prepared_commit = None

        with self.assertRaisesRegex(RuntimeError, "forced prepare failure"):
            file_store.prepare_commit(1)

        self.assertEqual(first.aborted, ["first"])
        self.assertEqual(second.aborted, ["second"])

    def test_failed_next_round_does_not_abort_previous_handoff(self):
        writer = _TransactionalListWriter("round-1")
        first = writer.prepare_commit()
        writer.committed_files.append("round-2")
        writer.fail_prepare = True

        with self.assertRaisesRegex(RuntimeError, "forced prepare failure"):
            writer.prepare_commit()

        self.assertEqual(first, ["round-1"])
        self.assertEqual(writer.aborted, ["round-2"])

    def test_dedicated_later_child_failure_aborts_earlier_stage(self):
        first = _TransactionalListWriter("blob-1")
        second = _TransactionalListWriter("blob-2", fail_prepare=True)
        writer = object.__new__(DedicatedFormatWriter)
        writer.pending_normal_data = None
        writer.blob_file_column_names = ["blob_1", "blob_2"]
        writer.blob_writers = {"blob_1": first, "blob_2": second}
        writer.vector_writer = None
        writer.committed_files = []

        with self.assertRaisesRegex(RuntimeError, "forced prepare failure"):
            writer._close_current_writers()

        self.assertEqual(first.aborted, ["blob-1"])
        self.assertEqual(second.aborted, ["blob-2"])

    def test_dedicated_validation_failure_keeps_child_owned_for_abort(self):
        child = _TransactionalListWriter("blob")
        writer = object.__new__(DedicatedFormatWriter)
        writer.pending_normal_data = SimpleNamespace(num_rows=1)
        writer._write_normal_data_to_file = Mock(return_value="normal")
        writer.blob_file_column_names = ["blob"]
        writer.blob_writers = {"blob": child}
        writer.vector_writer = None
        writer.committed_files = []
        writer._validate_consistency = Mock(
            side_effect=RuntimeError("forced consistency failure"))

        with self.assertRaisesRegex(RuntimeError, "forced consistency failure"):
            writer._close_current_writers()

        self.assertEqual(child.aborted, ["blob"])
        self.assertEqual(writer.committed_files, ["normal"])

    def test_index_prepare_failure_aborts_staged_data(self):
        prepared = Mock()
        prepared.messages = []
        table_write = object.__new__(TableWrite)
        table_write.file_store_write = Mock()
        table_write.file_store_write.stage_commit.return_value = prepared
        table_write.row_key_extractor = Mock()
        table_write.row_key_extractor.prepare_commit.side_effect = RuntimeError(
            "forced index failure")

        with self.assertRaisesRegex(RuntimeError, "forced index failure"):
            table_write._prepare_commit(1)

        prepared.abort.assert_called_once_with()
        table_write.row_key_extractor.abort.assert_called_once_with()

    def test_bucket_stage_failure_rolls_back_index_assignment_state(self):
        table_write = object.__new__(TableWrite)
        table_write.file_store_write = Mock()
        table_write.file_store_write.stage_commit.side_effect = RuntimeError(
            "forced bucket failure")
        table_write.row_key_extractor = Mock()

        with self.assertRaisesRegex(RuntimeError, "forced bucket failure"):
            table_write._prepare_commit(1)

        table_write.row_key_extractor.abort.assert_called_once_with()


class ManagedBlobWriteOwnershipTest(unittest.TestCase):

    def setUp(self):
        from pypaimon.catalog.catalog_factory import CatalogFactory

        self.temp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temp_dir, True)
        self.warehouse = os.path.join(self.temp_dir, "warehouse")
        self.catalog = CatalogFactory.create({"warehouse": self.warehouse})
        self.catalog.create_database("default", True)
        self.arrow_schema = pa.schema([
            pa.field("id", pa.int32(), nullable=False),
            pa.field("payload", pa.large_binary()),
        ])

    def _create_table(self, name):
        from pypaimon.schema.schema import Schema

        schema = Schema.from_pyarrow_schema(
            self.arrow_schema,
            primary_keys=["id"],
            options={
                "bucket": "1",
                "merge-engine": "deduplicate",
                "changelog-producer": "none",
            },
        )
        identifier = "default.%s" % name
        self.catalog.create_table(identifier, schema, False)
        return self.catalog.get_table(identifier)

    def _batch(self, row_id, payload):
        return pa.Table.from_pydict(
            {"id": [row_id], "payload": [payload]},
            schema=self.arrow_schema,
        )

    @staticmethod
    def _message_paths(messages):
        return {
            file.file_path
            for message in messages
            for file in message.new_files
        }

    def test_stream_prepare_transfers_only_current_round(self):
        table = self._create_table("stream_drain")
        builder = table.new_stream_write_builder()
        writer = builder.new_write()
        committer = builder.new_commit()
        try:
            writer.write_arrow(self._batch(1, b"one"))
            first = writer.prepare_commit(1)
            first_paths = self._message_paths(first)
            self.assertTrue(first_paths)
            committer.commit(first, 1)

            writer.write_arrow(self._batch(2, b"two"))
            second = writer.prepare_commit(2)
            second_paths = self._message_paths(second)
            self.assertTrue(second_paths)
            self.assertTrue(first_paths.isdisjoint(second_paths))
            committer.commit(second, 2)
        finally:
            writer.close()
            committer.close()

        for path in first_paths.union(second_paths):
            self.assertTrue(table.file_io.exists(path))

    def test_abort_after_prepare_does_not_delete_handed_off_files(self):
        table = self._create_table("abort_after_prepare")
        writer = table.new_stream_write_builder().new_write()
        writer.write_arrow(self._batch(1, b"prepared"))
        messages = writer.prepare_commit(1)
        paths = self._message_paths(messages)
        extra_paths = {
            "%s/%s" % (file.file_path.rsplit("/", 1)[0], extra)
            for message in messages
            for file in message.new_files
            for extra in file.extra_files
        }

        writer.abort()

        for path in paths.union(extra_paths):
            self.assertTrue(table.file_io.exists(path))

    def test_committer_abort_preserves_potentially_shared_managed_blob_packs(self):
        table = self._create_table("committer_abort_packs")
        builder = table.new_stream_write_builder()
        writer = builder.new_write()
        committer = builder.new_commit()
        writer.write_arrow(self._batch(1, b"prepared"))
        messages = writer.prepare_commit(1)
        packs = set(glob.glob(
            os.path.join(table.table_path, "**", "*.managed.blob"), recursive=True))
        self.assertTrue(packs)

        committer.abort(messages)

        for path in packs:
            self.assertTrue(table.file_io.exists(path))
        writer.close()
        committer.close()

    def test_close_without_prepare_removes_all_writer_owned_files(self):
        table = self._create_table("close_cleanup")
        writer = table.new_batch_write_builder().new_write()
        writer.write_arrow(self._batch(1, b"uncommitted"))

        writer.close()

        patterns = ("*.parquet", "*.orc", "*.avro", "*.blobref", "*.managed.blob")
        leftovers = []
        for pattern in patterns:
            leftovers.extend(glob.glob(
                os.path.join(table.table_path, "**", pattern), recursive=True))
        self.assertEqual(leftovers, [])

    def test_metadata_failure_removes_main_sidecar_and_managed_pack(self):
        table = self._create_table("metadata_failure_cleanup")
        writer = table.new_batch_write_builder().new_write()
        writer.write_arrow(self._batch(None, b"uncommitted"))

        with self.assertRaisesRegex(
                RuntimeError, r"(Primary key should not be null|contains nulls)"):
            writer.prepare_commit()
        writer.close()

        leftovers = []
        for pattern in ("*.parquet", "*.blobref", "*.managed.blob"):
            leftovers.extend(glob.glob(
                os.path.join(table.table_path, "**", pattern), recursive=True))
        self.assertEqual(leftovers, [])

    def test_failed_next_stream_prepare_keeps_previous_round_and_aborts_new_packs(self):
        table = self._create_table("stream_failure_isolation")
        writer = table.new_stream_write_builder().new_write()
        writer.write_arrow(self._batch(1, b"first"))
        first = writer.prepare_commit(1)
        first_paths = self._message_paths(first)
        first_extra_paths = {
            "%s/%s" % (file.file_path.rsplit("/", 1)[0], extra)
            for message in first
            for file in message.new_files
            for extra in file.extra_files
        }
        first_packs = set(glob.glob(
            os.path.join(table.table_path, "**", "*.managed.blob"), recursive=True))
        self.assertTrue(first_packs)

        writer.write_arrow(self._batch(2, b"second"))
        with patch.object(
                writer.row_key_extractor,
                "prepare_commit",
                create=True,
                side_effect=RuntimeError("forced index failure")):
            with self.assertRaisesRegex(RuntimeError, "forced index failure"):
                writer.prepare_commit(2)

        for path in first_paths.union(first_extra_paths).union(first_packs):
            self.assertTrue(table.file_io.exists(path))
        self.assertEqual(
            first_packs,
            set(glob.glob(
                os.path.join(table.table_path, "**", "*.managed.blob"), recursive=True)),
        )
        writer.close()


class BlobDescriptorSourceTableTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temp_dir, True)
        self.target_file_io = FileIO.get(self.temp_dir, Options({}))

    def test_source_table_reader_copies_descriptor_payload_to_new_pack(self):
        payload = b"source-table-credential-payload"
        source_file_io = _SourceFileIO(payload)
        catalog = _SourceCatalog(source_file_io)
        context = object()
        environment = _SourceCatalogEnvironment(catalog, context)
        table = SimpleNamespace(
            file_io=self.target_file_io,
            options=CoreOptions(Options({
                "blob-descriptor.source-table": "default.source$branch_rt",
                "blob-descriptor.fs.s3.endpoint": "must-be-ignored",
            })),
            catalog_environment=environment,
        )
        reader_factory = BlobDescriptorReaderFactory.create(table)
        pack_path = os.path.join(self.temp_dir, "target.managed.blob")
        externalizer = PrimaryKeyBlobExternalizer(
            self.target_file_io,
            [DataField(0, "payload", AtomicType("BLOB"))],
            {"payload"},
            lambda: pack_path,
            1024 * 1024,
            4096,
            "data-",
            reader_factory,
        )
        descriptor = BlobDescriptor(
            "source://credential-scoped/object", 0, len(payload)).serialize()
        batch = pa.RecordBatch.from_arrays(
            [pa.array([descriptor], type=pa.large_binary())],
            names=["payload"],
        )

        result = externalizer.externalize_record_batch(batch)
        externalizer.prepare_commit()
        copied = Blob.from_bytes(
            result.column("payload")[0].as_py(), self.target_file_io).to_data()
        externalizer.close()

        self.assertEqual(copied, payload)
        self.assertEqual(source_file_io.opened, ["source://credential-scoped/object"])
        self.assertEqual(catalog.identifier.get_database_name(), "default")
        self.assertEqual(catalog.identifier.get_table_name(), "source")
        self.assertEqual(catalog.identifier.get_branch_name(), "rt")
        self.assertTrue(source_file_io.token_initialized)
        self.assertTrue(source_file_io.closed)
        self.assertTrue(catalog.closed)

    def test_static_descriptor_options_are_isolated_and_prefix_stripped(self):
        target_factory = object()
        table = SimpleNamespace(
            file_io=SimpleNamespace(uri_reader_factory=target_factory),
            options=CoreOptions(Options({
                "fs.s3.endpoint": "target-endpoint",
                "blob-descriptor.fs.s3.endpoint": "source-endpoint",
                "blob-descriptor.fs.s3.accessKeyId": "source-key",
                "blob-descriptor.fs.s3.accessKeySecret": "source-secret",
            })),
        )

        reader_factory = BlobDescriptorReaderFactory.create(table)

        self.assertIsNot(reader_factory, target_factory)
        self.assertEqual(
            {
                "fs.s3.endpoint": "source-endpoint",
                "fs.s3.accessKeyId": "source-key",
                "fs.s3.accessKeySecret": "source-secret",
            },
            reader_factory.catalog_options.to_map(),
        )

    def test_without_descriptor_options_reuses_target_reader_factory(self):
        target_factory = object()
        table = SimpleNamespace(
            file_io=SimpleNamespace(uri_reader_factory=target_factory),
            options=CoreOptions(Options({"fs.s3.endpoint": "target-endpoint"})),
        )

        self.assertIs(
            target_factory, BlobDescriptorReaderFactory.create(table))

    def test_source_table_reader_copies_v1_descriptor_payload(self):
        payload = b"legacy-source-descriptor"
        source_file_io = _SourceFileIO(payload)
        catalog = _SourceCatalog(source_file_io)
        context = object()
        table = SimpleNamespace(
            file_io=self.target_file_io,
            options=CoreOptions(Options({
                "blob-descriptor.source-table": "default.source",
            })),
            catalog_environment=_SourceCatalogEnvironment(catalog, context),
        )
        reader_factory = BlobDescriptorReaderFactory.create(table)
        pack_path = os.path.join(self.temp_dir, "target-v1.managed.blob")
        externalizer = PrimaryKeyBlobExternalizer(
            self.target_file_io,
            [DataField(0, "payload", AtomicType("BLOB"))],
            {"payload"},
            lambda: pack_path,
            1024 * 1024,
            4096,
            "data-",
            reader_factory,
        )
        descriptor = BlobDescriptor(
            "source://credential-scoped/v1", 0, len(payload))
        descriptor._version = 1
        batch = pa.RecordBatch.from_arrays(
            [pa.array([descriptor.serialize()], type=pa.large_binary())],
            names=["payload"],
        )

        result = externalizer.externalize_record_batch(batch)
        externalizer.prepare_commit()

        copied = Blob.from_descriptor_bytes(
            result.column("payload")[0].as_py(), self.target_file_io).to_data()
        self.assertEqual(copied, payload)
        self.assertEqual(
            source_file_io.opened, ["source://credential-scoped/v1"])

    def test_static_descriptor_options_reader_copies_v1_descriptor_payload(self):
        payload = b"legacy-static-descriptor"
        source_file_io = _SourceFileIO(payload)
        table = SimpleNamespace(
            file_io=SimpleNamespace(uri_reader_factory=object()),
            options=CoreOptions(Options({
                "blob-descriptor.fs.default-scheme": "source",
            })),
            catalog_environment=SimpleNamespace(catalog_loader=None),
        )
        pack_path = os.path.join(self.temp_dir, "static-v1.managed.blob")
        descriptor = BlobDescriptor(
            "source://credential-scoped/v1", 0, len(payload))
        descriptor._version = 1
        batch = pa.RecordBatch.from_arrays(
            [pa.array([descriptor.serialize()], type=pa.large_binary())],
            names=["payload"],
        )
        with patch("pypaimon.common.file_io.FileIO.get", return_value=source_file_io):
            reader_factory = BlobDescriptorReaderFactory.create(table)
            self.assertTrue(reader_factory.force_descriptor_bytes)
            externalizer = PrimaryKeyBlobExternalizer(
                self.target_file_io,
                [DataField(0, "payload", AtomicType("BLOB"))],
                {"payload"},
                lambda: pack_path,
                1024 * 1024,
                4096,
                "data-",
                reader_factory,
            )
            result = externalizer.externalize_record_batch(batch)
            externalizer.prepare_commit()

        copied = Blob.from_descriptor_bytes(
            result.column("payload")[0].as_py(), self.target_file_io).to_data()
        self.assertEqual(copied, payload)

    def test_source_table_requires_catalog_loader(self):
        table = SimpleNamespace(
            file_io=self.target_file_io,
            options=CoreOptions(Options({
                "blob-descriptor.source-table": "default.source",
            })),
            catalog_environment=SimpleNamespace(catalog_loader=None),
        )

        with self.assertRaisesRegex(
                ValueError, "tables without a catalog loader"):
            BlobDescriptorReaderFactory.create(table)


class _SourceFileIO:

    def __init__(self, payload):
        self.payload = payload
        self.opened = []
        self.token_initialized = False
        self.closed = False

    def valid_token(self):
        self.token_initialized = True
        return object()

    def new_input_stream(self, uri):
        self.opened.append(uri)
        return io.BytesIO(self.payload)

    def close(self):
        self.closed = True


class _SourceCatalog:

    def __init__(self, file_io):
        self.file_io = file_io
        self.identifier = None
        self.closed = False

    def get_table(self, identifier: Identifier):
        self.identifier = identifier
        return SimpleNamespace(file_io=self.file_io)

    def close(self):
        self.closed = True


class _SourceCatalogLoader:

    def __init__(self, catalog, context):
        self.catalog = catalog
        self._context = context

    def context(self):
        return self._context

    def load(self):
        return self.catalog


class _SourceCatalogEnvironment:

    def __init__(self, catalog, context):
        self.catalog_loader = _SourceCatalogLoader(catalog, context)
        self._context = context

    def catalog_context(self):
        return self._context

    def dependency_read_context(self):
        return self._context


if __name__ == "__main__":
    unittest.main()
