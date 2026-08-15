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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import io
import os
import shutil
import struct
import tempfile
import unittest
from unittest.mock import Mock, patch

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.read.reader.managed_blob_convert_record_reader import (
    convert_managed_blob_batch,
)
from pypaimon.read.split_read import MergeFileSplitRead, RawFileSplitRead
from pypaimon.schema.data_types import ArrayType, AtomicType, DataField, MapType
from pypaimon.table.row.blob import Blob, BlobDescriptor, BlobViewStruct
from pypaimon.table.row.offset_row import OffsetRow


class _OneBatchReader(RecordBatchReader):

    def __init__(self, batch):
        self._batch = batch

    def read_arrow_batch(self):
        batch = self._batch
        self._batch = None
        return batch

    def close(self):
        pass


class _OneRowIterator(RecordIterator):

    def __init__(self, row):
        self._row = row

    def next(self):
        row = self._row
        self._row = None
        return row


class _OneRowReader(RecordReader):

    def __init__(self, row):
        self._row = row

    def read_batch(self):
        if self._row is None:
            return None
        row = self._row
        self._row = None
        return _OneRowIterator(row)

    def close(self):
        pass


class _FakeBlobViewLookup:

    preload_calls = []
    close_calls = 0

    def __init__(self, table):
        self._table = table

    def preload(self, structs):
        self.preload_calls.append(list(structs))

    def resolve_to_null(self, _view_struct):
        return False

    def resolve_descriptor(self, _view_struct):
        return self._table.view_descriptor

    def resolve_blob(self, _view_struct):
        return Blob.from_data(self._table.view_payload)

    def close(self):
        type(self).close_calls += 1


def _v1_descriptor(uri, offset, length):
    uri_bytes = uri.encode("utf-8")
    return (
        b"\x01"
        + struct.pack("<I", len(uri_bytes))
        + uri_bytes
        + struct.pack("<q", offset)
        + struct.pack("<q", length)
    )


class PrimaryKeyInlineBlobCompatibilityTest(unittest.TestCase):

    def setUp(self):
        _FakeBlobViewLookup.preload_calls = []
        _FakeBlobViewLookup.close_calls = 0
        self.descriptor_payload = b"descriptor-payload"
        self.view_payload = b"view-payload"
        self.pack = b"prefix" + self.descriptor_payload + b"suffix"
        self.descriptor = _v1_descriptor(
            "file:///managed-v1.blob",
            len(b"prefix"),
            len(self.descriptor_payload),
        )
        self.view_struct = BlobViewStruct("db.source", 7, 42).serialize()
        self.view_descriptor = BlobDescriptor(
            "file:///view.blob", 0, len(self.view_payload))
        self.fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "inline_descriptor", AtomicType("BLOB")),
            DataField(2, "inline_view", AtomicType("BLOB")),
        ]

    def _table(self, **options):
        values = {
            "blob-descriptor-field": "inline_descriptor",
            "blob-view-field": "inline_view",
        }
        values.update(options)
        table = Mock()
        table.is_primary_key_table = True
        table.options = CoreOptions(Options(values))
        table.file_io.new_input_stream.side_effect = lambda _: io.BytesIO(self.pack)
        table.catalog_environment.catalog_loader = object()
        table.view_descriptor = self.view_descriptor
        table.view_payload = self.view_payload
        return table

    def _create_real_pk_inline_table(self, table_name, versions):
        from pypaimon import CatalogFactory, Schema

        temp_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, temp_dir, True)
        catalog = CatalogFactory.create({
            "warehouse": os.path.join(temp_dir, "warehouse"),
        })
        catalog.create_database("default", True)
        arrow_schema = pa.schema([
            pa.field("id", pa.int32(), nullable=False),
            pa.field("inline_descriptor", pa.large_binary()),
            pa.field("inline_view", pa.large_binary()),
        ])
        schema = Schema.from_pyarrow_schema(
            arrow_schema,
            primary_keys=["id"],
            options={
                "bucket": "1",
                "file.format": "parquet",
                "merge-engine": "deduplicate",
                "changelog-producer": "none",
                "blob-descriptor-field": "inline_descriptor",
                "blob-view-field": "inline_view",
            },
        )
        identifier = "default.%s" % table_name
        catalog.create_table(identifier, schema, False)
        table = catalog.get_table(identifier)

        for version, (descriptor_payload, view_payload) in enumerate(versions):
            external_path = os.path.join(
                temp_dir, "descriptor-payload-%s" % version)
            prefix = b"prefix-"
            with open(external_path, "wb") as out:
                out.write(prefix + descriptor_payload)
            descriptor = BlobDescriptor(
                external_path, len(prefix), len(descriptor_payload)).serialize()
            view = BlobViewStruct(
                "default.controlled_source", 7, version).serialize()
            write_builder = table.new_batch_write_builder()
            writer = write_builder.new_write()
            committer = write_builder.new_commit()
            try:
                writer.write_arrow(pa.Table.from_pydict({
                    "id": [1],
                    "inline_descriptor": [descriptor],
                    "inline_view": [view],
                }, schema=arrow_schema))
                committer.commit(writer.prepare_commit())
            finally:
                writer.close()
                committer.close()

        table.view_payload = versions[-1][1]
        table.view_descriptor = BlobDescriptor(
            "file:///controlled-view.blob", 0, len(table.view_payload))
        return table

    def _read_real_table_with_reader_tracking(self, table, projection):
        raw_calls = []
        merge_calls = []
        raw_create = RawFileSplitRead.create_reader
        merge_create = MergeFileSplitRead.create_reader

        def track_raw(split_read):
            raw_calls.append(split_read)
            return raw_create(split_read)

        def track_merge(split_read):
            merge_calls.append(split_read)
            return merge_create(split_read)

        with patch.object(RawFileSplitRead, "create_reader", new=track_raw), \
                patch.object(
                    MergeFileSplitRead, "create_reader", new=track_merge):
            read_builder = table.new_read_builder().with_projection(projection)
            result = read_builder.new_read().to_arrow(
                read_builder.new_scan().plan().splits())
        return result, raw_calls, merge_calls

    @patch(
        "pypaimon.utils.blob_view_lookup.BlobViewLookup",
        _FakeBlobViewLookup,
    )
    def test_raw_reader_resolves_inline_descriptor_and_view_after_output_stage(self):
        schema = pa.schema([
            pa.field("id", pa.int32(), nullable=False),
            pa.field("inline_descriptor", pa.large_binary()),
            pa.field("inline_view", pa.large_binary()),
        ])
        batch = pa.RecordBatch.from_arrays([
            pa.array([1], type=pa.int32()),
            pa.array([self.descriptor], type=pa.large_binary()),
            pa.array([self.view_struct], type=pa.large_binary()),
        ], schema=schema)
        split_read = RawFileSplitRead.__new__(RawFileSplitRead)
        split_read.table = self._table()
        split_read.value_fields = self.fields
        split_read.outer_extract_name_paths = None
        split_read.outer_flat_read_type = None

        wrapped = split_read._wrap_managed_blob_reader(_OneBatchReader(batch))
        result = wrapped.read_arrow_batch()

        self.assertEqual(schema, result.schema)
        self.assertEqual(
            [{
                "id": 1,
                "inline_descriptor": self.descriptor_payload,
                "inline_view": self.view_payload,
            }],
            result.to_pylist(),
        )
        self.assertEqual(1, len(_FakeBlobViewLookup.preload_calls))

    @patch(
        "pypaimon.utils.blob_view_lookup.BlobViewLookup",
        _FakeBlobViewLookup,
    )
    def test_wrapped_reader_close_releases_view_lookup(self):
        batch = pa.record_batch(
            [pa.array([self.view_struct], type=pa.large_binary())],
            names=["inline_view"],
        )
        split_read = RawFileSplitRead.__new__(RawFileSplitRead)
        split_read.table = self._table()
        split_read.value_fields = [self.fields[2]]
        split_read.outer_extract_name_paths = None
        split_read.outer_flat_read_type = None
        wrapped = split_read._wrap_managed_blob_reader(_OneBatchReader(batch))

        wrapped.close()

        self.assertEqual(1, _FakeBlobViewLookup.close_calls)

    @patch(
        "pypaimon.utils.blob_view_lookup.BlobViewLookup",
        _FakeBlobViewLookup,
    )
    def test_merge_reader_resolves_projected_inline_fields_by_output_index(self):
        projected_fields = [self.fields[2], self.fields[1]]
        row = OffsetRow((self.view_struct, self.descriptor), 0, 2)
        split_read = MergeFileSplitRead.__new__(MergeFileSplitRead)
        split_read.table = self._table()
        split_read.value_fields = self.fields
        split_read.outer_extract_name_paths = [
            ["inline_view"], ["inline_descriptor"]]
        split_read.outer_flat_read_type = projected_fields

        wrapped = split_read._wrap_managed_blob_reader(_OneRowReader(row))
        result = wrapped.read_batch().next()

        self.assertIsInstance(result, OffsetRow)
        self.assertEqual(
            (self.view_payload, self.descriptor_payload), result.row_tuple)

    @patch(
        "pypaimon.utils.blob_view_lookup.BlobViewLookup",
        _FakeBlobViewLookup,
    )
    def test_merge_descriptor_mode_keeps_descriptor_and_resolves_view_reference(self):
        projected_fields = [self.fields[2], self.fields[1]]
        row = OffsetRow((self.view_struct, self.descriptor), 0, 2)
        split_read = MergeFileSplitRead.__new__(MergeFileSplitRead)
        split_read.table = self._table(**{"blob-as-descriptor": "true"})
        split_read.value_fields = self.fields
        split_read.outer_extract_name_paths = [
            ["inline_view"], ["inline_descriptor"]]
        split_read.outer_flat_read_type = projected_fields

        result = split_read._wrap_managed_blob_reader(
            _OneRowReader(row)).read_batch().next()

        self.assertEqual(
            (self.view_descriptor.serialize(), self.descriptor),
            result.row_tuple,
        )

    @patch(
        "pypaimon.utils.blob_view_lookup.BlobViewLookup",
        _FakeBlobViewLookup,
    )
    def test_descriptor_mode_resolves_view_to_descriptor_but_keeps_inline_descriptor(self):
        batch = pa.record_batch(
            [
                pa.array([self.descriptor], type=pa.large_binary()),
                pa.array([self.view_struct], type=pa.large_binary()),
            ],
            names=["inline_descriptor", "inline_view"],
        )
        split_read = RawFileSplitRead.__new__(RawFileSplitRead)
        split_read.table = self._table(**{"blob-as-descriptor": "true"})
        split_read.value_fields = self.fields[1:]
        split_read.outer_extract_name_paths = None
        split_read.outer_flat_read_type = None

        result = split_read._wrap_managed_blob_reader(
            _OneBatchReader(batch)).read_arrow_batch()

        self.assertEqual(self.descriptor, result.column(0)[0].as_py())
        self.assertEqual(
            self.view_descriptor.serialize(), result.column(1)[0].as_py())

    def test_disabled_view_resolution_preserves_view_metadata(self):
        batch = pa.record_batch(
            [
                pa.array([self.descriptor], type=pa.large_binary()),
                pa.array([self.view_struct], type=pa.large_binary()),
            ],
            names=["inline_descriptor", "inline_view"],
        )
        split_read = RawFileSplitRead.__new__(RawFileSplitRead)
        split_read.table = self._table(**{"blob-view.resolve.enabled": "false"})
        split_read.value_fields = self.fields[1:]
        split_read.outer_extract_name_paths = None
        split_read.outer_flat_read_type = None

        result = split_read._wrap_managed_blob_reader(
            _OneBatchReader(batch)).read_arrow_batch()

        self.assertEqual(self.descriptor_payload, result.column(0)[0].as_py())
        self.assertEqual(self.view_struct, result.column(1)[0].as_py())

    @patch(
        "pypaimon.utils.blob_view_lookup.BlobViewLookup",
        _FakeBlobViewLookup,
    )
    def test_real_raw_scan_installs_inline_descriptor_and_view_wrapper(self):
        table = self._create_real_pk_inline_table(
            "pk_inline_raw",
            [(b"raw-descriptor", b"raw-view")],
        )

        result, raw_calls, merge_calls = self._read_real_table_with_reader_tracking(
            table, ["id", "inline_descriptor", "inline_view"])

        self.assertTrue(raw_calls)
        self.assertFalse(merge_calls)
        self.assertEqual(result.to_pylist(), [{
            "id": 1,
            "inline_descriptor": b"raw-descriptor",
            "inline_view": b"raw-view",
        }])
        self.assertTrue(_FakeBlobViewLookup.preload_calls)

    @patch(
        "pypaimon.utils.blob_view_lookup.BlobViewLookup",
        _FakeBlobViewLookup,
    )
    def test_real_merge_scan_installs_projected_inline_wrapper(self):
        table = self._create_real_pk_inline_table(
            "pk_inline_merge",
            [
                (b"old-descriptor", b"old-view"),
                (b"new-descriptor", b"new-view"),
            ],
        )

        result, raw_calls, merge_calls = self._read_real_table_with_reader_tracking(
            table, ["inline_view", "inline_descriptor"])

        self.assertFalse(raw_calls)
        self.assertTrue(merge_calls)
        self.assertEqual(result.to_pylist(), [{
            "inline_view": b"new-view",
            "inline_descriptor": b"new-descriptor",
        }])
        self.assertTrue(_FakeBlobViewLookup.preload_calls)


class LegacyManagedBlobDescriptorTest(unittest.TestCase):

    def test_known_descriptor_context_supports_v1_without_changing_heuristic_mode(self):
        payload = b"legacy"
        descriptor = _v1_descriptor("file:///legacy.blob", 2, len(payload))
        file_io = Mock()
        file_io.new_input_stream.side_effect = lambda _: io.BytesIO(b"xx" + payload)

        self.assertEqual(
            payload, Blob.from_descriptor_bytes(descriptor, file_io).to_data())
        self.assertEqual(
            payload,
            Blob.from_bytes(
                descriptor, file_io, allow_blob_data=False).to_data(),
        )
        self.assertEqual(descriptor, Blob.from_bytes(descriptor, file_io).to_data())

    def test_managed_v1_scalar_array_and_map_are_resolved_recursively(self):
        first = b"first"
        second = b"second"
        pack = first + b"/" + second
        first_descriptor = _v1_descriptor(
            "file:///legacy.pack", 0, len(first))
        second_descriptor = _v1_descriptor(
            "file:///legacy.pack", len(first) + 1, len(second))
        map_type = pa.map_(pa.field("key", pa.string(), nullable=False),
                           pa.field("value", pa.large_binary(), nullable=True))
        schema = pa.schema([
            pa.field("scalar", pa.large_binary()),
            pa.field("array", pa.list_(pa.field("item", pa.large_binary()))),
            pa.field("map", map_type),
        ])
        batch = pa.RecordBatch.from_arrays([
            pa.array([first_descriptor], type=schema.field("scalar").type),
            pa.array(
                [[first_descriptor, None, second_descriptor]],
                type=schema.field("array").type,
            ),
            pa.array(
                [[("a", first_descriptor), ("b", None),
                  ("c", second_descriptor)]],
                type=map_type,
            ),
        ], schema=schema)
        file_io = Mock()
        file_io.new_input_stream.side_effect = lambda _: io.BytesIO(pack)

        result = convert_managed_blob_batch(
            batch, file_io, {"scalar", "array", "map"})

        self.assertEqual(schema, result.schema)
        self.assertEqual(first, result.column("scalar")[0].as_py())
        self.assertEqual(
            [first, None, second], result.column("array")[0].as_py())
        self.assertEqual(
            [("a", first), ("b", None), ("c", second)],
            result.column("map")[0].as_py(),
        )

    def test_merge_managed_array_map_use_projected_output_indices(self):
        first = b"first"
        second = b"second"
        pack = first + b"/" + second
        first_descriptor = _v1_descriptor(
            "file:///legacy.pack", 0, len(first))
        second_descriptor = _v1_descriptor(
            "file:///legacy.pack", len(first) + 1, len(second))
        array_field = DataField(
            1, "managed_array", ArrayType(True, AtomicType("BLOB")))
        map_field = DataField(
            2,
            "managed_map",
            MapType(True, AtomicType("STRING", False), AtomicType("BLOB")),
        )
        table = Mock()
        table.is_primary_key_table = True
        table.options = CoreOptions(Options({
            "blob-field": "managed_array,managed_map",
        }))
        table.file_io.new_input_stream.side_effect = lambda _: io.BytesIO(pack)
        split_read = MergeFileSplitRead.__new__(MergeFileSplitRead)
        split_read.table = table
        split_read.value_fields = [
            DataField(0, "id", AtomicType("INT")), array_field, map_field]
        split_read.outer_extract_name_paths = [
            ["managed_map"], ["managed_array"]]
        split_read.outer_flat_read_type = [map_field, array_field]
        row = OffsetRow((
            [("a", first_descriptor), ("b", second_descriptor)],
            [second_descriptor, None, first_descriptor],
        ), 0, 2)

        result = split_read._wrap_managed_blob_reader(
            _OneRowReader(row)).read_batch().next()

        self.assertEqual(
            [("a", first), ("b", second)], result.get_field(0))
        self.assertEqual(
            [second, None, first], result.get_field(1))


if __name__ == "__main__":
    unittest.main()
