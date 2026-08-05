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
import unittest
from unittest.mock import Mock, patch

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.read.read_builder import ReadBuilder
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.reader.managed_blob_convert_record_reader import (
    ManagedBlobConvertBatchReader,
    _ConvertedRow,
)
from pypaimon.read.split_read import RawFileSplitRead
from pypaimon.read.table_read import (TableRead,
                                      validate_primary_key_blob_predicate)
from pypaimon.schema.data_types import ArrayType, AtomicType, DataField, MapType
from pypaimon.table.row.blob import BlobData, BlobDescriptor


class _OneBatchReader(RecordBatchReader):

    def __init__(self, batch):
        self._batch = batch

    def read_arrow_batch(self):
        batch = self._batch
        self._batch = None
        return batch

    def close(self):
        pass


class PrimaryKeyBlobReadContractTest(unittest.TestCase):

    @staticmethod
    def _table_read(primary_key, data_evolution):
        read = TableRead.__new__(TableRead)
        read.table = Mock()
        read.table.is_primary_key_table = primary_key
        read.table.options.data_evolution_enabled.return_value = data_evolution
        read.table.options.row_tracking_enabled.return_value = data_evolution
        read.table.options.sequence_field.return_value = []
        read.predicate = None
        read.read_type = [DataField(0, "id", AtomicType("INT"))]
        read._scan_read_type = read.read_type
        read._predicate_extra_fields = []
        read.nested_name_paths = None
        read.limit = None
        return read

    @patch("pypaimon.read.table_read.DataEvolutionSplitRead")
    @patch("pypaimon.read.table_read.RawFileSplitRead")
    def test_primary_key_data_evolution_raw_split_uses_raw_reader(
            self, raw_read, data_evolution_read):
        read = self._table_read(primary_key=True, data_evolution=True)
        split = Mock(raw_convertible=True)

        result = read._build_split_read(split)

        self.assertIs(result, raw_read.return_value)
        raw_read.assert_called_once()
        data_evolution_read.assert_not_called()

    @patch("pypaimon.read.table_read.DataEvolutionSplitRead")
    @patch("pypaimon.read.table_read.MergeFileSplitRead")
    def test_primary_key_data_evolution_non_raw_split_uses_merge_reader(
            self, merge_read, data_evolution_read):
        read = self._table_read(primary_key=True, data_evolution=True)
        split = Mock(raw_convertible=False)

        result = read._build_split_read(split)

        self.assertIs(result, merge_read.return_value)
        merge_read.assert_called_once()
        data_evolution_read.assert_not_called()

    @patch("pypaimon.read.table_read.DataEvolutionSplitRead")
    def test_append_data_evolution_split_keeps_data_evolution_reader(
            self, data_evolution_read):
        read = self._table_read(primary_key=False, data_evolution=True)
        split = Mock(raw_convertible=True)

        result = read._build_split_read(split)

        self.assertIs(result, data_evolution_read.return_value)
        data_evolution_read.assert_called_once()

    def test_managed_blob_default_resolves_payload_and_descriptor_mode_bypasses(self):
        payload = b"managed payload"
        pack_bytes = b"prefix" + payload + b"suffix"
        descriptor = BlobDescriptor(
            "file:///managed.pack", len(b"prefix"), len(payload)).serialize()
        batch = pa.record_batch(
            [pa.array([descriptor], type=pa.binary())], names=["payload"])
        field = DataField(0, "payload", AtomicType("BLOB"))
        file_io = Mock()
        file_io.new_input_stream.side_effect = lambda _: io.BytesIO(pack_bytes)

        split_read = RawFileSplitRead.__new__(RawFileSplitRead)
        split_read.value_fields = [field]
        split_read.outer_extract_name_paths = None
        split_read.outer_flat_read_type = None
        split_read.table = Mock()
        split_read.table.file_io = file_io
        split_read.table.options = CoreOptions(Options({"blob-field": "payload"}))

        wrapped = split_read._wrap_managed_blob_reader(_OneBatchReader(batch))
        self.assertIsInstance(wrapped, ManagedBlobConvertBatchReader)
        self.assertEqual(
            [payload], wrapped.read_arrow_batch().column("payload").to_pylist())

        split_read.table.options = CoreOptions(Options({
            "blob-field": "payload",
            "blob-as-descriptor": "true",
        }))
        descriptor_reader = _OneBatchReader(batch)
        self.assertIs(
            descriptor_reader,
            split_read._wrap_managed_blob_reader(descriptor_reader),
        )
        self.assertEqual(
            [descriptor],
            descriptor_reader.read_arrow_batch().column("payload").to_pylist(),
        )

    def test_converted_row_get_blob_returns_blob_object(self):
        payload = b"managed payload"
        pack_bytes = b"prefix" + payload + b"suffix"
        descriptor = BlobDescriptor(
            "file:///managed.pack", len(b"prefix"), len(payload)).serialize()
        file_io = Mock()
        file_io.new_input_stream.side_effect = lambda _: io.BytesIO(pack_bytes)

        class _InnerRow:
            def get_field(self, pos):
                return descriptor

            def get_blob(self, pos):
                raise AssertionError("should delegate to converted row")

            def get_row_kind(self):
                return 0

            def get_vector(self, pos):
                raise NotImplementedError

            def __len__(self):
                return 1

        reader = _ConvertedRow(
            _InnerRow(),
            file_io,
            blob_field_indices={0},
            descriptor_field_indices=set(),
            view_field_indices=set(),
            blob_as_descriptor=False,
            view_resolver=None,
        )
        blob = reader.get_blob(0)
        self.assertIsInstance(blob, BlobData)
        self.assertEqual(payload, blob.to_data())


class BlobDescriptorConvertReaderTest(unittest.TestCase):

    def test_descriptor_field_resolves_v1_descriptor_bytes(self):
        from pypaimon.read.reader.blob_descriptor_convert_reader import (
            BlobInlineConvertReader,
        )

        payload = b"v1-inline-payload"
        pack_bytes = b"xx" + payload + b"yy"
        descriptor = BlobDescriptor("file:///pack.blob", 2, len(payload))
        descriptor._version = 1
        batch = pa.record_batch(
            [pa.array([descriptor.serialize()], type=pa.large_binary())],
            names=["payload"],
        )
        table = Mock()
        table.file_io = Mock()
        table.file_io.new_input_stream.side_effect = lambda _: io.BytesIO(pack_bytes)
        table.options = CoreOptions(Options({
            "blob-descriptor-field": "payload",
        }))
        table.catalog_environment = Mock(catalog_loader=None)

        reader = BlobInlineConvertReader(_OneBatchReader(batch), table)
        result = reader.read_arrow_batch()
        self.assertEqual([payload], result.column("payload").to_pylist())


class BlobPredicateBuilderContractTest(unittest.TestCase):

    @staticmethod
    def _validate(fields, predicate, primary_key=True):
        table = Mock()
        table.is_primary_key_table = primary_key
        table.fields = fields
        validate_primary_key_blob_predicate(table, predicate)

    def test_blob_shapes_reject_value_comparison_and_allow_null_checks(self):
        fields = [
            DataField(0, "scalar", AtomicType("BLOB")),
            DataField(1, "array", ArrayType(True, AtomicType("BLOB"))),
            DataField(
                2,
                "map",
                MapType(True, AtomicType("STRING", False), AtomicType("BLOB")),
            ),
        ]
        builder = PredicateBuilder(fields)

        for field in fields:
            with self.subTest(field=field.name):
                with self.assertRaisesRegex(ValueError, "only support isNull and isNotNull"):
                    self._validate(fields, builder.equal(field.name, b"payload"))
                self._validate(fields, builder.is_null(field.name))
                self._validate(fields, builder.is_not_null(field.name))

    def test_append_blob_predicate_builder_api_is_unchanged(self):
        fields = [DataField(0, "payload", AtomicType("BLOB"))]
        predicate = PredicateBuilder(fields).equal("payload", b"value")

        self.assertEqual("equal", predicate.method)
        self._validate(fields, predicate, primary_key=False)

    def test_compound_predicate_validates_blob_leaf(self):
        fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "payload", AtomicType("BLOB")),
        ]
        builder = PredicateBuilder(fields)
        predicate = PredicateBuilder.and_predicates([
            builder.equal("id", 1),
            builder.equal("payload", b"value"),
        ])

        with self.assertRaisesRegex(ValueError, "field 'payload'"):
            self._validate(fields, predicate)

    def test_read_builder_scan_read_and_explain_share_blob_validation(self):
        fields = [
            DataField(0, "id", AtomicType("INT")),
            DataField(1, "payload", AtomicType("BLOB")),
        ]
        table = Mock()
        table.is_primary_key_table = True
        table.fields = fields
        predicate = PredicateBuilder(fields).equal("payload", b"value")
        builder = ReadBuilder(table).with_filter(predicate)

        for boundary in (builder.new_scan, builder.new_read, builder.explain):
            with self.subTest(boundary=boundary.__name__):
                with self.assertRaisesRegex(
                        ValueError, "only support isNull and isNotNull"):
                    boundary()

    @patch("pypaimon.read.read_builder._build_explain_result")
    @patch("pypaimon.read.read_builder.TableRead")
    @patch("pypaimon.read.read_builder.TableScan")
    def test_read_builder_keeps_append_blob_predicates_unchanged(
            self, table_scan, table_read, build_explain_result):
        fields = [DataField(0, "payload", AtomicType("BLOB"))]
        table = Mock()
        table.is_primary_key_table = False
        table.fields = fields
        table.options.row_tracking_enabled.return_value = False
        predicate = PredicateBuilder(fields).equal("payload", b"value")
        builder = ReadBuilder(table).with_filter(predicate)
        table_scan.return_value.scan_with_stats.return_value = (Mock(), Mock())

        self.assertIs(table_scan.return_value, builder.new_scan())
        self.assertIs(table_read.return_value, builder.new_read())
        self.assertIs(build_explain_result.return_value, builder.explain())


if __name__ == "__main__":
    unittest.main()
