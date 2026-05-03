################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

import unittest
from datetime import date, datetime, time as dt_time
from decimal import Decimal

from pypaimon.data.timestamp import Timestamp
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.schema.data_types import AtomicType, DataField
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.internal_row import RowKind
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.commit_message_serializer import CommitMessageSerializer


def _key_field(idx: int, name: str, type_str: str) -> DataField:
    return DataField(idx, name, AtomicType(type_str))


def _build_data_file_meta(file_name: str = "data-1.parquet") -> DataFileMeta:
    pk_fields = [_key_field(0, "id", "BIGINT"), _key_field(1, "name", "STRING")]
    min_key = GenericRow([1, "alice"], pk_fields)
    max_key = GenericRow([99, "zoe"], pk_fields)
    key_stats = SimpleStats(
        min_values=GenericRow([1, "alice"], pk_fields),
        max_values=GenericRow([99, "zoe"], pk_fields),
        null_counts=[0, 0],
    )
    value_stats = SimpleStats(
        min_values=GenericRow([], []),
        max_values=GenericRow([], []),
        null_counts=[],
    )
    return DataFileMeta.create(
        file_name=file_name,
        file_size=4096,
        row_count=99,
        min_key=min_key,
        max_key=max_key,
        key_stats=key_stats,
        value_stats=value_stats,
        min_sequence_number=10,
        max_sequence_number=200,
        schema_id=0,
        level=0,
        extra_files=["index-1.idx"],
        creation_time=Timestamp.from_epoch_millis(1_700_000_000_000, 123_456),
        delete_row_count=2,
        embedded_index=b"\x00\x01\x02\x03embedded",
        file_source=1,
        value_stats_cols=["c1"],
        external_path="oss://bucket/path/to/file",
        first_row_id=1000,
        write_cols=["id", "name"],
        file_path="/abs/path/data-1.parquet",
    )


class DataFileMetaSerdeTest(unittest.TestCase):

    def test_to_from_dict_roundtrip(self):
        original = _build_data_file_meta()
        rebuilt = DataFileMeta.from_dict(original.to_dict())

        self.assertEqual(original, rebuilt)
        # spot check of complex sub-fields that use tagged encoding
        self.assertEqual(original.embedded_index, rebuilt.embedded_index)
        self.assertEqual(original.creation_time, rebuilt.creation_time)
        self.assertEqual(original.min_key.values, rebuilt.min_key.values)
        self.assertEqual(original.min_key.row_kind, rebuilt.min_key.row_kind)
        self.assertEqual(
            [f.to_dict() for f in original.min_key.fields],
            [f.to_dict() for f in rebuilt.min_key.fields],
        )

    def test_value_encoding_supports_decimal_and_temporal_types(self):
        fields = [
            _key_field(0, "amount", "DECIMAL(10, 2)"),
            _key_field(1, "ts", "TIMESTAMP(6)"),
            _key_field(2, "d", "DATE"),
            _key_field(3, "t", "TIME"),
            _key_field(4, "blob", "BYTES"),
        ]
        row = GenericRow(
            values=[
                Decimal("12.34"),
                datetime(2024, 1, 2, 3, 4, 5, 678901),
                date(2024, 1, 2),
                dt_time(13, 45, 30, 250000),
                b"binary-payload",
            ],
            fields=fields,
            row_kind=RowKind.UPDATE_AFTER,
        )
        # Reuse the GenericRow encode path through SimpleStats
        stats = SimpleStats(min_values=row, max_values=row, null_counts=[0, 0, 0, 0, 0])
        meta = _build_data_file_meta()
        meta.key_stats = stats

        rebuilt = DataFileMeta.from_dict(meta.to_dict())

        self.assertEqual(rebuilt.key_stats.min_values.values[0], Decimal("12.34"))
        self.assertEqual(rebuilt.key_stats.min_values.values[1], datetime(2024, 1, 2, 3, 4, 5, 678901))
        self.assertEqual(rebuilt.key_stats.min_values.values[2], date(2024, 1, 2))
        self.assertEqual(rebuilt.key_stats.min_values.values[3], dt_time(13, 45, 30, 250000))
        self.assertEqual(rebuilt.key_stats.min_values.values[4], b"binary-payload")
        self.assertEqual(rebuilt.key_stats.min_values.row_kind, RowKind.UPDATE_AFTER)


class CommitMessageSerializerTest(unittest.TestCase):

    def test_serialize_deserialize_roundtrip_for_compact_message(self):
        before_files = [_build_data_file_meta(f"old-{i}.parquet") for i in range(3)]
        after_files = [_build_data_file_meta("new-merged.parquet")]
        message = CommitMessage(
            partition=("2024-01-01", "us"),
            bucket=2,
            new_files=[],
            compact_before=before_files,
            compact_after=after_files,
            check_from_snapshot=42,
        )

        payload = CommitMessageSerializer.serialize(message)
        rebuilt = CommitMessageSerializer.deserialize(payload)

        self.assertIsInstance(payload, bytes)
        self.assertEqual(message.partition, rebuilt.partition)
        self.assertEqual(message.bucket, rebuilt.bucket)
        self.assertEqual(message.new_files, rebuilt.new_files)
        self.assertEqual(message.compact_before, rebuilt.compact_before)
        self.assertEqual(message.compact_after, rebuilt.compact_after)
        self.assertEqual(message.check_from_snapshot, rebuilt.check_from_snapshot)

    def test_serialize_deserialize_roundtrip_for_append_message(self):
        message = CommitMessage(
            partition=(),
            bucket=0,
            new_files=[_build_data_file_meta("append-1.parquet")],
        )

        rebuilt = CommitMessageSerializer.deserialize(CommitMessageSerializer.serialize(message))

        self.assertEqual(message.partition, rebuilt.partition)
        self.assertEqual(message.bucket, rebuilt.bucket)
        self.assertEqual(message.new_files, rebuilt.new_files)
        self.assertEqual([], rebuilt.compact_before)
        self.assertEqual([], rebuilt.compact_after)

    def test_unsupported_version_is_rejected(self):
        message = CommitMessage(partition=(), bucket=0, new_files=[_build_data_file_meta()])
        payload_dict = CommitMessageSerializer.to_dict(message)
        payload_dict["version"] = CommitMessageSerializer.VERSION + 1

        with self.assertRaises(ValueError):
            CommitMessageSerializer.from_dict(payload_dict)

    def test_serialize_list_round_trip(self):
        messages = [
            CommitMessage(partition=(f"p{i}",), bucket=i, new_files=[_build_data_file_meta(f"f{i}.parquet")])
            for i in range(3)
        ]
        payloads = CommitMessageSerializer.serialize_list(messages)
        rebuilt = CommitMessageSerializer.deserialize_list(payloads)

        self.assertEqual(len(messages), len(rebuilt))
        for original, copy in zip(messages, rebuilt):
            self.assertEqual(original.partition, copy.partition)
            self.assertEqual(original.bucket, copy.bucket)
            self.assertEqual(original.new_files, copy.new_files)


if __name__ == "__main__":
    unittest.main()
