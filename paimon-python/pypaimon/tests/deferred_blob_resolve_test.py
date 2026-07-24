# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon import CatalogFactory, Schema
from pypaimon.read.query_auth_split import QueryAuthSplit


_ROW_COUNT = 10
_TABLE_OPTIONS = {
    "row-tracking.enabled": "true",
    "data-evolution.enabled": "true",
}


class _BlobCountingFileIO:

    def __init__(self, inner):
        self._inner = inner
        self.blobs_fetched = 0

    def read_blobs_concurrent(self, blobs, parallelism):
        self.blobs_fetched += sum(blob is not None for blob in blobs)
        return self._inner.read_blobs_concurrent(blobs, parallelism)

    def __getattr__(self, name):
        return getattr(self._inner, name)


class _RejectScoreOneAuthResult:
    column_masking = None
    filter = [json.dumps({
        "kind": "LEAF",
        "transform": {
            "name": "FIELD_REF",
            "fieldRef": {"index": 2, "name": "score", "type": "INT"},
        },
        "function": "NOT_EQUAL",
        "literals": [1],
    })]

    @staticmethod
    def get_extra_fields_for_filter(read_fields, table_fields):
        return []

    @staticmethod
    def extract_row_filter():
        return lambda batch: pc.not_equal(batch.column("score"), 1)


class _PayloadAuthResult:
    column_masking = None

    def __init__(self, expected_payload):
        self._expected_payload = expected_payload
        self.filter = [json.dumps({
            "kind": "LEAF",
            "transform": {
                "name": "FIELD_REF",
                "fieldRef": {
                    "index": 1,
                    "name": "payload",
                    "type": "BYTES",
                },
            },
            "function": "EQUAL",
            "literals": [],
        })]

    @staticmethod
    def get_extra_fields_for_filter(read_fields, table_fields):
        return []

    def extract_row_filter(self):
        return lambda batch: pc.equal(
            batch.column("payload"), self._expected_payload)


class DeferredBlobResolveTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.tempdir = tempfile.mkdtemp()
        cls.catalog = CatalogFactory.create({
            "warehouse": os.path.join(cls.tempdir, "warehouse")
        })
        cls.catalog.create_database("default", False)
        cls.schema = pa.schema([
            ("sample_id", pa.string()),
            ("payload", pa.large_binary()),
            ("score", pa.int32()),
        ])

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree(cls.tempdir, ignore_errors=True)

    def _create_table(self, name, extra_options=None, payloads=None,
                      partition_keys=None, sample_ids=None):
        options = dict(_TABLE_OPTIONS)
        options.update(extra_options or {})
        identifier = "default.%s" % name
        self.catalog.create_table(
            identifier,
            Schema.from_pyarrow_schema(
                self.schema,
                partition_keys=partition_keys,
                options=options,
            ),
            False,
        )
        table = self.catalog.get_table(identifier)
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        commit = write_builder.new_commit()
        if sample_ids is None:
            sample_ids = [
                "sample_%d" % index for index in range(_ROW_COUNT)
            ]
        writer.write_arrow(pa.table({
            "sample_id": sample_ids,
            "payload": (
                payloads if payloads is not None else
                [bytes([index]) * 1024 for index in range(_ROW_COUNT)]
            ),
            "score": list(range(_ROW_COUNT)),
        }, schema=self.schema))
        commit.commit(writer.prepare_commit())
        writer.close()
        commit.close()
        return self.catalog.get_table(identifier)

    def _read(self, table, predicate, limit=None, blob_parallelism=None):
        counting_file_io = _BlobCountingFileIO(table.file_io)
        table.file_io = counting_file_io
        read_builder = table.new_read_builder()
        if predicate is not None:
            read_builder = read_builder.with_filter(predicate)
        read_builder = read_builder.with_projection(
            ["sample_id", "payload", "score"])
        if limit is not None:
            read_builder = read_builder.with_limit(limit)
        splits = read_builder.new_scan().plan().splits()
        table_read = read_builder.new_read()
        if blob_parallelism is None:
            batch_reader = table_read.to_arrow_batch_reader(splits)
        else:
            batch_reader = table_read.to_arrow_batch_reader(
                splits, blob_parallelism=blob_parallelism)
        result = pa.Table.from_batches(batch_reader)
        return result, counting_file_io

    def test_fetches_payloads_only_for_filtered_rows(self):
        table = self._create_table("defer_filtered")
        predicate = table.new_read_builder().new_predicate_builder().less_than(
            "score", 5)

        result, counting_file_io = self._read(table, predicate)

        self.assertEqual(5, result.num_rows)
        self.assertEqual(5, counting_file_io.blobs_fetched)
        self.assertEqual(
            [bytes([index]) * 1024 for index in range(5)],
            result.column("payload").to_pylist(),
        )

    def test_applies_limit_before_fetching_payloads(self):
        table = self._create_table("defer_limit")
        predicate = table.new_read_builder().new_predicate_builder().less_than(
            "score", 8)

        result, counting_file_io = self._read(table, predicate, limit=2)

        self.assertEqual(2, result.num_rows)
        self.assertEqual(2, counting_file_io.blobs_fetched)

    def test_limit_without_predicate_defers_payloads(self):
        table = self._create_table("defer_limit_only")

        result, counting_file_io = self._read(table, None, limit=2)

        self.assertEqual(2, result.num_rows)
        self.assertEqual(2, counting_file_io.blobs_fetched)

    def test_limit_does_not_prefetch_payloads_across_splits(self):
        table = self._create_table(
            "defer_limit_splits",
            extra_options={"source.split.target-size": "1b"},
            partition_keys=["sample_id"],
        )
        counting_file_io = _BlobCountingFileIO(table.file_io)
        table.file_io = counting_file_io
        splits = table.new_read_builder().new_scan().plan().splits()
        read_builder = table.new_read_builder().with_limit(1)

        table_read = read_builder.new_read()
        with patch.object(
                table_read,
                "_to_arrow_parallel",
                side_effect=AssertionError("deferred LIMIT must run serially"),
        ) as parallel_read:
            result = table_read.to_arrow(splits, parallelism=4)

        self.assertGreater(len(splits), 1)
        parallel_read.assert_not_called()
        self.assertEqual(1, result.num_rows)
        self.assertEqual(1, counting_file_io.blobs_fetched)

    def test_limit_covering_all_rows_preserves_parallelism(self):
        table = self._create_table(
            "defer_limit_all_rows",
            extra_options={"source.split.target-size": "1b"},
            partition_keys=["sample_id"],
        )
        counting_file_io = _BlobCountingFileIO(table.file_io)
        table.file_io = counting_file_io
        read_builder = table.new_read_builder().with_limit(_ROW_COUNT)
        splits = read_builder.new_scan().plan().splits()
        table_read = read_builder.new_read()

        with patch.object(
                table_read,
                "_to_arrow_parallel",
                wraps=table_read._to_arrow_parallel,
        ) as parallel_read:
            result = table_read.to_arrow(splits, parallelism=4)

        self.assertGreater(len(splits), 1)
        parallel_read.assert_called_once()
        self.assertEqual(_ROW_COUNT, result.num_rows)
        self.assertEqual(_ROW_COUNT, counting_file_io.blobs_fetched)

    def test_iterator_passes_remaining_limit_across_splits(self):
        table = self._create_table(
            "defer_iterator_limit_splits",
            extra_options={"source.split.target-size": "1b"},
            partition_keys=["sample_id"],
            sample_ids=["a"] + ["b"] * (_ROW_COUNT - 1),
        )
        counting_file_io = _BlobCountingFileIO(table.file_io)
        table.file_io = counting_file_io
        read_builder = table.new_read_builder().with_projection(
            ["sample_id", "payload", "score"]
        ).with_limit(2)
        splits = read_builder.new_scan().plan().splits()

        rows = list(read_builder.new_read().to_iterator(splits))

        self.assertEqual(2, len(splits))
        self.assertEqual(2, len(rows))
        self.assertEqual(2, counting_file_io.blobs_fetched)

    def test_iterator_applies_limit_after_auth_filter(self):
        table = self._create_table(
            "defer_iterator_auth_limit_splits",
            extra_options={"source.split.target-size": "1b"},
            partition_keys=["sample_id"],
            sample_ids=["a"] + ["b"] * (_ROW_COUNT - 1),
        )
        read_builder = table.new_read_builder().with_projection(
            ["sample_id", "payload", "score"]
        ).with_limit(2)
        counting_file_io = _BlobCountingFileIO(table.file_io)
        table.file_io = counting_file_io
        auth_result = _RejectScoreOneAuthResult()
        splits = [
            QueryAuthSplit(split, auth_result)
            for split in read_builder.new_scan().plan().splits()
        ]

        scores = [
            row.get_field(2)
            for row in read_builder.new_read().to_iterator(splits)
        ]

        self.assertEqual([0, 2], scores)
        self.assertEqual(2, counting_file_io.blobs_fetched)

    def test_auth_blob_filter_keeps_eager_resolution(self):
        table = self._create_table("defer_auth_blob_filter")
        expected_payload = bytes([3]) * 1024
        counting_file_io = _BlobCountingFileIO(table.file_io)
        table.file_io = counting_file_io
        read_builder = table.new_read_builder().with_projection(
            ["sample_id", "payload", "score"]
        ).with_limit(1)
        auth_result = _PayloadAuthResult(expected_payload)
        splits = [
            QueryAuthSplit(split, auth_result)
            for split in read_builder.new_scan().plan().splits()
        ]

        result = pa.Table.from_batches(
            read_builder.new_read().to_arrow_batch_reader(
                splits, blob_parallelism=4)
        )

        self.assertEqual([expected_payload], result.column("payload").to_pylist())
        self.assertEqual(_ROW_COUNT, counting_file_io.blobs_fetched)

    def test_auth_only_defers_non_auth_payloads(self):
        # An auth filter with no predicate/limit still defers scalar BLOBs, so payloads of
        # the rows the auth filter drops are not read.
        table = self._create_table("defer_auth_only")
        counting_file_io = _BlobCountingFileIO(table.file_io)
        table.file_io = counting_file_io
        read_builder = table.new_read_builder().with_projection(
            ["sample_id", "payload", "score"])
        splits = [
            QueryAuthSplit(split, _RejectScoreOneAuthResult())
            for split in read_builder.new_scan().plan().splits()
        ]

        result = pa.Table.from_batches(
            read_builder.new_read().to_arrow_batch_reader(
                splits, blob_parallelism=4))

        self.assertEqual(_ROW_COUNT - 1, result.num_rows)
        self.assertEqual(_ROW_COUNT - 1, counting_file_io.blobs_fetched)

    def test_preserves_null_payloads_after_filtering(self):
        payloads = [
            None if index == 1 else bytes([index]) * 1024
            for index in range(_ROW_COUNT)
        ]
        table = self._create_table("defer_null", payloads=payloads)
        predicate = table.new_read_builder().new_predicate_builder().less_than(
            "score", 4)

        result, counting_file_io = self._read(table, predicate)

        self.assertEqual(4, result.num_rows)
        self.assertEqual(3, counting_file_io.blobs_fetched)
        self.assertEqual(payloads[:4], result.column("payload").to_pylist())

    def test_can_disable_deferred_resolution(self):
        table = self._create_table(
            "defer_disabled",
            {"read.defer-blob-resolve": "false"},
        )
        predicate = table.new_read_builder().new_predicate_builder().less_than(
            "score", 5)

        result, counting_file_io = self._read(
            table, predicate, blob_parallelism=4)

        self.assertEqual(5, result.num_rows)
        self.assertEqual(_ROW_COUNT, counting_file_io.blobs_fetched)

    def test_blob_predicate_keeps_eager_resolution(self):
        table = self._create_table("defer_blob_predicate")
        expected_payload = bytes([3]) * 1024
        predicate = table.new_read_builder().new_predicate_builder().equal(
            "payload", expected_payload)

        result, counting_file_io = self._read(
            table, predicate, blob_parallelism=4)

        self.assertEqual(1, result.num_rows)
        self.assertEqual([expected_payload], result.column("payload").to_pylist())
        self.assertEqual(_ROW_COUNT, counting_file_io.blobs_fetched)

    def test_defers_payloads_for_blob_fallback_reader(self):
        table = self._create_table("defer_fallback")
        update_builder = table.new_batch_write_builder()
        table_update = update_builder.new_update().with_update_type(["payload"])
        updated_payload = b"updated-payload"
        update_messages = table_update.update_by_arrow_with_row_id(pa.table({
            "_ROW_ID": pa.array([3], type=pa.int64()),
            "payload": pa.array([updated_payload], type=pa.large_binary()),
        }))
        update_builder.new_commit().commit(update_messages)

        predicate = table.new_read_builder().new_predicate_builder().less_than(
            "score", 5)
        result, counting_file_io = self._read(table, predicate)

        self.assertEqual(5, result.num_rows)
        self.assertEqual(5, counting_file_io.blobs_fetched)
        payload_by_score = dict(zip(
            result.column("score").to_pylist(),
            result.column("payload").to_pylist(),
        ))
        self.assertEqual(
            updated_payload,
            payload_by_score[3],
        )


if __name__ == "__main__":
    unittest.main()
