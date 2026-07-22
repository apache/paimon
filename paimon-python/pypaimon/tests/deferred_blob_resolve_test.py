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

import os
import shutil
import tempfile
import unittest

import pyarrow as pa

from pypaimon import CatalogFactory, Schema


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

    def _create_table(self, name, extra_options=None):
        options = dict(_TABLE_OPTIONS)
        options.update(extra_options or {})
        identifier = "default.%s" % name
        self.catalog.create_table(
            identifier,
            Schema.from_pyarrow_schema(self.schema, options=options),
            False,
        )
        table = self.catalog.get_table(identifier)
        write_builder = table.new_batch_write_builder()
        writer = write_builder.new_write()
        commit = write_builder.new_commit()
        writer.write_arrow(pa.table({
            "sample_id": ["sample_%d" % index for index in range(_ROW_COUNT)],
            "payload": [bytes([index]) * 1024 for index in range(_ROW_COUNT)],
            "score": list(range(_ROW_COUNT)),
        }, schema=self.schema))
        commit.commit(writer.prepare_commit())
        writer.close()
        commit.close()
        return self.catalog.get_table(identifier)

    def _read(self, table, predicate, limit=None, blob_parallelism=None):
        counting_file_io = _BlobCountingFileIO(table.file_io)
        table.file_io = counting_file_io
        read_builder = table.new_read_builder().with_filter(predicate)
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
