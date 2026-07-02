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
import json
import os
import shutil
import tempfile
import unittest

import pyarrow as pa
import pypaimon.multimodal as pmm
from pypaimon.multimodal import source_col
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon import Schema as PaimonSchema
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.table.row.blob import BlobData
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.utils.range import Range


_PARQUET_OPTIONS = {
    "row-tracking.enabled": "true",
    "data-evolution.enabled": "true",
    "deletion-vectors.enabled": "true",
    "file.format": "parquet",
    "vector.file.format": "parquet",
}


def _schema(fields):
    return pa.schema([
        pa.field(name, field_type)
        for name, field_type in fields.items()
    ])


def _vector(dim):
    return pa.list_(pa.float32(), dim)


def _raw_schema(options=None, primary_keys=None):
    return PaimonSchema.from_pyarrow_schema(
        _schema({"id": pa.int32(), "name": pa.string()}),
        primary_keys=list(primary_keys or []),
        options=dict(options or {"file.format": "parquet"}),
    )


class MultimodalTableTest(unittest.TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp(prefix="pypaimon_mm_")
        self.warehouse = os.path.join(self.temp_dir, "warehouse")
        self.conn = pmm.connect(options={"warehouse": self.warehouse})

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_connect_accepts_options(self):
        conn = pmm.connect(
            database="analytics",
            options={
                "warehouse": os.path.join(self.temp_dir, "warehouse_options"),
            },
        )

        table = conn.create_table(
            "docs",
            schema=_schema({"id": pa.int32()}),
            options=_PARQUET_OPTIONS,
        )

        self.assertEqual("analytics.docs", table.identifier)

    def test_connect_rejects_positional_warehouse(self):
        with self.assertRaises(TypeError):
            pmm.connect(self.warehouse)

    def test_create_table_defaults_data_evolution_options(self):
        table = self.conn.create_table(
            "docs",
            schema=_schema({
                "id": pa.int32(),
                "content": pa.string(),
                "embedding": _vector(3),
                "payload": pa.large_binary(),
            }),
        )

        options = table.raw_table.table_schema.options
        self.assertEqual("true", options["row-tracking.enabled"])
        self.assertEqual("true", options["data-evolution.enabled"])
        self.assertEqual("true", options["deletion-vectors.enabled"])
        self.assertEqual("true", options["blob-as-descriptor"])
        self.assertNotIn("data-evolution.row-sidecar.enabled", options)
        self.assertEqual("vortex", options["file.format"])
        self.assertEqual("full", options["global-index.search-mode"])
        self.assertEqual("vortex", options["vector.file.format"])
        self.assertEqual("default.docs", self.conn.get_table("docs").identifier)
        self.assertEqual(["id", "content", "embedding", "payload"],
                         [field.name for field in table.raw_table.fields])

    def test_create_table_uses_options_and_partitioned(self):
        table = self.conn.create_table(
            "docs",
            schema=_schema({
                "id": pa.int32(),
                "embedding": _vector(3),
                "dt": pa.string(),
            }),
            options=dict(_PARQUET_OPTIONS, **{
                "deletion-vectors.enabled": "false",
            }),
            partitioned=["dt"],
        )

        options = table.raw_table.table_schema.options
        self.assertEqual(["dt"], table.raw_table.table_schema.partition_keys)
        self.assertEqual("false", options["deletion-vectors.enabled"])
        self.assertEqual("parquet", options["file.format"])
        self.assertEqual("parquet", options["vector.file.format"])

    def test_create_table_rejects_non_arrow_schema(self):
        with self.assertRaisesRegex(ValueError, "pyarrow.Schema"):
            self.conn.create_table("docs", schema={"id": pa.int32()})

    def test_create_table_accepts_pyarrow_schema_types(self):
        table = self.conn.create_table(
            "typed",
            schema=pa.schema([
                pa.field("flag", pa.bool_(), nullable=False),
                pa.field("tiny", pa.int8()),
                pa.field("small", pa.int16()),
                pa.field("id", pa.int64()),
                pa.field("score", pa.float32()),
                pa.field("ratio", pa.float64()),
                pa.field("title", pa.string()),
                pa.field("payload", pa.binary()),
                pa.field("hash", pa.binary(16)),
                pa.field("blob", pa.large_binary()),
                pa.field("amount", pa.decimal128(12, 2)),
                pa.field("dt", pa.date32()),
                pa.field("created_at", pa.timestamp("us")),
                pa.field("created_ltz", pa.timestamp("us", tz="UTC")),
                pa.field("tags", pa.list_(pa.string())),
                pa.field("attrs", pa.map_(pa.string(), pa.int32())),
                pa.field("meta", pa.struct([pa.field("rank", pa.int32())])),
                pa.field("embedding", _vector(3)),
            ]),
        )

        types_by_name = {
            field.name: str(field.type) for field in table.raw_table.fields
        }
        self.assertEqual("BOOLEAN NOT NULL", types_by_name["flag"])
        self.assertEqual("TINYINT", types_by_name["tiny"])
        self.assertEqual("SMALLINT", types_by_name["small"])
        self.assertEqual("BIGINT", types_by_name["id"])
        self.assertEqual("FLOAT", types_by_name["score"])
        self.assertEqual("DOUBLE", types_by_name["ratio"])
        self.assertEqual("STRING", types_by_name["title"])
        self.assertEqual("BYTES", types_by_name["payload"])
        self.assertEqual("BINARY(16)", types_by_name["hash"])
        self.assertEqual("BLOB", types_by_name["blob"])
        self.assertEqual("DECIMAL(12, 2)", types_by_name["amount"])
        self.assertEqual("DATE", types_by_name["dt"])
        self.assertEqual("TIMESTAMP(6)", types_by_name["created_at"])
        self.assertEqual("TIMESTAMP_LTZ(6)", types_by_name["created_ltz"])
        self.assertEqual("ARRAY<STRING>", types_by_name["tags"])
        self.assertEqual("MAP<STRING, INT>", types_by_name["attrs"])
        self.assertEqual("ROW<rank: INT>", types_by_name["meta"])
        self.assertEqual("VECTOR<FLOAT, 3>", types_by_name["embedding"])

    def test_blob_store_put_objects_get_list_and_delete(self):
        table = self.conn.create_table(
            "objects",
            schema=_schema({
                "key": pa.string(),
                "image": pa.large_binary(),
                "content_type": pa.string(),
                "owner": pa.string(),
            }),
            options=_PARQUET_OPTIONS,
        )
        store = table.blobs(column="image")

        results = store.put_objects([
            {
                "key": "images/cat.jpg",
                "body": b"cat-image-v1",
                "columns": {"content_type": "image/jpeg", "owner": "alice"},
            },
            {
                "key": "images/dog.jpg",
                "body": bytearray(b"dog-image"),
                "columns": {"content_type": "image/jpeg", "owner": "bob"},
            },
        ])

        self.assertEqual(["images/cat.jpg", "images/dog.jpg"],
                         [result.key for result in results])
        self.assertEqual([12, 9], [result.size for result in results])
        self.assertTrue(all(result.descriptor is not None for result in results))

        cat = store.get_object("images/cat.jpg")
        self.assertEqual(b"cat-image-v1", cat.read())
        self.assertEqual(b"at-i", store.get_object(
            "images/cat.jpg", range="bytes=1-4").read())
        clipped = store.get_object("images/cat.jpg", range="bytes=10-999")
        self.assertEqual(2, clipped.content_length)
        self.assertEqual(b"v1", clipped.read())
        with self.assertRaisesRegex(ValueError, "Range start"):
            store.get_object("images/cat.jpg", range="bytes=12-13")
        self.assertEqual("image/jpeg", cat.columns["content_type"])
        self.assertEqual("alice", cat.columns["owner"])
        owner_only = store.get_object(
            "images/cat.jpg",
            columns=["owner"],
        )
        self.assertEqual({"owner": "alice"}, owner_only.columns)

        listed = store.list_objects(prefix="images/")
        self.assertEqual(["images/cat.jpg", "images/dog.jpg"],
                         sorted(obj.key for obj in listed))
        self.assertEqual(
            {
                "images/cat.jpg": "image/jpeg",
                "images/dog.jpg": "image/jpeg",
            },
            {obj.key: obj.columns["content_type"] for obj in listed},
        )
        listed_without_columns = store.list_objects(
            prefix="images/",
            columns=[],
        )
        self.assertEqual(
            {"images/cat.jpg": {}, "images/dog.jpg": {}},
            {obj.key: obj.columns for obj in listed_without_columns},
        )
        self.assertEqual([], store.list_objects(prefix="images/", limit=0))
        with self.assertRaisesRegex(ValueError, "limit"):
            store.list_objects(prefix="images/", limit=-1)

        store.put_object(
            "images/cat.jpg",
            b"cat-image-v2",
            columns={"content_type": "image/png", "owner": "alice"},
        )
        self.assertEqual(b"cat-image-v2", store.get_object("images/cat.jpg").read())
        info = store.head_object("images/cat.jpg")
        self.assertEqual("image/png", info.columns["content_type"])
        self.assertEqual(
            {"content_type": "image/png"},
            store.head_object(
                "images/cat.jpg",
                columns="content_type",
            ).columns,
        )
        self.assertEqual(2, table.scan().to_arrow().num_rows)

        previous_descriptor = info.descriptor
        updated = store.update_object_columns(
            "images/cat.jpg",
            {"content_type": "image/webp"},
        )
        self.assertEqual(previous_descriptor, updated.descriptor)
        self.assertEqual("image/webp", updated.columns["content_type"])
        self.assertEqual("alice", updated.columns["owner"])
        self.assertEqual(b"cat-image-v2", store.get_object("images/cat.jpg").read())

        batch_updates = store.update_objects_columns([
            {"key": "images/cat.jpg", "columns": {"owner": "carol"}},
            {"key": "images/dog.jpg", "columns": {"owner": "dave"}},
        ])
        self.assertEqual(
            ["images/cat.jpg", "images/dog.jpg"],
            [obj.key for obj in batch_updates],
        )
        self.assertEqual("carol", store.head_object("images/cat.jpg").columns["owner"])
        self.assertEqual("dave", store.head_object("images/dog.jpg").columns["owner"])
        self.assertEqual(2, table.scan().to_arrow().num_rows)

        with self.assertRaisesRegex(ValueError, "columns must not be empty"):
            store.update_object_columns("images/cat.jpg", {})
        with self.assertRaises(pmm.NoSuchKey):
            store.update_object_columns("images/missing.jpg", {"owner": "nobody"})
        with self.assertRaisesRegex(ValueError, "columns must not include"):
            store.update_object_columns("images/cat.jpg", {"image": b"new"})

        store.delete_object("images/dog.jpg")
        with self.assertRaises(pmm.NoSuchKey):
            store.head_object("images/dog.jpg")
        self.assertEqual(["images/cat.jpg"],
                         [obj.key for obj in store.list_objects(prefix="images/")])

    def test_blob_store_put_object_accepts_blob_without_materializing(self):
        from pypaimon.table.row.blob import Blob, BlobDescriptor

        table = self.conn.create_table(
            "streamed_objects",
            schema=_schema({
                "key": pa.string(),
                "payload": pa.large_binary(),
            }),
            options=_PARQUET_OPTIONS,
        )
        data = b"streamed-managed-payload"
        stream_uri = "stream://payloads/1"
        stream_calls = []

        class StreamOnlyReader:

            def new_input_stream(self, uri):
                stream_calls.append(("new_input_stream", uri))
                return io.BytesIO(data)

        class StreamOnlyReaderFactory:

            def create(self, uri):
                stream_calls.append(("create", uri))
                return StreamOnlyReader()

        class DescriptorOnlyBlob(Blob):

            def to_data(self):
                raise AssertionError("put_object should not materialize Blob data")

            def to_descriptor(self):
                return BlobDescriptor(stream_uri, 0, len(data))

            def new_input_stream(self):
                raise AssertionError("put_object should use the URI stream")

        store = table.blobs(column="payload")
        original_factory = table.raw_table.file_io.uri_reader_factory
        table.raw_table.file_io.uri_reader_factory = StreamOnlyReaderFactory()
        try:
            result = store.put_object("payloads/1", DescriptorOnlyBlob())
        finally:
            table.raw_table.file_io.uri_reader_factory = original_factory

        self.assertEqual(len(data), result.size)
        self.assertEqual([
            ("create", stream_uri),
            ("new_input_stream", stream_uri),
        ], stream_calls)
        self.assertNotEqual(stream_uri, result.descriptor.uri)
        self.assertEqual(data, store.get_object("payloads/1").read())

    def test_blob_store_put_object_recovers_absent_key_race(self):
        table = self.conn.create_table(
            "raced_objects",
            schema=_schema({
                "key": pa.string(),
                "payload": pa.large_binary(),
            }),
            options=_PARQUET_OPTIONS,
        )
        store = table.blobs(column="payload")

        stale_row = GenericRow(
            ["payloads/1", BlobData(b"stale")],
            table.raw_table.fields,
        )
        write_builder = table.raw_table.new_batch_write_builder()
        stale_write = write_builder.new_write()
        stale_update = write_builder.new_update()
        stale_commit = write_builder.new_commit()
        stale_messages = stale_update.delete_by_predicate(
            store._keys_predicate(["payloads/1"]))
        stale_write.write_row(stale_row)
        stale_messages.extend(stale_write.prepare_commit())

        original_commit = store._commit_upsert_once
        injected = {"done": False}

        def commit_with_race(rows):
            snapshot_id = original_commit(rows)
            if not injected["done"]:
                injected["done"] = True
                stale_commit.commit(stale_messages)
            return snapshot_id

        store._commit_upsert_once = commit_with_race
        try:
            result = store.put_object("payloads/1", b"winner")
        finally:
            store._commit_upsert_once = original_commit
            stale_write.close()
            stale_commit.close()

        self.assertEqual(len(b"winner"), result.size)
        self.assertEqual(b"winner", store.get_object("payloads/1").read())
        self.assertEqual(1, table.scan().to_arrow().num_rows)

    def test_blob_store_put_object_recovers_result_read_race(self):
        table = self.conn.create_table(
            "result_raced_objects",
            schema=_schema({
                "key": pa.string(),
                "payload": pa.large_binary(),
            }),
            options=_PARQUET_OPTIONS,
        )
        store = table.blobs(column="payload")

        stale_row = GenericRow(
            ["payloads/1", BlobData(b"stale")],
            table.raw_table.fields,
        )
        write_builder = table.raw_table.new_batch_write_builder()
        stale_write = write_builder.new_write()
        stale_update = write_builder.new_update()
        stale_commit = write_builder.new_commit()
        stale_messages = stale_update.delete_by_predicate(
            store._keys_predicate(["payloads/1"]))
        stale_write.write_row(stale_row)
        stale_messages.extend(stale_write.prepare_commit())

        original_read_results = store._read_put_results
        injected = {"done": False}

        def read_results_with_race(keys, snapshot_id):
            if not injected["done"]:
                injected["done"] = True
                stale_commit.commit(stale_messages)
            return original_read_results(keys, snapshot_id)

        store._read_put_results = read_results_with_race
        try:
            result = store.put_object("payloads/1", b"winner")
        finally:
            store._read_put_results = original_read_results
            stale_write.close()
            stale_commit.close()

        self.assertTrue(injected["done"])
        self.assertEqual(len(b"winner"), result.size)
        self.assertEqual(b"winner", store.get_object("payloads/1").read())
        self.assertEqual(1, table.scan().to_arrow().num_rows)

    def test_blob_store_put_object_reference_preserves_descriptor_uri(self):
        table = self.conn.create_table(
            "referenced_objects",
            schema=_schema({
                "object_key": pa.string(),
                "payload": pa.large_binary(),
                "media_type": pa.string(),
            }),
            options=dict(_PARQUET_OPTIONS, **{
                "blob-descriptor-field": "payload",
            }),
        )
        data = b"external-video-payload"
        external_path = os.path.join(self.temp_dir, "video.bin")
        with open(external_path, "wb") as f:
            f.write(data)

        store = table.blobs(column="payload")
        result = store.put_object(
            "videos/1",
            uri=external_path,
            length=len(data),
            columns={"media_type": "video/mp4"},
        )
        more = store.put_objects([
            {
                "key": "videos/2",
                "descriptor": result.descriptor,
                "columns": {"media_type": "video/mp4"},
            }
        ])

        self.assertEqual("videos/1", result.key)
        self.assertEqual(len(data), result.size)
        self.assertEqual(external_path, result.descriptor.uri)
        self.assertEqual("videos/2", more[0].key)
        self.assertEqual(external_path, more[0].descriptor.uri)
        obj = store.get_object("videos/1")
        self.assertEqual(data, obj.read())
        self.assertEqual("video/mp4", obj.columns["media_type"])
        self.assertEqual(external_path, store.head_object("videos/1").descriptor.uri)
        self.assertEqual(data, store.get_object("videos/2").read())

    def test_blob_store_put_object_uri_streams_into_managed_blob(self):
        table = self.conn.create_table(
            "managed_only_objects",
            schema=_schema({
                "key": pa.string(),
                "payload": pa.large_binary(),
            }),
            options=_PARQUET_OPTIONS,
        )
        data = b"external-managed-payload"
        external_path = os.path.join(self.temp_dir, "managed.bin")
        with open(external_path, "wb") as f:
            f.write(data)

        store = table.blobs(column="payload")
        result = store.put_object(
            "payloads/1",
            uri=external_path,
            length=len(data),
        )

        self.assertEqual(len(data), result.size)
        self.assertNotEqual(external_path, result.descriptor.uri)
        self.assertEqual(data, store.get_object("payloads/1").read())

    def test_drop_table_can_ignore_missing_table(self):
        self.conn.drop_table("missing", ignore_if_not_exists=True)

    def test_get_table_rejects_non_data_evolution_table(self):
        self.conn.catalog.create_database("default", ignore_if_exists=True)
        self.conn.catalog.create_table(
            "default.raw",
            _raw_schema(),
            False,
        )

        with self.assertRaisesRegex(ValueError, "data-evolution.enabled"):
            self.conn.get_table("raw")

    def test_get_table_rejects_primary_key_table(self):
        self.conn.catalog.create_database("default", ignore_if_exists=True)
        self.conn.catalog.create_table(
            "default.pk",
            _raw_schema(
                options=dict(_PARQUET_OPTIONS, **{"bucket": "1"}),
                primary_keys=["id"],
            ),
            False,
        )

        with self.assertRaisesRegex(ValueError, "primary keys"):
            self.conn.get_table("pk")

    def test_create_table_can_add_initial_data_and_get_by_short_name(self):
        self.conn.create_table(
            "users",
            data=[
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": "Bob", "age": 25},
            ],
            schema=_schema({
                "id": pa.int32(),
                "name": pa.string(),
                "age": pa.int32(),
            }),
            options=_PARQUET_OPTIONS,
        )

        users = self.conn.get_table("users")
        result = users.scan().select(["id", "name"]).to_arrow()

        self.assertEqual(["id", "name"], result.column_names)
        self.assertEqual([1, 2], result["id"].to_pylist())

    def test_add_scan_where_select_limit(self):
        users = self.conn.create_table(
            "users",
            schema=_schema({
                "id": pa.int32(),
                "name": pa.string(),
                "age": pa.int32(),
            }),
            options=_PARQUET_OPTIONS,
        )

        users.add([
            {"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25},
            {"id": 3, "name": "Carol", "age": 40},
        ])

        result = (
            users.scan()
            .where("age >= 30")
            .select(["id", "name"])
            .limit(1)
            .to_arrow()
        )

        self.assertEqual(["id", "name"], result.column_names)
        self.assertEqual(1, result.num_rows)
        self.assertEqual([1], result["id"].to_pylist())

    def test_scan_does_not_expose_pre_filter(self):
        users = self.conn.create_table(
            "users",
            schema=_schema({"id": pa.int32()}),
            options=_PARQUET_OPTIONS,
        )

        self.assertFalse(hasattr(users.scan(), "pre_filter"))

    def test_where_rejects_predicate_object(self):
        users = self.conn.create_table(
            "users",
            schema=_schema({"id": pa.int32()}),
            options=_PARQUET_OPTIONS,
        )
        predicate = PredicateBuilder(users.raw_table.fields).equal("id", 1)

        with self.assertRaisesRegex(ValueError, "SQL-like string"):
            users.scan().where(predicate)

    def test_update_by_filter(self):
        users = self.conn.create_table(
            "users",
            data=[
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": "Bob", "age": 25},
            ],
            schema=_schema({
                "id": pa.int32(),
                "name": pa.string(),
                "age": pa.int32(),
            }),
            options=_PARQUET_OPTIONS,
        )

        users.update(where="id = 2", values={"age": 26})

        rows = sorted(users.scan().to_list(), key=lambda r: r["id"])
        self.assertEqual(
            [
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": "Bob", "age": 26},
            ],
            rows,
        )

    def test_delete_by_filter(self):
        users = self.conn.create_table(
            "users",
            data=[
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": "Bob", "age": 25},
                {"id": 3, "name": "Carol", "age": 40},
            ],
            schema=_schema({
                "id": pa.int32(),
                "name": pa.string(),
                "age": pa.int32(),
            }),
            options=_PARQUET_OPTIONS,
        )

        users.delete(where="id = 2")

        rows = sorted(users.scan().to_list(), key=lambda r: r["id"])
        self.assertEqual(
            [
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 3, "name": "Carol", "age": 40},
            ],
            rows,
        )

    def test_merge_updates_matches_and_inserts_new_rows(self):
        users = self.conn.create_table(
            "users",
            data=[
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": "Bob", "age": 25},
            ],
            schema=_schema({
                "id": pa.int32(),
                "name": pa.string(),
                "age": pa.int32(),
            }),
            options=_PARQUET_OPTIONS,
        )

        users.merge("id") \
            .when_matched_update() \
            .when_not_matched_insert() \
            .execute([
                {"id": 2, "name": "Bob_v2", "age": 26},
                {"id": 3, "name": "Carol", "age": 40},
            ])

        rows = sorted(users.scan().to_list(), key=lambda r: r["id"])
        self.assertEqual(
            [
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": "Bob_v2", "age": 26},
                {"id": 3, "name": "Carol", "age": 40},
            ],
            rows,
        )

    def test_merge_deletes_matched_rows(self):
        users = self.conn.create_table(
            "users",
            data=[
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": "Bob", "age": 25},
                {"id": 3, "name": "Carol", "age": 40},
            ],
            schema=_schema({
                "id": pa.int32(),
                "name": pa.string(),
                "age": pa.int32(),
            }),
            options=_PARQUET_OPTIONS,
        )

        users.merge("id") \
            .when_matched_delete() \
            .execute([
                {"id": 2},
                {"id": 3},
            ])

        rows = sorted(users.scan().to_list(), key=lambda r: r["id"])
        self.assertEqual(
            [
                {"id": 1, "name": "Alice", "age": 30},
            ],
            rows,
        )

    def test_merge_all_uses_only_source_columns(self):
        users = self.conn.create_table(
            "users",
            data=[
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": "Bob", "age": 25},
            ],
            schema=_schema({
                "id": pa.int32(),
                "name": pa.string(),
                "age": pa.int32(),
            }),
            options=_PARQUET_OPTIONS,
        )

        users.merge("id") \
            .when_matched_update() \
            .when_not_matched_insert() \
            .execute([
                {"id": 2, "age": 26},
                {"id": 3, "age": 40},
            ])

        rows = sorted(users.scan().to_list(), key=lambda r: r["id"])
        self.assertEqual(
            [
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": "Bob", "age": 26},
                {"id": 3, "name": None, "age": 40},
            ],
            rows,
        )

    def test_merge_supports_source_key_mapping(self):
        users = self.conn.create_table(
            "users",
            data=[
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": "Bob", "age": 25},
            ],
            schema=_schema({
                "id": pa.int32(),
                "name": pa.string(),
                "age": pa.int32(),
            }),
            options=_PARQUET_OPTIONS,
        )

        users.merge({"id": "source_id"}) \
            .when_matched_update({"age": source_col("age")}) \
            .when_not_matched_insert() \
            .execute([
                {"source_id": 2, "name": "Bob_v2", "age": 26},
                {"source_id": 3, "name": "Carol", "age": 40},
            ])

        rows = sorted(users.scan().to_list(), key=lambda r: r["id"])
        self.assertEqual(
            [
                {"id": 1, "name": "Alice", "age": 30},
                {"id": 2, "name": "Bob", "age": 26},
                {"id": 3, "name": "Carol", "age": 40},
            ],
            rows,
        )

    def test_merge_where_uses_source_and_target_aliases(self):
        users = self.conn.create_table(
            "users",
            schema=_schema({
                "id": pa.int32(),
                "name": pa.string(),
                "age": pa.int32(),
            }),
            options=_PARQUET_OPTIONS,
        )

        calls = {}

        class FakeUpdate:
            def merge_into(
                    self,
                    source,
                    on,
                    when_matched=None,
                    when_not_matched=None):
                calls["on"] = on
                calls["matched"] = list(when_matched or [])
                calls["not_matched"] = list(when_not_matched or [])
                return []

        class FakeCommit:
            def commit(self, messages):
                calls["messages"] = messages

            def close(self):
                pass

        class FakeWriteBuilder:
            def new_update(self):
                return FakeUpdate()

            def new_commit(self):
                return FakeCommit()

        users.raw_table.new_batch_write_builder = lambda: FakeWriteBuilder()

        (
            users.merge("id")
            .when_matched_update(
                {"age": source_col("age")},
                where="source.age > target.age and source.name != 'target.name'",
            )
            .when_not_matched_insert(where="source.age > 0")
            .execute([
                {"id": 1, "name": "Alice", "age": 31},
            ])
        )

        self.assertEqual({"id": "id"}, calls["on"])
        self.assertEqual(
            "s.age > t.age and s.name != 'target.name'",
            calls["matched"][0].condition,
        )
        self.assertEqual("s.age > 0", calls["not_matched"][0].condition)
        self.assertEqual([], calls["messages"])

    def test_merge_delete_where_uses_source_and_target_aliases(self):
        users = self.conn.create_table(
            "users",
            schema=_schema({
                "id": pa.int32(),
                "name": pa.string(),
                "age": pa.int32(),
            }),
            options=_PARQUET_OPTIONS,
        )

        calls = {}

        class FakeUpdate:
            def merge_into(
                    self,
                    source,
                    on,
                    when_matched=None,
                    when_not_matched=None):
                calls["matched"] = list(when_matched or [])
                return []

        class FakeCommit:
            def commit(self, messages):
                calls["messages"] = messages

            def close(self):
                pass

        class FakeWriteBuilder:
            def new_update(self):
                return FakeUpdate()

            def new_commit(self):
                return FakeCommit()

        users.raw_table.new_batch_write_builder = lambda: FakeWriteBuilder()

        (
            users.merge("id")
            .when_matched_delete(
                where="source.age < target.age and source.name != 'target.name'",
            )
            .execute([
                {"id": 1, "name": "Alice", "age": 29},
            ])
        )

        self.assertTrue(calls["matched"][0].delete)
        self.assertEqual(
            "s.age < t.age and s.name != 'target.name'",
            calls["matched"][0].condition,
        )
        self.assertEqual([], calls["messages"])

    def test_merge_validates_on_columns(self):
        users = self.conn.create_table(
            "users",
            schema=_schema({"id": pa.int32(), "name": pa.string()}),
            options=_PARQUET_OPTIONS,
        )

        with self.assertRaisesRegex(ValueError, "source columns"):
            users.merge({"id": "missing"}) \
                .when_matched_update() \
                .execute([{"id": 1, "name": "Alice"}])

    def test_create_index_normalizes_full_text_alias(self):
        docs = self.conn.create_table(
            "docs",
            schema=_schema({
                "id": pa.int32(),
                "content": pa.string(),
            }),
            options=_PARQUET_OPTIONS,
        )

        calls = []

        def create_global_index(column, index_type, options=None):
            calls.append((column, index_type, options))
            return index_type

        docs.raw_table.create_global_index = create_global_index

        options = {"tokenizer": "default"}
        self.assertEqual(
            "tantivy-fulltext",
            docs.create_index("content", index_type="full-text",
                              options=options),
        )
        self.assertEqual(
            "tantivy-fulltext",
            docs.create_index("content", index_type="full_text"),
        )
        self.assertEqual(
            "tantivy-fulltext",
            docs.create_index("content", index_type="fulltext"),
        )

        self.assertEqual(
            [
                ("content", "tantivy-fulltext", options),
                ("content", "tantivy-fulltext", None),
                ("content", "tantivy-fulltext", None),
            ],
            calls,
        )

    def test_create_index_requires_index_type(self):
        docs = self.conn.create_table(
            "docs",
            schema=_schema({
                "id": pa.int32(),
                "content": pa.string(),
            }),
            options=_PARQUET_OPTIONS,
        )

        def create_global_index(column, index_type, options=None):
            return index_type

        docs.raw_table.create_global_index = create_global_index

        with self.assertRaises(TypeError):
            docs.create_index("content")

    def test_table_does_not_expose_predicate_builder(self):
        docs = self.conn.create_table(
            "docs",
            schema=_schema({"id": pa.int32()}),
            options=_PARQUET_OPTIONS,
        )

        self.assertFalse(hasattr(docs, "predicate_builder"))

    def test_search_reads_vector_matching_rows(self):
        docs = self.conn.create_table(
            "docs",
            schema=_schema({
                "id": pa.int32(),
                "content": pa.string(),
                "embedding": _vector(3),
            }),
            options=_PARQUET_OPTIONS,
        )
        docs.add([
            {"id": 1, "content": "a", "embedding": [1.0, 0.0, 0.0]},
            {"id": 2, "content": "b", "embedding": [0.0, 1.0, 0.0]},
            {"id": 3, "content": "c", "embedding": [0.0, 0.0, 1.0]},
        ])

        calls = {}

        class FakeVectorBuilder:
            def with_vector_column(self, column):
                calls["column"] = column
                return self

            def with_query_vector(self, vector):
                calls["vector"] = vector
                return self

            def with_limit(self, limit):
                calls["limit"] = limit
                return self

            def with_options(self, options):
                calls["options"] = options
                return self

            def with_filter(self, predicate):
                calls["filter"] = predicate
                return self

            def execute_local(self):
                return GlobalIndexResult.from_range(Range(1, 1))

        docs.raw_table.new_vector_search_builder = lambda: FakeVectorBuilder()

        result = (
            docs.search([0.0, 1.0, 0.0])
            .where("id >= 1")
            .limit(5)
            .to_arrow()
        )

        self.assertEqual("embedding", calls["column"])
        self.assertEqual([0.0, 1.0, 0.0], calls["vector"])
        self.assertEqual(5, calls["limit"])
        self.assertEqual([2], result["id"].to_pylist())

    def test_search_applies_pre_filter_to_vector_builder(self):
        docs = self.conn.create_table(
            "docs",
            schema=_schema({
                "id": pa.int32(),
                "category": pa.string(),
                "embedding": _vector(3),
            }),
            options=_PARQUET_OPTIONS,
        )
        docs.add([
            {"id": 1, "category": "lake", "embedding": [1.0, 0.0, 0.0]},
            {"id": 2, "category": "city", "embedding": [0.0, 1.0, 0.0]},
        ])

        calls = {}

        class FakeVectorBuilder:
            def with_vector_column(self, column):
                return self

            def with_query_vector(self, vector):
                return self

            def with_limit(self, limit):
                return self

            def with_options(self, options):
                return self

            def with_filter(self, predicate):
                calls["pre_filter"] = predicate
                return self

            def execute_local(self):
                return GlobalIndexResult.from_range(Range(0, 0))

        docs.raw_table.new_vector_search_builder = lambda: FakeVectorBuilder()

        docs.search(
            [1.0, 0.0, 0.0],
            pre_filter="category = 'lake'",
        ).limit(1).to_list()

        self.assertEqual("equal", calls["pre_filter"].method)
        self.assertEqual("category", calls["pre_filter"].field)
        self.assertEqual(["lake"], calls["pre_filter"].literals)

    def test_search_pre_filter_rejects_predicate_object(self):
        docs = self.conn.create_table(
            "docs",
            schema=_schema({
                "id": pa.int32(),
                "embedding": _vector(3),
            }),
            options=_PARQUET_OPTIONS,
        )
        predicate = PredicateBuilder(docs.raw_table.fields).equal("id", 1)

        with self.assertRaisesRegex(ValueError, "SQL-like string"):
            docs.search([1.0, 0.0, 0.0]).pre_filter(predicate)

    def test_search_accepts_generator_vector(self):
        docs = self.conn.create_table(
            "docs",
            schema=_schema({
                "id": pa.int32(),
                "embedding": _vector(3),
            }),
            options=_PARQUET_OPTIONS,
        )
        docs.add([{"id": 1, "embedding": [1.0, 0.0, 0.0]}])

        calls = {}

        class FakeVectorBuilder:
            def with_vector_column(self, column):
                calls["column"] = column
                return self

            def with_query_vector(self, vector):
                calls["vector"] = vector
                return self

            def with_limit(self, limit):
                calls["limit"] = limit
                return self

            def with_options(self, options):
                calls["options"] = options
                return self

            def execute_local(self):
                return GlobalIndexResult.from_range(Range(0, 0))

        docs.raw_table.new_vector_search_builder = lambda: FakeVectorBuilder()

        result = docs.search((v for v in [1.0, 0.0, 0.0])).limit(1).to_list()

        self.assertEqual("embedding", calls["column"])
        self.assertEqual([1.0, 0.0, 0.0], calls["vector"])
        self.assertEqual([{"id": 1, "embedding": [1.0, 0.0, 0.0]}], result)

    def test_search_rejects_batch_vectors(self):
        docs = self.conn.create_table(
            "docs",
            schema=_schema({
                "id": pa.int32(),
                "embedding": _vector(3),
            }),
            options=_PARQUET_OPTIONS,
        )

        with self.assertRaisesRegex(ValueError, "use search_vectors"):
            docs.search([[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]])

    def test_search_vectors_reads_one_result_set_per_query_vector(self):
        docs = self.conn.create_table(
            "docs",
            schema=_schema({
                "id": pa.int32(),
                "content": pa.string(),
                "embedding": _vector(3),
            }),
            options=_PARQUET_OPTIONS,
        )
        docs.add([
            {"id": 1, "content": "a", "embedding": [1.0, 0.0, 0.0]},
            {"id": 2, "content": "b", "embedding": [0.0, 1.0, 0.0]},
            {"id": 3, "content": "c", "embedding": [0.0, 0.0, 1.0]},
        ])

        calls = {}

        class FakeBatchVectorBuilder:
            def with_vector_column(self, column):
                calls["column"] = column
                return self

            def with_query_vectors(self, vectors):
                calls["vectors"] = vectors
                return self

            def with_limit(self, limit):
                calls["limit"] = limit
                return self

            def with_options(self, options):
                calls["options"] = options
                return self

            def with_filter(self, predicate):
                calls["filter"] = predicate
                return self

            def execute_batch_local(self):
                return [
                    GlobalIndexResult.from_range(Range(0, 0)),
                    GlobalIndexResult.from_range(Range(2, 2)),
                ]

        docs.raw_table.new_batch_vector_search_builder = (
            lambda: FakeBatchVectorBuilder())

        result = (
            docs.search_vectors(
                [[1.0, 0.0, 0.0], [0.0, 0.0, 1.0]],
                options={"nprobe": "8"},
                pre_filter="content = 'a'",
            )
            .where("id >= 1")
            .select(["id"])
            .limit(2)
            .to_list()
        )

        self.assertEqual("embedding", calls["column"])
        self.assertEqual(
            [[1.0, 0.0, 0.0], [0.0, 0.0, 1.0]],
            calls["vectors"],
        )
        self.assertEqual(2, calls["limit"])
        self.assertEqual({"nprobe": "8"}, calls["options"])
        self.assertEqual("content", calls["filter"].field)
        self.assertEqual(["a"], calls["filter"].literals)
        self.assertEqual([[{"id": 1}], [{"id": 3}]], result)

    def test_search_vectors_rejects_text_parameter(self):
        docs = self.conn.create_table(
            "docs",
            schema=_schema({
                "id": pa.int32(),
                "content": pa.string(),
                "embedding": _vector(3),
            }),
            options=_PARQUET_OPTIONS,
        )

        with self.assertRaises(TypeError):
            docs.search_vectors([[1.0, 0.0, 0.0]], text="paimon")

    def test_search_reads_text_query(self):
        docs = self.conn.create_table(
            "docs",
            schema=_schema({
                "id": pa.int32(),
                "content": pa.string(),
            }),
            options=_PARQUET_OPTIONS,
        )
        docs.add([
            {"id": 1, "content": "paimon vector"},
            {"id": 2, "content": "lakehouse"},
        ])

        calls = {}

        class FakeFullTextBuilder:
            def with_query(self, query):
                calls["query"] = query.to_dict()
                return self

            def with_limit(self, limit):
                calls["limit"] = limit
                return self

            def execute_local(self):
                return GlobalIndexResult.from_range(Range(0, 0))

        docs.raw_table.new_full_text_search_builder = lambda: FakeFullTextBuilder()

        result = (
            docs.search("paimon vector")
            .limit(1)
            .to_arrow()
        )

        self.assertEqual(1, calls["limit"])
        self.assertEqual("content", calls["query"]["match"]["column"])
        self.assertEqual("paimon vector", calls["query"]["match"]["terms"])
        self.assertEqual("Or", calls["query"]["match"]["operator"])
        self.assertEqual([1], result["id"].to_pylist())

    def test_search_string_requires_unambiguous_text_column(self):
        docs = self.conn.create_table(
            "docs",
            schema=_schema({
                "id": pa.int32(),
                "title": pa.string(),
                "content": pa.string(),
            }),
            options=_PARQUET_OPTIONS,
        )

        with self.assertRaisesRegex(ValueError, "Multiple text columns"):
            docs.search("paimon")

    def test_search_hybrid_rejects_shorthand_arguments(self):
        docs = self.conn.create_table(
            "docs",
            schema=_schema({
                "id": pa.int32(),
                "embedding": _vector(3),
            }),
            options=_PARQUET_OPTIONS,
        )

        with self.assertRaises(TypeError):
            docs.search_hybrid(vector=[1.0, 0.0, 0.0])

    def test_hybrid_route_helpers_use_route_specific_arguments(self):
        with self.assertRaises(TypeError):
            pmm.vector_route([1.0, 0.0, 0.0])
        route = pmm.text_route("paimon")
        self.assertFalse(hasattr(route, "column"))
        self.assertEqual("paimon", route.query)

    def test_search_can_build_hybrid_routes(self):
        docs = self.conn.create_table(
            "docs",
            schema=_schema({
                "id": pa.int32(),
                "content": pa.string(),
                "embedding": _vector(3),
            }),
            options=_PARQUET_OPTIONS,
        )
        docs.add([
            {"id": 1, "content": "paimon", "embedding": [1.0, 0.0, 0.0]},
            {"id": 2, "content": "vector", "embedding": [0.0, 1.0, 0.0]},
        ])

        calls = {}

        class FakeHybridBuilder:
            def with_limit(self, limit):
                calls["limit"] = limit
                return self

            def with_ranker(self, ranker):
                calls["ranker"] = ranker
                return self

            def add_vector_route(
                    self, column, vector, limit, weight=1.0, options=None):
                calls["vector"] = (column, vector, limit, weight, options)
                return self

            def add_full_text_route(
                    self, query_json, limit, weight=1.0, options=None):
                calls["text"] = (json.loads(query_json), limit, weight, options)
                return self

            def with_filter(self, predicate):
                calls["filter"] = predicate
                return self

            def execute_local(self):
                return GlobalIndexResult.from_range(Range(0, 1))

        docs.raw_table.new_hybrid_search_builder = lambda: FakeHybridBuilder()

        result = (
            docs.search_hybrid(
                [
                    pmm.vector_route("embedding", [1.0, 0.0, 0.0]),
                    pmm.text_route("paimon"),
                ],
            )
            .rerank("rrf")
            .limit(2)
            .to_arrow()
        )

        self.assertEqual(2, calls["limit"])
        self.assertEqual("rrf", calls["ranker"])
        self.assertEqual(
            ("embedding", [1.0, 0.0, 0.0], 2, 1.0, {}),
            calls["vector"],
        )
        self.assertEqual("content", calls["text"][0]["match"]["column"])
        self.assertEqual("paimon", calls["text"][0]["match"]["terms"])
        self.assertEqual([1, 2], result["id"].to_pylist())

    def test_search_hybrid_applies_pre_filter_to_vector_routes(self):
        docs = self.conn.create_table(
            "docs",
            schema=_schema({
                "id": pa.int32(),
                "category": pa.string(),
                "embedding": _vector(3),
            }),
            options=_PARQUET_OPTIONS,
        )
        docs.add([
            {"id": 1, "category": "lake", "embedding": [1.0, 0.0, 0.0]},
            {"id": 2, "category": "city", "embedding": [0.0, 1.0, 0.0]},
        ])

        calls = {}

        class FakeHybridBuilder:
            def with_limit(self, limit):
                calls["limit"] = limit
                return self

            def with_ranker(self, ranker):
                calls["ranker"] = ranker
                return self

            def add_vector_route(
                    self, column, vector, limit, weight=1.0, options=None):
                calls["vector"] = (column, vector, limit, weight, options)
                return self

            def with_filter(self, predicate):
                calls["pre_filter"] = predicate
                return self

            def execute_local(self):
                return GlobalIndexResult.from_range(Range(0, 0))

        docs.raw_table.new_hybrid_search_builder = lambda: FakeHybridBuilder()

        result = (
            docs.search_hybrid(
                [pmm.vector_route("embedding", [1.0, 0.0, 0.0])],
                pre_filter="category = 'lake'",
            )
            .limit(1)
            .to_list()
        )

        self.assertEqual(1, calls["limit"])
        self.assertEqual(
            ("embedding", [1.0, 0.0, 0.0], 1, 1.0, {}),
            calls["vector"],
        )
        self.assertEqual("category", calls["pre_filter"].field)
        self.assertEqual(["lake"], calls["pre_filter"].literals)
        self.assertEqual(
            [{"id": 1, "category": "lake", "embedding": [1.0, 0.0, 0.0]}],
            result,
        )

    def test_module_does_not_export_vectors_route(self):
        self.assertFalse(hasattr(pmm, "vectors_route"))
        self.assertFalse(hasattr(pmm, "VectorsRoute"))

    def test_search_hybrid_can_build_multiple_vector_routes(self):
        docs = self.conn.create_table(
            "docs",
            schema=_schema({
                "id": pa.int32(),
                "content": pa.string(),
                "image_embedding": _vector(3),
                "text_embedding": _vector(3),
            }),
            options=_PARQUET_OPTIONS,
        )
        docs.add([
            {
                "id": 1,
                "content": "paimon",
                "image_embedding": [1.0, 0.0, 0.0],
                "text_embedding": [0.0, 1.0, 0.0],
            },
            {
                "id": 2,
                "content": "vector",
                "image_embedding": [0.0, 1.0, 0.0],
                "text_embedding": [1.0, 0.0, 0.0],
            },
        ])

        calls = {"vector_routes": [], "text_routes": []}

        class FakeHybridBuilder:
            def with_limit(self, limit):
                calls["limit"] = limit
                return self

            def with_ranker(self, ranker):
                calls["ranker"] = ranker
                return self

            def add_vector_route(
                    self, column, vector, limit, weight=1.0, options=None):
                calls["vector_routes"].append(
                    (column, vector, limit, weight, options))
                return self

            def add_full_text_route(
                    self, query_json, limit, weight=1.0, options=None):
                calls["text_routes"].append(
                    (json.loads(query_json), limit, weight, options))
                return self

            def execute_local(self):
                return GlobalIndexResult.from_range(Range(0, 0))

        docs.raw_table.new_hybrid_search_builder = lambda: FakeHybridBuilder()

        result = (
            docs.search_hybrid(
                [
                    pmm.vector_route(
                        "image_embedding",
                        [1.0, 0.0, 0.0],
                        weight=0.7,
                        limit=6,
                        options={"nprobe": "8"},
                    ),
                    pmm.vector_route(
                        "text_embedding",
                        [0.0, 1.0, 0.0],
                        weight=0.3,
                        limit=4,
                        options={"nprobe": "4"},
                    ),
                    pmm.text_route("paimon", weight=0.2),
                ],
                ranker="weighted_score",
                route_limit=4,
            )
            .limit(2)
            .to_list()
        )

        self.assertEqual(2, calls["limit"])
        self.assertEqual("weighted_score", calls["ranker"])
        self.assertEqual(
            [
                (
                    "image_embedding",
                    [1.0, 0.0, 0.0],
                    6,
                    0.7,
                    {"nprobe": "8"},
                ),
                (
                    "text_embedding",
                    [0.0, 1.0, 0.0],
                    4,
                    0.3,
                    {"nprobe": "4"},
                ),
            ],
            calls["vector_routes"],
        )
        self.assertEqual("content", calls["text_routes"][0][0]["match"]["column"])
        self.assertEqual("paimon", calls["text_routes"][0][0]["match"]["terms"])
        self.assertEqual(4, calls["text_routes"][0][1])
        self.assertEqual(0.2, calls["text_routes"][0][2])
        self.assertEqual(
            [{
                "id": 1,
                "content": "paimon",
                "image_embedding": [1.0, 0.0, 0.0],
                "text_embedding": [0.0, 1.0, 0.0],
            }],
            result,
        )


if __name__ == "__main__":
    unittest.main()
