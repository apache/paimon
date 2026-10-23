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

"""Lance exports retain logical scan results and own their BLOB payloads."""

import shutil
import sys
from unittest.mock import patch

import pyarrow as pa
import pytest

import pypaimon.multimodal as pmm

lance = pytest.importorskip("lance", minversion="13.0.0", reason="pylance >= 13.0.0 is required for Lance exports")


@pytest.fixture
def conn(tmp_path):
    return pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})


def _table(conn, name, fields, data=None, options=None):
    return conn.create_table(
        name, schema=pa.schema([pa.field(name, dtype) for name, dtype in fields.items()]),
        data=data, options=dict({"file.format": "parquet", "read.batch-size": "2"}, **(options or {})))


def _rows(path):
    dataset = lance.dataset(str(path))
    rows = dataset.to_table().to_pylist()
    for field in dataset.schema:
        if (field.metadata or {}).get(b"lance-encoding:blob") != b"true":
            continue
        # Native scans return descriptors. take_blobs gives NULL and empty
        # BLOBs the same zero-length file, so preserve NULL from the scan.
        indices = [index for index, row in enumerate(rows) if row[field.name] is not None]
        if not indices:
            continue
        for index, blob in zip(indices, dataset.take_blobs(field.name, indices=indices)):
            try:
                rows[index][field.name] = blob.read()
            finally:
                blob.close()
    return rows


def test_export_lance_preserves_filter_projection_limit_and_vector(conn, tmp_path):
    table = _table(conn, "vectors", {
        "id": pa.int32(), "split": pa.string(),
        "embedding": pa.list_(pa.float32(), 3), "text": pa.string(),
    }, [{"id": i, "split": "train" if i % 2 else "test",
         "embedding": [float(i), 0.0, 1.0], "text": "row-%s" % i}
        for i in range(6)])
    query = table.scan().where("split = 'train'").select(["embedding", "text", "id"]).limit(2)
    path = tmp_path / "train.lance"

    exported = query.export_lance(str(path))

    assert isinstance(exported, lance.LanceDataset)
    result = lance.dataset(str(path)).to_table()
    assert result.column_names == ["embedding", "text", "id"]
    assert result.schema.field("embedding").type == pa.list_(pa.float32(), 3)
    assert result.num_rows == 2
    assert {row["id"] for row in result.to_pylist()} <= {1, 3, 5}
    assert len({row["id"] for row in result.to_pylist()}) == 2
    for row in result.to_pylist():
        assert row["text"] == "row-%s" % row["id"]
        assert row["embedding"] == [float(row["id"]), 0.0, 1.0]


def test_export_lance_exports_logical_updates_deletes_and_historical_schema(conn, tmp_path):
    from pypaimon.schema.data_types import AtomicType
    from pypaimon.schema.schema_change import SchemaChange

    table = _table(conn, "history", {
        "id": pa.int32(), "text": pa.string(), "image": pa.large_binary(),
    }, [
        {"id": 1, "text": "before", "image": b"before"},
        {"id": 2, "text": "deleted", "image": b"deleted"},
    ])
    snapshot_id = table.raw_table.snapshot_manager().get_latest_snapshot().id
    table.raw_table.create_tag("training-v1", snapshot_id=snapshot_id)
    conn.catalog.alter_table("default.history", [
        SchemaChange.add_column("note", AtomicType("STRING")),
    ])
    table = conn.get_table("history")
    table.update("id = 1", {"text": "after", "image": b"after"})
    table.delete("id = 2")
    table.add([{"id": 3, "text": "new", "image": b"new", "note": "added"}])

    current_path = tmp_path / "current.lance"
    table.scan().export_lance(str(current_path))

    assert lance.dataset(str(current_path)).schema.field("image").metadata[b"lance-encoding:blob"] == b"true"
    assert sorted(_rows(current_path), key=lambda row: row["id"]) == [
        {"id": 1, "text": "after", "image": b"after", "note": None},
        {"id": 3, "text": "new", "image": b"new", "note": "added"},
    ]
    for label, version in (("snapshot", {"snapshot_id": snapshot_id}),
                           ("tag", {"tag_name": "training-v1"})):
        path = tmp_path / (label + ".lance")
        query = table.scan(**version).where("id <= 2")
        names = ["id", "text", "image"]
        if label == "tag":
            names = ["text", "image", "id"]
            query.select(names)
        query.export_lance(str(path))
        assert lance.dataset(str(path)).schema.names == names
        assert lance.dataset(str(path)).schema.field("image").metadata[b"lance-encoding:blob"] == b"true"
        assert sorted(_rows(path), key=lambda row: row["id"]) == [
            {"text": "before", "image": b"before", "id": 1},
            {"text": "deleted", "image": b"deleted", "id": 2},
        ]


@pytest.mark.parametrize("empty", ["uncommitted", "filter", "limit"])
def test_export_lance_empty_scan_preserves_schema(conn, tmp_path, empty):
    table = _table(conn, "empty", {"id": pa.int32(), "image": pa.large_binary()})
    if empty != "uncommitted":
        table.add([{"id": 1, "image": b"body"}])
    query = table.scan().select(["image", "id"])
    if empty == "filter":
        query.where("id < 0")
    elif empty == "limit":
        query.limit(0)
    path = tmp_path / "empty.lance"

    query.export_lance(str(path))

    reopened = lance.dataset(str(path))
    assert reopened.count_rows() == 0
    assert reopened.schema.names == ["image", "id"]
    assert reopened.schema.field("id").type == pa.int32()
    assert reopened.schema.field("image").type == pa.large_binary()
    assert reopened.schema.field("image").metadata[b"lance-encoding:blob"] == b"true"
    assert _rows(path) == []


def test_export_lance_defaults_scalar_blobs_to_native_lazy_files(conn, tmp_path):
    payload = b"0123456789abcdef"
    expected = [
        {"id": 1, "image": payload, "raw": b"plain-bytes"},
        {"id": 2, "image": b"", "raw": b""},
        {"id": 3, "image": None, "raw": None},
    ]
    table = _table(conn, "native_blobs", {
        "id": pa.int32(), "image": pa.large_binary(), "raw": pa.binary(),
    }, expected)
    path = tmp_path / "native.lance"

    table.scan().export_lance(str(path))
    shutil.rmtree(str(tmp_path / "warehouse"))

    reopened = lance.dataset(str(path))
    image = reopened.schema.field("image")
    assert image.type == pa.large_binary()
    assert (image.metadata or {}).get(b"lance-encoding:blob") == b"true"
    raw = reopened.schema.field("raw")
    assert raw.type == pa.binary()
    assert b"lance-encoding:blob" not in (raw.metadata or {})
    descriptors = reopened.to_table().to_pylist()
    indices = {row["id"]: index for index, row in enumerate(descriptors)}
    assert descriptors[indices[1]]["image"]["size"] == len(payload)
    assert descriptors[indices[2]]["image"] is not None
    assert descriptors[indices[2]]["image"]["size"] == 0
    assert descriptors[indices[3]]["image"] is None
    assert [row["raw"] for row in descriptors] == [row["raw"] for row in expected]

    files = reopened.take_blobs("image", indices=[indices[1], indices[2]])
    with files[0] as blob:
        assert blob.readable()
        assert blob.seekable()
        assert blob.size() == len(payload)
        assert blob.tell() == 0
        assert blob.read(3) == payload[:3]
        assert blob.tell() == 3
        blob.seek(5)
        assert blob.read(4) == payload[5:9]
        blob.seek(-4, 2)
        assert blob.read() == payload[-4:]
    assert files[0].closed
    with files[1] as empty_blob:
        assert empty_blob.size() == 0
        assert empty_blob.read() == b""
    assert sorted(_rows(path), key=lambda row: row["id"]) == expected


def test_export_lance_materializes_blob_and_array_payloads_independently(conn, tmp_path):
    schema = {"id": pa.int32(), "image": pa.large_binary(), "pages": pa.list_(pa.large_binary())}
    expected = [
        {"id": 1, "image": b"first", "pages": [b"one", None, b"", b"one"]},
        {"id": 2, "image": b"", "pages": None},
        {"id": 3, "image": None, "pages": []},
    ]
    table = _table(conn, "blobs", schema, expected)
    path = tmp_path / "blobs.lance"

    table.scan().select(["pages", "id", "image"]).export_lance(str(path), blob_parallelism=2)
    shutil.rmtree(str(tmp_path / "warehouse"))

    reopened = lance.dataset(str(path))
    assert reopened.schema.names == ["pages", "id", "image"]
    assert reopened.schema.field("image").type == pa.large_binary()
    assert reopened.schema.field("image").metadata[b"lance-encoding:blob"] == b"true"
    assert reopened.schema.field("pages").type.value_type == pa.large_binary()
    assert b"lance-encoding:blob" not in (reopened.schema.field("pages").metadata or {})
    assert b"lance-encoding:blob" not in (reopened.schema.field("pages").type.value_field.metadata or {})
    assert sorted(_rows(path), key=lambda row: row["id"]) == expected


def test_export_lance_blob_only_projection_filters_on_hidden_column(conn, tmp_path):
    table = _table(conn, "blob_only", {"id": pa.int32(), "image": pa.large_binary()}, [
        {"id": 1, "image": b"one"}, {"id": 2, "image": b""},
        {"id": 3, "image": None}, {"id": 4, "image": b"excluded"},
    ])
    path = tmp_path / "blob_only.lance"

    table.scan().where("id <= 3").select("image").export_lance(str(path))

    assert lance.dataset(str(path)).schema.names == ["image"]
    rows = _rows(path)
    assert len(rows) == 3
    assert {row["image"] for row in rows} == {b"one", b"", None}


def test_export_lance_materializes_map_blobs_as_ordered_key_value_entries(conn, tmp_path):
    table = _table(conn, "map_blobs", {
        "id": pa.int32(), "image": pa.large_binary(),
        "assets": pa.map_(pa.string(), pa.large_binary()),
        "counts": pa.map_(pa.string(), pa.int32()),
    }, [
        {"id": 1, "image": b"preview", "assets": [
            ("last", b"last"), ("empty", b""), ("null", None), ("first", b"first"),
        ], "counts": [("z", 3), ("a", 1)]},
        {"id": 2, "image": b"", "assets": None, "counts": None},
        {"id": 3, "image": None, "assets": [], "counts": []},
    ])
    path = tmp_path / "maps.lance"

    table.scan().select(["assets", "id", "counts", "image"]).export_lance(str(path))
    shutil.rmtree(str(tmp_path / "warehouse"))

    reopened = lance.dataset(str(path))
    assert reopened.schema.names == ["assets", "id", "counts", "image"]
    assert reopened.schema.field("image").metadata[b"lance-encoding:blob"] == b"true"
    for name, value_type in (("assets", pa.large_binary()), ("counts", pa.int32())):
        dtype = reopened.schema.field(name).type
        assert pa.types.is_list(dtype)
        assert [field.name for field in dtype.value_type] == ["key", "value"]
        assert dtype.value_type.field("value").type == value_type
        assert b"lance-encoding:blob" not in (reopened.schema.field(name).metadata or {})
        assert b"lance-encoding:blob" not in (dtype.value_type.field("value").metadata or {})
    assert sorted(_rows(path), key=lambda row: row["id"]) == [
        {"id": 1, "image": b"preview", "assets": [
            {"key": "last", "value": b"last"}, {"key": "empty", "value": b""},
            {"key": "null", "value": None}, {"key": "first", "value": b"first"},
        ], "counts": [{"key": "z", "value": 3}, {"key": "a", "value": 1}]},
        {"id": 2, "image": b"", "assets": None, "counts": None},
        {"id": 3, "image": None, "assets": [], "counts": []},
    ]


def test_export_lance_converts_maps_nested_in_rows_and_arrays(conn, tmp_path):
    map_type = pa.map_(pa.string(), pa.int32())
    row_type = pa.struct([pa.field("label", pa.string()), pa.field("counts", map_type)])
    table = _table(conn, "nested_maps", {
        "id": pa.int32(), "metadata": row_type, "groups": pa.list_(map_type),
    })
    rows = [
        {"id": 1, "metadata": {"label": "first", "counts": [("z", 3), ("a", 1)]},
         "groups": [[("b", 2)], None, []]},
        {"id": 2, "metadata": None, "groups": None},
        {"id": 3, "metadata": {"label": None, "counts": None}, "groups": []},
    ]
    table.add(pa.Table.from_pylist(rows, schema=pa.schema([
        pa.field("id", pa.int32()), pa.field("metadata", row_type), pa.field("groups", pa.list_(map_type)),
    ])))
    path = tmp_path / "nested_maps.lance"

    table.scan().export_lance(str(path))

    assert sorted(_rows(path), key=lambda row: row["id"]) == [
        {"id": 1, "metadata": {"label": "first", "counts": [
            {"key": "z", "value": 3}, {"key": "a", "value": 1},
        ]}, "groups": [[{"key": "b", "value": 2}], None, []]},
        {"id": 2, "metadata": None, "groups": None},
        {"id": 3, "metadata": {"label": None, "counts": None}, "groups": []},
    ]


@pytest.mark.parametrize("initial_rows", [[], [{"id": 1}]])
def test_export_lance_pins_snapshot_before_concurrent_commit(conn, tmp_path, initial_rows):
    from pypaimon.snapshot.time_travel_util import TimeTravelUtil

    table = _table(conn, "concurrent", {"id": pa.int32()}, initial_rows or None)
    query = table.scan()
    path = tmp_path / "concurrent.lance"
    resolve_snapshot = TimeTravelUtil.resolve_snapshot
    committed = []

    def commit_after_snapshot(read_table):
        snapshot = resolve_snapshot(read_table)
        if not committed:
            committed.append(True)
            table.add([{"id": 2}])
        return snapshot

    with patch.object(TimeTravelUtil, "resolve_snapshot", side_effect=commit_after_snapshot):
        query.export_lance(str(path))

    assert _rows(path) == initial_rows
    expected = initial_rows + [{"id": 2}]
    assert sorted(query.to_list(), key=lambda row: row["id"]) == expected
    assert sorted(table.scan().to_list(), key=lambda row: row["id"]) == expected


def test_export_lance_preserves_requested_row_ids(conn, tmp_path):
    table = _table(conn, "row_ids", {"id": pa.int32(), "image": pa.large_binary()}, [
        {"id": 1, "image": b"one"}, {"id": 2, "image": b"two"},
    ])
    path = tmp_path / "row_ids.lance"
    query = table.scan().where("id = 2").select("image").with_row_id()
    row_id = table.scan().where("id = 2").select("id").with_row_id().to_list()[0]["_ROW_ID"]

    query.export_lance(str(path))

    assert lance.dataset(str(path)).schema.names == ["image", "_ROW_ID"]
    assert lance.dataset(str(path)).schema.field("image").metadata[b"lance-encoding:blob"] == b"true"
    assert _rows(path) == [{"image": b"two", "_ROW_ID": row_id}]


def test_export_lance_create_protects_existing_dataset_and_overwrite_replaces_it(conn, tmp_path):
    table = _table(conn, "modes", {"id": pa.int32()}, [{"id": 1}, {"id": 2}])
    path = tmp_path / "modes.lance"
    table.scan().where("id = 1").export_lance(str(path))

    with pytest.raises((OSError, ValueError), match="(?i)exist|already"):
        table.scan().where("id = 2").export_lance(str(path))
    assert _rows(path) == [{"id": 1}]

    table.scan().where("id = 2").export_lance(str(path), mode="overwrite")

    assert _rows(path) == [{"id": 2}]


@pytest.mark.parametrize("kwargs, message", [
    ({"mode": "append"}, "mode"),
    ({"blob_mode": "reference"}, "blob_mode"),
    ({"blob_parallelism": 0}, "blob_parallelism"),
])
def test_export_lance_rejects_unsupported_options_before_writing(conn, tmp_path, kwargs, message):
    table = _table(conn, "invalid_options", {"id": pa.int32()}, [{"id": 1}])
    path = tmp_path / "invalid.lance"

    with pytest.raises(ValueError, match=message):
        table.scan().export_lance(str(path), **kwargs)

    assert not path.exists()


@pytest.mark.parametrize("projection", [[], ["missing"], ["id", "id"]])
def test_export_lance_rejects_invalid_projection_before_writing(conn, tmp_path, projection):
    table = _table(conn, "invalid_projection", {"id": pa.int32()}, [{"id": 1}])
    path = tmp_path / "invalid.lance"

    with pytest.raises(ValueError, match="(?i)column|projection|duplicate"):
        table.scan().select(projection).export_lance(str(path))

    assert not path.exists()


def test_export_lance_rejects_search_queries(conn, tmp_path):
    table = _table(conn, "search", {"id": pa.int32(), "embedding": pa.list_(pa.float32(), 2)})
    queries = [
        table.search([1.0, 0.0], column="embedding"),
        table.search_vectors([[1.0, 0.0], [0.0, 1.0]], column="embedding"),
    ]

    for query in queries:
        with pytest.raises(TypeError, match="only supported on scan"):
            query.export_lance(str(tmp_path / "search.lance"))

    assert not (tmp_path / "search.lance").exists()


def test_export_lance_missing_dependency_has_installation_hint(conn, tmp_path):
    table = _table(conn, "missing_dependency", {"id": pa.int32()}, [{"id": 1}])
    path = tmp_path / "missing_dependency.lance"

    with patch.dict(sys.modules, {"lance": None}):
        with pytest.raises(ImportError, match="pylance|pypaimon\\[lance\\]"):
            table.scan().export_lance(str(path))

    assert not path.exists()


def test_export_lance_old_dependency_has_upgrade_hint_before_writing(conn, tmp_path):
    table = _table(conn, "old_dependency", {"id": pa.int32()}, [{"id": 1}])
    path = tmp_path / "old_dependency.lance"

    with patch.object(lance, "__version__", "12.0.0"):
        with pytest.raises(ImportError, match="13\\.0\\.0"):
            table.scan().export_lance(str(path))

    assert not path.exists()


def test_export_lance_retained_tag_query_uses_schema_of_resolved_snapshot(conn, tmp_path):
    from pypaimon.schema.data_types import AtomicType
    from pypaimon.schema.schema_change import SchemaChange

    table = _table(conn, "moved_tag", {"id": pa.int32(), "text": pa.string()}, [
        {"id": 1, "text": "before"},
    ])
    table.raw_table.create_tag("training")
    query = table.scan(tag_name="training")
    conn.catalog.alter_table("default.moved_tag", [
        SchemaChange.add_column("note", AtomicType("STRING")),
    ])
    current = conn.get_table("moved_tag")
    current.add([{"id": 2, "text": "after", "note": "new-schema"}])
    current.raw_table.replace_tag("training")
    path = tmp_path / "moved_tag.lance"

    query.export_lance(str(path))

    assert lance.dataset(str(path)).schema.names == ["id", "text", "note"]
    assert sorted(_rows(path), key=lambda row: row["id"]) == [
        {"id": 1, "text": "before", "note": None},
        {"id": 2, "text": "after", "note": "new-schema"},
    ]


def test_export_lance_rejects_video_frame_but_allows_projection(conn, tmp_path):
    table = _table(conn, "video", {"id": pa.int32(), "video": pa.large_binary()},
                   options={"video-frame-field": "video"})
    table.add([{"id": 1, "video": None}])
    path = tmp_path / "video.lance"

    with pytest.raises((ValueError, NotImplementedError), match="(?i)video"):
        table.scan().export_lance(str(path))
    assert not path.exists()

    table.scan().select("id").export_lance(str(path))

    assert _rows(path) == [{"id": 1}]


def test_export_lance_rejects_variant_but_allows_projection(conn, tmp_path):
    from pypaimon.data.generic_variant import GenericVariant

    values = GenericVariant.to_arrow_array([GenericVariant.from_python({"answer": 42})])
    table = _table(conn, "variant", {"id": pa.int32(), "details": values.type})
    table.add(pa.Table.from_arrays([pa.array([1], type=pa.int32()), values], names=["id", "details"]))
    path = tmp_path / "variant.lance"

    with pytest.raises((ValueError, NotImplementedError), match="(?i)variant"):
        table.scan().export_lance(str(path))
    assert not path.exists()

    table.scan().select("id").export_lance(str(path))

    assert _rows(path) == [{"id": 1}]


@pytest.mark.python_read
@pytest.mark.parametrize("failure", ["read", "write", "blob"])
def test_export_lance_closes_source_on_failure(conn, tmp_path, failure):
    from pypaimon.execution.python.read.table_read import PythonTableRead
    from pypaimon.multimodal import lance_export
    from pypaimon.read.table_read import TableRead

    table = _table(conn, "failure", {"id": pa.int32(), "image": pa.large_binary()}, [
        {"id": i, "image": b"body"} for i in range(3)
    ])
    original = PythonTableRead._arrow_batch_generator
    fetch_blob_bodies = lance_export.fetch_blob_bodies
    closed = []

    def tracked_read(read, *args, **kwargs):
        reader = original(read, *args, **kwargs)
        try:
            for batch in reader:
                yield batch
                if failure == "read":
                    raise RuntimeError("source read failed")
        finally:
            reader.close()
            closed.append(True)

    def failed_write(data, *args, **kwargs):
        next(iter(data))
        raise RuntimeError("destination write failed")

    def fetch(*args, **kwargs):
        if failure == "blob":
            raise OSError("payload fetch failed")
        return fetch_blob_bodies(*args, **kwargs)

    write = failed_write if failure == "write" else lance.write_dataset
    expected_error = {"read": "source read failed", "write": "destination write failed", "blob": "payload fetch failed"}
    with patch.object(TableRead, "_try_native_batches", return_value=None), \
            patch.object(PythonTableRead, "_arrow_batch_generator", tracked_read), \
            patch.object(lance_export, "fetch_blob_bodies", side_effect=fetch), \
            patch.object(lance, "write_dataset", write):
        try:
            table.scan().export_lance(str(tmp_path / "failure.lance"))
        except Exception as error:
            assert expected_error[failure] in str(error)
            # Check while the traceback still retains the generator, ruling out
            # closure that only happens later through garbage collection.
            assert closed == [True]
        else:
            pytest.fail("Expected export to propagate its read/write failure")


@pytest.mark.parametrize("mode", ["create", "overwrite"])
def test_export_lance_failed_later_blob_batch_never_publishes_partial_dataset(conn, tmp_path, mode):
    from pypaimon.multimodal import lance_export

    table = _table(conn, "partial_failure", {"id": pa.int32(), "image": pa.large_binary()}, [
        {"id": i, "image": ("image-%s" % i).encode()} for i in range(5)
    ])
    path = tmp_path / "partial.lance"
    previous = pa.table({"id": [99], "image": [b"previous"]}, schema=pa.schema([
        pa.field("id", pa.int32()),
        pa.field("image", pa.large_binary(), metadata={b"lance-encoding:blob": b"true"}),
    ]))
    if mode == "overwrite":
        lance.write_dataset(previous, str(path), data_storage_version="2.1")
    fetch_blob_bodies = lance_export.fetch_blob_bodies
    write_dataset = lance.write_dataset
    fetched = []

    def fetch(*args, **kwargs):
        if fetched:
            raise OSError("BLOB unavailable in a later batch")
        fetched.append(True)
        return fetch_blob_bodies(*args, **kwargs)

    def write(*args, **kwargs):
        # Exercise Lance's real writer with small file limits and a later
        # payload failure, checking only whether a dataset was published.
        return write_dataset(*args, max_rows_per_file=1, max_rows_per_group=1, **kwargs)

    with patch.object(lance_export, "fetch_blob_bodies", side_effect=fetch), \
            patch.object(lance, "write_dataset", side_effect=write):
        with pytest.raises(OSError, match="BLOB unavailable in a later batch"):
            table.scan().export_lance(str(path), mode=mode)

    if mode == "overwrite":
        assert lance.dataset(str(path)).schema.equals(previous.schema, check_metadata=True)
        assert _rows(path) == previous.to_pylist()
    else:
        with pytest.raises(ValueError):
            lance.dataset(str(path))


@pytest.mark.python_plan
@pytest.mark.parametrize("view", ["latest", "moved_tag"])
def test_export_lance_rebinds_predicates_after_schema_reordering(conn, tmp_path, view):
    from copy import deepcopy

    from pypaimon.schema.data_types import AtomicType
    from pypaimon.schema.schema_change import Move, SchemaChange

    table = conn.create_table(
        "reordered", schema=pa.schema([
            pa.field("part", pa.int32()), pa.field("id", pa.int32()),
            pa.field("value", pa.int32()),
        ]), data=[{"part": 99, "id": 1, "value": 20}],
        partitioned=["part", "id"], options={"file.format": "parquet"})
    if view == "moved_tag":
        table.raw_table.create_tag("training")
    query = table.scan(**({"tag_name": "training"} if view == "moved_tag" else {}))
    query.where("id = 1 AND (value >= 20 OR part = 3)").select(["value", "id"])
    original_predicate = deepcopy(query._predicate)
    conn.catalog.alter_table("default.reordered", [
        SchemaChange.add_column("pad", AtomicType("INT"), move=Move.first("pad")),
    ])
    current = conn.get_table("reordered")
    current.add([
        {"pad": 999, "part": 99, "id": 1, "value": 30},
        {"pad": 999, "part": 99, "id": 1, "value": 5},
        {"pad": 999, "part": 99, "id": 2, "value": 40},
    ])
    if view == "moved_tag":
        current.raw_table.replace_tag("training")
    path = tmp_path / (view + ".lance")

    query.export_lance(str(path))

    assert query._predicate == original_predicate
    assert [field.name for field in query._table.fields] == ["part", "id", "value"]
    assert lance.dataset(str(path)).schema.names == ["value", "id"]
    assert sorted(_rows(path), key=lambda row: row["value"]) == [
        {"value": 20, "id": 1}, {"value": 30, "id": 1},
    ]


def test_export_lance_rebinds_row_id_predicate_after_schema_reordering(conn, tmp_path):
    from copy import deepcopy

    from pypaimon.schema.data_types import AtomicType
    from pypaimon.schema.schema_change import Move, SchemaChange

    table = _table(conn, "reordered_row_ids", {"id": pa.int32()}, [{"id": 1}])
    row_id = table.scan().with_row_id().to_list()[0]["_ROW_ID"]
    query = table.take_row_ids([row_id]).select("id").with_row_id()
    original_predicate = deepcopy(query._predicate)
    conn.catalog.alter_table("default.reordered_row_ids", [
        SchemaChange.add_column("pad", AtomicType("INT"), move=Move.first("pad")),
    ])
    conn.get_table("reordered_row_ids").add([{"pad": 999, "id": 2}])
    path = tmp_path / "reordered_row_ids.lance"

    query.export_lance(str(path))

    assert query._predicate == original_predicate
    assert _rows(path) == [{"id": 1, "_ROW_ID": row_id}]


def test_export_lance_allows_scalar_alias_matching_unselected_video_column(conn, tmp_path):
    table = _table(conn, "video_alias", {
        "id": pa.int32(), "video": pa.struct([pa.field("frame", pa.int32())]),
        "video_frame": pa.large_binary(),
    }, [
        {"id": 1, "video": {"frame": 7}, "video_frame": None},
        {"id": 2, "video": {"frame": None}, "video_frame": None},
        {"id": 3, "video": None, "video_frame": None},
    ], options={"video-frame-field": "video_frame"})
    query = table.scan().select(["id", "video.frame"])
    expected = [
        {"id": 1, "video_frame": 7},
        {"id": 2, "video_frame": None},
        {"id": 3, "video_frame": None},
    ]
    assert sorted(query.to_list(), key=lambda row: row["id"]) == expected
    path = tmp_path / "video_alias.lance"

    query.export_lance(str(path))

    reopened = lance.dataset(str(path))
    assert reopened.schema.names == ["id", "video_frame"]
    assert reopened.schema.field("video_frame").type == pa.int32()
    assert sorted(_rows(path), key=lambda row: row["id"]) == expected
