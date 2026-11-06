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

from unittest.mock import patch

import pyarrow as pa
import pytest

import pypaimon.multimodal as pmm
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.write.table_commit import BatchTableCommit


@pytest.fixture
def frames(tmp_path):
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    return connection.create_table(
        "frames",
        schema=pa.schema([
            ("id", pa.int64()), ("label", pa.string()),
            ("image", pa.large_binary()),
        ]),
        data=[{"id": i, "label": "old", "image": bytes([i]) * 100}
              for i in range(5)],
        options={"file.format": "parquet", "read.batch-size": "2",
                 "deletion-vectors.enabled": "true"},
    )


def _snapshot(table):
    return table.raw_table.snapshot_manager().get_latest_snapshot().id


def _files(table):
    return {file.file_name: file
            for split in table.raw_table.new_read_builder().new_scan().plan_all().splits()
            for file in split.files}


def _blobs(table):
    return {name for name in _files(table) if DataFileMeta.is_blob_file(name)}


def _rows(table, columns):
    return sorted(table.scan().select(columns).to_list(), key=lambda row: row["id"])


def test_merge_columns_adds_typed_features_without_reading_or_rewriting_media(frames):
    snapshot = _snapshot(frames)
    files = set(_files(frames))
    blobs = _blobs(frames)
    features = pa.table({
        "id": [3, 1],
        "embedding": pa.array([[3.0, 4.0], [1.0, 2.0]],
                              type=pa.list_(pa.float32(), 2)),
    })
    with patch.object(frames.raw_table.file_io, "read_ranges_coalesced",
                      side_effect=AssertionError("must not fetch media")):
        result = frames.merge_columns(features, on="id")

    assert result.source_snapshot_id == snapshot
    assert result.snapshot_id > snapshot
    assert result.schema_id == frames.raw_table.table_schema.id
    assert result.updated_rows is None
    assert result.added_columns == ("embedding",)
    assert _rows(frames, ["id", "label", "embedding"]) == [
        {"id": i, "label": "old", "embedding":
         [float(i), float(i + 1)] if i in (1, 3) else None}
        for i in range(5)
    ]
    assert _blobs(frames) == blobs
    assert files.issubset(_files(frames))
    assert frames.scan(snapshot_id=snapshot).select(["id", "label"]).to_arrow().num_rows == 5


def test_merge_columns_preserves_omitted_values_and_writes_explicit_null(frames):
    frames.merge_columns(pa.table({"id": [1, 3], "label": [None, "new"]}), on="id")

    assert _rows(frames, ["id", "label"]) == [
        {"id": i, "label": None if i == 1 else "new" if i == 3 else "old"}
        for i in range(5)
    ]
    _, blobs = frames.scan().select(["id", "image"]).read_blobs()
    assert blobs["image"] == [bytes([i]) * 100 for i in range(5)]


def test_merge_columns_rejects_multiple_sources_for_the_same_target(frames):
    snapshot = _snapshot(frames)
    with pytest.raises(pmm.ColumnUpdateError) as failure:
        frames.merge_columns(pa.table({
            "id": [1, 1], "new_label": ["first", "second"],
        }), on="id")

    assert failure.value.phase == "write"
    assert failure.value.schema_changed is True
    assert failure.value.commit_outcome == "not_started"
    assert "multiple source rows" in str(failure.value.__cause__)
    assert _snapshot(frames) == snapshot
    assert frames.scan().select(["new_label"]).to_arrow()["new_label"].to_pylist() == [None] * 5


def test_merge_columns_ignores_unmatched_and_null_keys_and_supports_key_mapping(frames):
    result = frames.merge_columns(
        pa.table({"source_id": pa.array([2, 99, 99, None], type=pa.int32()),
                  "label": ["new", "missing", "duplicate unmatched", "null"]}),
        on={"id": "source_id"})

    assert result.updated_rows is None
    assert _rows(frames, ["id", "label"])[2] == {"id": 2, "label": "new"}
    assert len(_rows(frames, ["id"])) == 5


def test_merge_columns_updates_all_target_rows_with_the_same_key(frames):
    frames.add([{"id": 1, "label": "duplicate", "image": b"other"}])
    snapshot = _snapshot(frames)
    frames.merge_columns(pa.table({"id": [1], "label": ["new"]}), on="id")
    assert _snapshot(frames) > snapshot
    assert frames.scan().where("id = 1").select(["label"]).to_list() == [
        {"label": "new"}, {"label": "new"}]


def test_merge_columns_rejects_incompatible_types_before_schema_changes(frames):
    snapshot = _snapshot(frames)
    with pytest.raises(ValueError, match="label"):
        frames.merge_columns(pa.table({
            "id": [0], "new_label": ["new"], "label": [123],
        }), on="id")
    assert _snapshot(frames) == snapshot
    assert "new_label" not in frames.catalog.get_table(frames.identifier).field_names


def test_add_columns_batches_payloads_and_writes_each_file_group_once(frames):
    snapshot = _snapshot(frames)
    files = set(_files(frames))
    blobs = _blobs(frames)
    batches = []

    def embed(batch):
        assert batch.schema.names == ["image"]
        batches.append(batch.num_rows)
        return pa.table({"embedding": pa.array(
            [[float(image[0]), float(len(image))] for image in batch["image"].to_pylist()],
            type=pa.list_(pa.float32(), 2))})

    result = frames.add_columns(
        schema=pa.schema([("embedding", pa.list_(pa.float32(), 2))]),
        inputs=["image"], fn=embed, batch_size=2)

    assert sum(batches) == 5
    assert max(batches) <= 2
    assert (result.updated_rows, result.source_snapshot_id) == (5, snapshot)
    assert result.added_columns == ("embedding",)
    assert _rows(frames, ["id", "embedding"]) == [
        {"id": i, "embedding": [float(i), 100.0]} for i in range(5)]
    assert _blobs(frames) == blobs
    assert files.issubset(_files(frames))
    feature_files = [file for name, file in _files(frames).items() if name not in files]
    assert len(feature_files) == 1
    assert feature_files[0].write_cols == ["embedding"]


def test_add_columns_schema_only_updates_the_existing_facade(frames):
    snapshot = _snapshot(frames)
    files = set(_files(frames))
    result = frames.add_columns(pa.schema([("caption", pa.string())]))

    assert result.updated_rows == 0
    assert result.snapshot_id == snapshot
    assert set(_files(frames)) == files
    assert frames.scan().select(["caption"]).to_arrow()["caption"].to_pylist() == [None] * 5
    frames.merge_columns(pa.table({"id": [0], "caption": ["hello"]}), on="id")
    assert _rows(frames, ["id", "caption"])[0] == {"id": 0, "caption": "hello"}


def test_add_columns_preserves_deleted_rows_and_crosses_file_groups(frames):
    frames.add([{"id": 5, "label": "old", "image": b"five"}])
    frames.delete("id = 1")
    result = frames.add_columns(
        pa.schema([("doubled", pa.int64())]), inputs=["id"],
        fn=lambda batch: pa.table({"doubled": [i * 2 for i in batch["id"].to_pylist()]}),
        batch_size=1)

    assert result.updated_rows == 5
    assert _rows(frames, ["id", "doubled"]) == [
        {"id": i, "doubled": i * 2} for i in (0, 2, 3, 4, 5)]


@pytest.mark.parametrize("output", [
    lambda batch: pa.table({"feature": [1]}),
    lambda batch: pa.table({"wrong": [1] * batch.num_rows}),
    lambda batch: pa.table({"feature": ["invalid"] * batch.num_rows}),
])
def test_add_columns_reports_schema_publication_but_never_commits_bad_outputs(frames, output):
    snapshot = _snapshot(frames)
    with pytest.raises(pmm.ColumnUpdateError) as failure:
        frames.add_columns(pa.schema([("feature", pa.int64())]),
                           inputs=["id"], fn=output, batch_size=2)

    assert failure.value.phase == "compute"
    assert failure.value.schema_changed is True
    assert failure.value.commit_outcome == "not_started"
    assert failure.value.__cause__ is not None
    assert _snapshot(frames) == snapshot
    assert frames.scan().select(["feature"]).to_arrow()["feature"].to_pylist() == [None] * 5


def test_add_columns_rejects_existing_columns_before_calling_user_code(frames):
    def compute(batch):
        raise AssertionError("must not run")

    with pytest.raises(ValueError, match="already exists"):
        frames.add_columns(pa.schema([("label", pa.string())]), inputs=["image"], fn=compute)


def test_compute_columns_backfills_and_recomputes_without_schema_or_media_changes(frames):
    vector_type = pa.list_(pa.float32(), 2)
    frames.add_columns(pa.schema([("embedding", vector_type)]))
    raw = frames.raw_table
    schema_id = raw.table_schema.id
    blobs = _blobs(frames)
    original_files = set(_files(frames))

    for offset in (0, 10):
        snapshot = _snapshot(frames)
        files = set(_files(frames))
        batches = []

        def embed(batch):
            assert batch.schema.names == ["image"]
            images = batch["image"].to_pylist()
            assert all(isinstance(image, bytes) for image in images)
            batches.append(batch.num_rows)
            return pa.table({"embedding": pa.array(
                [[float(image[0] + offset), float(len(image))] for image in images],
                type=vector_type)})

        with patch.object(frames.catalog, "alter_table", side_effect=AssertionError("must not alter schema")):
            result = frames.compute_columns("embedding", inputs="image", fn=embed, batch_size=2)

        assert sum(batches) == 5
        assert max(batches) <= 2
        assert result.source_snapshot_id == snapshot
        assert result.snapshot_id > snapshot
        assert result.schema_id == schema_id
        assert result.updated_rows == 5
        assert result.added_columns == ()
        assert frames.raw_table is raw
        assert frames.catalog.get_table(frames.identifier).table_schema.id == schema_id
        assert _rows(frames, ["id", "label", "embedding"]) == [
            {"id": i, "label": "old", "embedding": [float(i + offset), 100.0]} for i in range(5)]
        assert _blobs(frames) == blobs
        assert original_files.issubset(_files(frames))
        feature_files = [file for name, file in _files(frames).items() if name not in files]
        assert len(feature_files) == 1
        assert feature_files[0].write_cols == ["embedding"]


def test_compute_columns_can_backfill_after_failed_add(frames):
    with pytest.raises(pmm.ColumnUpdateError):
        frames.add_columns(pa.schema([("feature", pa.int64())]), inputs=["id"],
                           fn=lambda batch: pa.table({"feature": ["invalid"] * batch.num_rows}))
    schema_id = frames.raw_table.table_schema.id
    result = frames.compute_columns(
        ["feature"], inputs=["id"],
        fn=lambda batch: pa.table({"feature": batch["id"]}))

    assert result.schema_id == schema_id
    assert result.added_columns == ()
    assert result.updated_rows == 5
    assert _rows(frames, ["id", "feature"]) == [{"id": i, "feature": i} for i in range(5)]


def test_compute_columns_reads_output_columns_and_accepts_reordered_record_batches(frames):
    def compute(batch):
        assert batch.schema.names == ["id", "label"]
        return pa.RecordBatch.from_pydict({
            "label": [label + "!" for label in batch["label"].to_pylist()],
            "id": [value + 10 for value in batch["id"].to_pylist()],
        })

    with patch.object(frames.raw_table.file_io, "read_ranges_coalesced",
                      side_effect=AssertionError("must not fetch media")):
        for _ in range(2):
            result = frames.compute_columns(["id", "label"], inputs=["id", "label"],
                                            fn=compute, batch_size=2)

    assert result.updated_rows == 5
    assert _rows(frames, ["id", "label"]) == [{"id": i + 20, "label": "old!!"} for i in range(5)]


def test_compute_columns_preserves_deleted_rows_across_partitions(tmp_path):
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = connection.create_table(
        "partitioned", schema=pa.schema([("id", pa.int64()), ("part", pa.string()), ("value", pa.int64())]),
        partitioned=["part"], options={"file.format": "parquet", "deletion-vectors.enabled": "true"},
        data=[{"id": i, "part": "a" if i < 3 else "b", "value": -1} for i in range(6)])
    table.delete("id = 1")
    result = table.compute_columns(
        ["value"], inputs=["id"], batch_size=1,
        fn=lambda batch: pa.table({"value": [i * 2 for i in batch["id"].to_pylist()]}))

    assert result.updated_rows == 5
    assert _rows(table, ["id", "part", "value"]) == [
        {"id": i, "part": "a" if i < 3 else "b", "value": i * 2} for i in (0, 2, 3, 4, 5)]
    with pytest.raises(ValueError, match="partition"):
        table.compute_columns(["part"], inputs=["id"], fn=lambda batch: batch)


@pytest.mark.parametrize("output", [
    lambda batch: ["new"] * batch.num_rows,
    lambda batch: pa.table({"label": ["new"]}),
    lambda batch: pa.table({"wrong": ["new"] * batch.num_rows}),
    lambda batch: pa.table({"label": [123] * batch.num_rows}),
    lambda batch: pa.Table.from_arrays([batch["id"], batch["id"]], names=["label", "label"]),
])
def test_compute_columns_rejects_bad_outputs_without_publishing(frames, output):
    snapshot = _snapshot(frames)
    schema_id = frames.raw_table.table_schema.id
    files = set(_files(frames))
    with pytest.raises(pmm.ColumnUpdateError) as failure:
        frames.compute_columns(["label"], inputs=["id"], fn=output, batch_size=2)

    assert failure.value.phase == "compute"
    assert failure.value.schema_changed is False
    assert failure.value.schema_id == schema_id
    assert failure.value.source_snapshot_id == snapshot
    assert failure.value.commit_outcome == "not_started"
    assert isinstance(failure.value.__cause__, ValueError)
    assert _snapshot(frames) == snapshot
    assert set(_files(frames)) == files
    assert _rows(frames, ["id", "label"]) == [{"id": i, "label": "old"} for i in range(5)]


def test_compute_columns_late_failure_can_be_fully_rerun(frames):
    frames.add([{"id": i, "label": "old", "image": b"other"} for i in range(5, 9)])
    snapshot = _snapshot(frames)
    files = set(_files(frames))
    calls = []

    def compute(batch):
        calls.append(batch.num_rows)
        if len(calls) == 5:
            raise RuntimeError("model failed after staging a file group")
        return pa.table({"label": [str(i) for i in batch["id"].to_pylist()]})

    with pytest.raises(pmm.ColumnUpdateError) as failure:
        frames.compute_columns(["label"], inputs=["id"], fn=compute, batch_size=2)

    assert len(calls) == 5
    assert failure.value.schema_changed is False
    assert failure.value.commit_outcome == "not_started"
    assert isinstance(failure.value.__cause__, RuntimeError)
    assert _snapshot(frames) == snapshot
    assert set(_files(frames)) == files
    assert _rows(frames, ["id", "label"]) == [{"id": i, "label": "old"} for i in range(9)]

    result = frames.compute_columns(["label"], inputs=["id"], fn=compute, batch_size=2)
    assert result.updated_rows == 9
    assert sum(calls[5:]) == 9
    assert _rows(frames, ["id", "label"]) == [{"id": i, "label": str(i)} for i in range(9)]


@pytest.mark.parametrize("nullable", [False, True])
def test_compute_columns_uses_existing_nullability(tmp_path, nullable):
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = connection.create_table(
        "values", schema=pa.schema([("id", pa.int64()), pa.field("value", pa.int64(), nullable=nullable)]),
        data=[{"id": 0, "value": 1}], options={"file.format": "parquet"})
    table.compute_columns(["value"], inputs=["id"], fn=lambda batch: pa.table({"value": [2]}))
    snapshot = _snapshot(table)

    def clear(batch):
        return pa.table({"value": [None] * batch.num_rows})

    if nullable:
        table.compute_columns(["value"], inputs=["id"], fn=clear)
        assert _rows(table, ["id", "value"]) == [{"id": 0, "value": None}]
    else:
        with pytest.raises(pmm.ColumnUpdateError) as failure:
            table.compute_columns(["value"], inputs=["id"], fn=clear)
        assert "non-null column value" in str(failure.value.__cause__)
        assert _snapshot(table) == snapshot
        assert _rows(table, ["id", "value"]) == [{"id": 0, "value": 2}]


@pytest.mark.parametrize("data_type, value, invalid", [
    (pa.struct([pa.field("x", pa.int64(), nullable=False)]), {"x": 2}, {"x": None}),
    (pa.list_(pa.field("element", pa.int64(), nullable=False)), [2], [None]),
])
@pytest.mark.parametrize("as_table", [False, True])
def test_compute_columns_restores_nested_nullability(tmp_path, data_type, value, invalid, as_table):
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    schema = pa.schema([("id", pa.int64()), ("value", data_type)])
    table = connection.create_table(
        "nested", schema=schema,
        data=pa.table({"id": [0, 1], "value": [value, None]}, schema=schema),
        options={"file.format": "parquet"})
    schema_id = table.raw_table.table_schema.id

    def identity(batch):
        field = batch.schema.field("value")
        child = field.type[0] if pa.types.is_struct(field.type) else field.type.value_field
        assert child.nullable
        return pa.Table.from_batches([batch]) if as_table else batch

    result = table.compute_columns(["value"], inputs=["value"], fn=identity)
    assert result.updated_rows == 2
    assert result.added_columns == ()
    assert result.schema_id == schema_id
    assert _rows(table, ["id", "value"]) == [{"id": 0, "value": value}, {"id": 1, "value": None}]
    snapshot = _snapshot(table)

    def invalid_children(batch):
        array = pa.array([invalid] * batch.num_rows, type=batch.schema.field("value").type)
        factory = pa.Table if as_table else pa.RecordBatch
        return factory.from_arrays([array], names=["value"])

    with pytest.raises(pmm.ColumnUpdateError) as failure:
        table.compute_columns(["value"], inputs=["value"], fn=invalid_children)
    assert failure.value.phase == "compute"
    assert failure.value.schema_changed is False
    assert failure.value.commit_outcome == "not_started"
    assert "non-nullable field value." in str(failure.value.__cause__)
    assert _snapshot(table) == snapshot
    assert _rows(table, ["id", "value"]) == [{"id": 0, "value": value}, {"id": 1, "value": None}]


def test_compute_columns_normalizes_reordered_outputs_with_null_siblings(tmp_path):
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    data_type = pa.struct([pa.field("x", pa.int64(), nullable=False)])
    schema = pa.schema([("id", pa.int64()), ("value", data_type), ("label", pa.string())])
    table = connection.create_table(
        "nested", schema=schema,
        data=pa.table({"id": [0], "value": [{"x": 2}], "label": ["old"]}, schema=schema),
        options={"file.format": "parquet"})

    def compute(batch):
        return pa.table({"value": batch["value"], "label": pa.nulls(batch.num_rows)})

    result = table.compute_columns(["label", "value"], inputs=["value"], fn=compute)
    assert result.updated_rows == 1
    assert _rows(table, ["id", "value", "label"]) == [{"id": 0, "value": {"x": 2}, "label": None}]


@pytest.mark.parametrize("method", ["add_columns", "merge_columns"])
def test_column_enrichment_restores_nested_nullability(tmp_path, method):
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    data_type = pa.struct([pa.field("x", pa.int64(), nullable=False)])
    schema = pa.schema([("id", pa.int64()), ("value", data_type)])
    table = connection.create_table(
        "nested", schema=schema, data=pa.table({"id": [0], "value": [{"x": 2}]}, schema=schema),
        options={"file.format": "parquet"})

    if method == "add_columns":
        table.add_columns(pa.schema([("derived", data_type)]), inputs=["value"],
                          fn=lambda batch: pa.table({"derived": batch["value"]}))
        assert _rows(table, ["id", "derived"]) == [{"id": 0, "derived": {"x": 2}}]
    else:
        table.merge_columns(table.scan().select(["id", "value"]).to_arrow(), on="id")
        assert _rows(table, ["id", "value"]) == [{"id": 0, "value": {"x": 2}}]


@pytest.mark.parametrize("kwargs, message", [
    ({"columns": []}, "unique column names"),
    ({"columns": ["label", "label"]}, "unique column names"),
    ({"columns": [""]}, "unique column names"),
    ({"columns": ["absent"]}, "Output columns do not exist"),
    ({"columns": ["_ROW_ID"]}, "system columns"),
    ({"inputs": []}, "unique column names"),
    ({"inputs": ["id", "id"]}, "unique column names"),
    ({"inputs": ["absent"]}, "Input columns do not exist"),
    ({"inputs": ["_ROW_ID"]}, "system columns"),
    ({"inputs": None}, "explicit inputs"),
    ({"fn": None}, "callable fn"),
    ({"fn": 1}, "callable fn"),
    ({"batch_size": 0}, "positive integer"),
    ({"batch_size": -1}, "positive integer"),
    ({"batch_size": True}, "positive integer"),
    ({"batch_size": 1.5}, "positive integer"),
])
def test_compute_columns_validates_before_execution(frames, kwargs, message):
    def never_called(batch):
        raise AssertionError("must not run")

    arguments = {"columns": ["label"], "inputs": ["id"], "fn": never_called}
    arguments.update(kwargs)
    schema_id = frames.raw_table.table_schema.id
    snapshot = _snapshot(frames)
    with patch.object(frames.catalog, "alter_table", side_effect=AssertionError("must not alter schema")):
        with pytest.raises(ValueError, match=message):
            frames.compute_columns(**arguments)
    assert frames.catalog.get_table(frames.identifier).table_schema.id == schema_id
    assert _snapshot(frames) == snapshot


def test_compute_columns_rejects_video_frame_outputs(tmp_path):
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = connection.create_table(
        "videos", schema=pa.schema([("id", pa.int64()), ("video", pa.large_binary())]),
        options={"file.format": "parquet", "video-frame-field": "video"})
    with pytest.raises(ValueError, match="replace_video"):
        table.compute_columns(["video"], inputs=["id"], fn=lambda batch: batch)


@pytest.mark.parametrize("method", ["merge_columns", "compute_columns"])
def test_column_updates_preserve_files_if_commit_response_is_lost(frames, method):
    commit = BatchTableCommit.commit

    def lost_response(self, messages, *args, **kwargs):
        commit(self, messages, *args, **kwargs)
        raise OSError("lost commit response")

    with patch.object(BatchTableCommit, "commit", lost_response), patch.object(
            BatchTableCommit, "abort", side_effect=AssertionError("must preserve files")):
        with pytest.raises(pmm.ColumnUpdateError) as failure:
            if method == "merge_columns":
                frames.merge_columns(pa.table({"id": [0], "label": ["committed"]}), on="id")
            else:
                frames.compute_columns(["label"], inputs=["id"], fn=lambda batch: pa.table({
                    "label": ["committed"] * batch.num_rows}))

    assert failure.value.phase == "commit"
    assert failure.value.commit_outcome == "unknown"
    assert isinstance(failure.value.__cause__, OSError)
    assert _rows(frames, ["id", "label"])[0]["label"] == "committed"


def test_compute_columns_on_fully_deleted_table_does_not_commit(frames):
    frames.delete("id >= 0")
    snapshot = _snapshot(frames)

    def never_called(batch):
        raise AssertionError("deleted rows must not be computed")

    result = frames.compute_columns(["label"], inputs=["id"], fn=never_called)
    assert result.source_snapshot_id == snapshot
    assert result.snapshot_id == snapshot
    assert result.updated_rows == 0
    assert result.added_columns == ()


def test_merge_columns_matches_composite_keys_without_materializing_payloads(frames):
    frames.add([{"id": 0, "label": "second", "image": b"another"}])
    result = frames.merge_columns(pa.table({
        "source_id": [0], "source_label": ["second"], "caption": ["matched"],
    }), on={"id": "source_id", "label": "source_label"})

    assert result.updated_rows is None
    rows = frames.scan().select(["id", "label", "caption"]).where("id = 0").to_list()
    assert sorted(rows, key=lambda row: row["label"]) == [
        {"id": 0, "label": "old", "caption": None},
        {"id": 0, "label": "second", "caption": "matched"},
    ]


def test_merge_columns_all_ignored_rows_do_not_create_a_data_snapshot(frames):
    snapshot = _snapshot(frames)
    result = frames.merge_columns(pa.table({"id": [99], "label": ["absent"]}),
                                  on="id")
    assert result.updated_rows is None
    assert result.snapshot_id == snapshot
    assert _snapshot(frames) == snapshot


def test_column_enrichment_on_an_empty_table(tmp_path):
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = connection.create_table("frames", schema=pa.schema([("id", pa.int64())]),
                                    options={"file.format": "parquet"})

    def never_called(batch):
        raise AssertionError("empty tables have no batches")

    result = table.add_columns(pa.schema([("feature", pa.int64())]),
                               inputs=["id"], fn=never_called)
    assert result.source_snapshot_id is None
    assert result.snapshot_id is None
    assert result.updated_rows == 0
    result = table.merge_columns(pa.table({"id": [1], "feature": [2]}),
                                 on="id")
    assert result.updated_rows is None
    assert result.snapshot_id is None
    result = table.compute_columns(["feature"], inputs=["id"], fn=never_called)
    assert result.source_snapshot_id is None
    assert result.snapshot_id is None
    assert result.updated_rows == 0
    assert result.added_columns == ()


@pytest.mark.parametrize("schema, kwargs", [
    (pa.schema([("label", pa.string())]), {}),
    (pa.schema([("feature", pa.int64())]), {"inputs": ["absent"], "fn": lambda batch: batch}),
    (pa.schema([pa.field("feature", pa.int64(), nullable=False)]), {}),
    (pa.schema([("_ROW_ID", pa.int64())]), {}),
    (pa.schema([("feature", pa.int64())]), {"batch_size": 0}),
])
def test_add_columns_validation_never_publishes_schema(frames, schema, kwargs):
    schema_id = frames.raw_table.table_schema.id
    with pytest.raises(ValueError):
        frames.add_columns(schema, **kwargs)
    assert frames.catalog.get_table(frames.identifier).table_schema.id == schema_id


def test_enrichment_rejects_historical_views(frames):
    historical = frames.copy({"scan.snapshot-id": str(_snapshot(frames))})
    with pytest.raises(ValueError, match="current full-table"):
        historical.add_columns(pa.schema([("feature", pa.int64())]))
    with pytest.raises(ValueError, match="current full-table"):
        historical.merge_columns(pa.table({"id": [0], "label": ["old"]}), on="id")
    with pytest.raises(ValueError, match="current full-table"):
        historical.compute_columns(["label"], inputs=["label"], fn=lambda batch: batch)


@pytest.mark.parametrize("options", [
    {"scan.mode": "from-snapshot"},
    {"incremental-between": "1,2"},
    {"scan.file-creation-time-millis": "1"},
])
def test_compute_columns_rejects_non_full_table_views(frames, options):
    with pytest.raises(ValueError, match="current full-table"):
        frames.copy(options).compute_columns(["label"], inputs=["label"], fn=lambda batch: batch)


def test_add_columns_keeps_published_schema_if_schema_response_is_lost(frames):
    alter_table = frames.catalog.alter_table

    def lost_response(*args, **kwargs):
        alter_table(*args, **kwargs)
        raise OSError("lost schema response")

    with patch.object(frames.catalog, "alter_table", lost_response):
        with pytest.raises(pmm.ColumnUpdateError) as failure:
            frames.add_columns(pa.schema([("feature", pa.int64())]))

    assert failure.value.phase == "schema"
    assert failure.value.schema_changed is None
    assert failure.value.commit_outcome == "not_started"
    assert "feature" in frames.catalog.get_table(frames.identifier).field_names


def test_schema_only_projection_retains_live_rows_and_row_id_filters(frames):
    frames.delete("id = 1")
    frames.add_columns(pa.schema([("feature", pa.int64())]))
    assert frames.scan().select(["feature"]).to_arrow()["feature"].to_pylist() == [None] * 4
    rows = frames.take_row_ids([1, 2, 3]).select(["feature"]).with_row_id().to_list()
    assert sorted(rows, key=lambda row: row["_ROW_ID"]) == [
        {"feature": None, "_ROW_ID": 2}, {"feature": None, "_ROW_ID": 3},
    ]


def test_merge_columns_can_clear_and_replace_a_partly_filled_vector(frames):
    vector_type = pa.list_(pa.float32(), 2)
    frames.merge_columns(pa.table({"id": [1], "embedding": pa.array([[1., 2.]], type=vector_type)}), on="id")
    frames.merge_columns(pa.table({"id": [1, 2], "embedding": pa.array([None, [3., 4.]], type=vector_type)}), on="id")
    assert _rows(frames, ["id", "embedding"]) == [
        {"id": i, "embedding": [3., 4.] if i == 2 else None} for i in range(5)]


@pytest.mark.parametrize("with_media", [False, True])
def test_merge_columns_inline_nullable_vectors_are_readable(tmp_path, with_media):
    from pypaimon import Schema

    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    schema = pa.schema([("id", pa.int64())] +
                       ([("image", pa.large_binary())] if with_media else []))
    connection.catalog.create_database("default", True)
    connection.catalog.create_table("default.inline", Schema.from_pyarrow_schema(schema, options={
        "file.format": "parquet", "data-evolution.enabled": "true", "row-tracking.enabled": "true",
    }), False)
    table = connection.get_table("inline")
    table.add([dict({"id": i}, **({"image": b"media"} if with_media else {})) for i in range(3)])
    vector_type = pa.list_(pa.float32(), 2)
    table.merge_columns(pa.table({"id": [1], "embedding": pa.array([[1., 2.]], type=vector_type)}), on="id")
    table.merge_columns(pa.table({"id": [1, 2], "embedding": pa.array([None, [3., 4.]], type=vector_type)}), on="id")

    assert _rows(table, ["id", "embedding"]) == [
        {"id": i, "embedding": [3., 4.] if i == 2 else None} for i in range(3)]


def test_merge_columns_keeps_the_callers_table_handle(frames):
    raw_table = frames.raw_table
    frames.merge_columns(pa.table({"id": [1], "label": ["new"]}), on="id")

    assert frames.raw_table is raw_table
    assert frames.table is raw_table
    assert _rows(frames, ["id", "label"])[1] == {"id": 1, "label": "new"}


def test_merge_columns_empty_source_can_add_schema_without_a_data_snapshot(frames):
    snapshot = _snapshot(frames)
    result = frames.merge_columns(
        pa.table({"id": pa.array([], type=pa.int64()), "caption": pa.array([], type=pa.string())}),
        on="id")

    assert result.updated_rows is None
    assert result.added_columns == ("caption",)
    assert result.snapshot_id == snapshot
    assert frames.scan().select(["caption"]).to_arrow()["caption"].to_pylist() == [None] * 5


@pytest.mark.native_plan
@pytest.mark.parametrize("native_read", [False, True])
def test_column_enrichment_uses_native_merge_and_shard_without_rewriting_media(frames, native_read):
    table = frames.copy({"write.native.enabled": "true",
                         "read.native.enabled": str(native_read).lower()})
    blobs = _blobs(table)
    snapshot = _snapshot(table)
    vector_type = pa.list_(pa.float32(), 2)
    with patch("pypaimon.execution.python.write.merge_into._build_tables",
               side_effect=AssertionError("must use native MERGE")):
        result = table.merge_columns(pa.table({
            "id": [3, 99], "embedding": pa.array([[3., 4.], [99., 100.]], type=vector_type),
        }), on="id")
    assert result.updated_rows is None
    assert result.added_columns == ("embedding",)
    assert _rows(table, ["id", "embedding"]) == [
        {"id": i, "embedding": [3., 4.] if i == 3 else None} for i in range(5)]

    with patch("pypaimon.execution.python.write.table_update.ShardTableUpdator.arrow_reader",
               side_effect=AssertionError("must use native shard")):
        result = table.add_columns(
            pa.schema([("image_size", pa.int64())]), inputs=["image"],
            fn=lambda batch: pa.table({"image_size": [len(value) for value in batch["image"].to_pylist()]}),
            batch_size=2)
        assert result.updated_rows == 5
        assert _rows(table, ["id", "image_size"]) == [{"id": i, "image_size": 100} for i in range(5)]
        result = table.compute_columns(
            ["image_size"], inputs=["image"],
            fn=lambda batch: pa.table({"image_size": [len(value) + 1 for value in batch["image"].to_pylist()]}),
            batch_size=2)
    assert result.updated_rows == 5
    assert result.added_columns == ()
    assert _rows(table, ["id", "image_size"]) == [{"id": i, "image_size": 101} for i in range(5)]
    assert _blobs(table) == blobs
    assert table.scan(snapshot_id=snapshot).select(["id", "label"]).to_arrow().num_rows == 5


@pytest.mark.native_plan
def test_native_merge_columns_uses_standard_merge_cardinality(frames):
    frames.add([{"id": 1, "label": "duplicate", "image": b"other"}])
    table = frames.copy({"write.native.enabled": "true"})
    with patch("pypaimon.execution.python.write.merge_into._build_tables",
               side_effect=AssertionError("must use native MERGE")):
        table.merge_columns(pa.table({
            "id": pa.array([1, 99, 99, None], type=pa.int64()),
            "label": ["new", "missing", "duplicate unmatched", "null"],
        }), on="id")
        assert table.scan().where("id = 1").select(["label"]).to_list() == [
            {"label": "new"}, {"label": "new"}]
        snapshot = _snapshot(table)
        with pytest.raises(pmm.ColumnUpdateError) as failure:
            table.merge_columns(pa.table({"id": [1, 1], "label": ["first", "second"]}), on="id")
    assert "multiple source rows" in str(failure.value.__cause__)
    assert failure.value.commit_outcome == "not_started"
    assert _snapshot(table) == snapshot
