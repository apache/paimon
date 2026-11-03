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
        options={"file.format": "parquet", "read.batch-size": "2"},
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
    assert result.updated_rows == 2
    assert result.unmatched_rows == 0
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


@pytest.mark.parametrize("ids, message", [
    ([1, 1], "duplicate"), ([None], "null"), ([99], "unmatched"),
])
def test_merge_columns_rejects_bad_keys_before_adding_schema(frames, ids, message):
    snapshot = _snapshot(frames)
    with pytest.raises(ValueError, match=message):
        frames.merge_columns(pa.table({
            "id": pa.array(ids, type=pa.int64()), "new_label": ["new"] * len(ids),
        }), on="id")

    assert _snapshot(frames) == snapshot
    assert "new_label" not in frames.catalog.get_table(frames.identifier).field_names


def test_merge_columns_counts_ignored_keys_and_supports_key_mapping(frames):
    result = frames.merge_columns(
        pa.table({"source_id": [2, 99], "label": ["new", "missing"]}),
        on={"id": "source_id"}, unmatched="ignore")

    assert (result.updated_rows, result.unmatched_rows) == (1, 1)
    assert _rows(frames, ["id", "label"])[2] == {"id": 2, "label": "new"}
    assert len(_rows(frames, ["id"])) == 5


def test_merge_columns_rejects_ambiguous_target_keys(frames):
    frames.add([{"id": 1, "label": "duplicate", "image": b"other"}])
    snapshot = _snapshot(frames)
    with pytest.raises(ValueError, match="multiple target rows"):
        frames.merge_columns(pa.table({"id": [1], "label": ["new"]}), on="id")
    assert _snapshot(frames) == snapshot


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


def test_merge_columns_preserves_files_if_commit_response_is_lost(frames):
    commit = BatchTableCommit.commit

    def lost_response(self, messages, *args, **kwargs):
        commit(self, messages, *args, **kwargs)
        raise OSError("lost commit response")

    with patch.object(BatchTableCommit, "commit", lost_response), patch.object(
            BatchTableCommit, "abort", side_effect=AssertionError("must preserve files")):
        with pytest.raises(pmm.ColumnUpdateError) as failure:
            frames.merge_columns(pa.table({"id": [0], "label": ["committed"]}), on="id")

    assert failure.value.phase == "commit"
    assert failure.value.commit_outcome == "unknown"
    assert isinstance(failure.value.__cause__, OSError)
    assert _rows(frames, ["id", "label"])[0]["label"] == "committed"


def test_merge_columns_matches_composite_keys_without_materializing_payloads(frames):
    frames.add([{"id": 0, "label": "second", "image": b"another"}])
    result = frames.merge_columns(pa.table({
        "source_id": [0], "source_label": ["second"], "caption": ["matched"],
    }), on={"id": "source_id", "label": "source_label"})

    assert result.updated_rows == 1
    rows = frames.scan().select(["id", "label", "caption"]).where("id = 0").to_list()
    assert sorted(rows, key=lambda row: row["label"]) == [
        {"id": 0, "label": "old", "caption": None},
        {"id": 0, "label": "second", "caption": "matched"},
    ]


def test_merge_columns_all_ignored_rows_do_not_create_a_data_snapshot(frames):
    snapshot = _snapshot(frames)
    result = frames.merge_columns(pa.table({"id": [99], "label": ["absent"]}),
                                  on="id", unmatched="ignore")
    assert (result.updated_rows, result.unmatched_rows) == (0, 1)
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
                                 on="id", unmatched="ignore")
    assert (result.updated_rows, result.unmatched_rows) == (0, 1)
    assert result.snapshot_id is None


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


@pytest.mark.parametrize("key_type", [pa.float32(), pa.float64()])
def test_merge_columns_treats_signed_zero_as_the_same_key(tmp_path, key_type):
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = connection.create_table(
        "zeros", schema=pa.schema([("id", key_type), ("label", pa.string())]),
        data=[{"id": 0., "label": "old"}], options={"file.format": "parquet"})
    table.merge_columns(pa.table({"id": pa.array([-0.], type=key_type), "label": ["new"]}), on="id")
    assert table.scan().select(["label"]).to_list() == [{"label": "new"}]

    with pytest.raises(ValueError, match="duplicate"):
        table.merge_columns(pa.table({"id": pa.array([0., -0.], type=key_type),
                                      "label": ["a", "b"]}), on="id")
    table.add([{"id": -0., "label": "second"}])
    with pytest.raises(ValueError, match="multiple target rows"):
        table.merge_columns(pa.table({"id": pa.array([0.], type=key_type), "label": ["a"]}), on="id")


def test_merge_columns_keeps_the_callers_table_handle(frames):
    raw_table = frames.raw_table
    frames.merge_columns(pa.table({"id": [1], "label": ["new"]}), on="id")

    assert frames.raw_table is raw_table
    assert frames.table is raw_table
    assert _rows(frames, ["id", "label"])[1] == {"id": 1, "label": "new"}


def test_merge_columns_rejects_a_lossy_match_key_cast_before_publication(tmp_path):
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = connection.create_table(
        "float_keys", schema=pa.schema([("id", pa.float32()), ("label", pa.string())]),
        data=[{"id": 1., "label": "old"}], options={"file.format": "parquet"})
    snapshot = _snapshot(table)
    schema_id = table.raw_table.table_schema.id
    with pytest.raises(ValueError, match="loss"):
        table.merge_columns(pa.table({
            "id": pa.array([1.0000000001], type=pa.float64()),
            "label": ["wrong"], "feature": [42],
        }), on="id")

    assert _snapshot(table) == snapshot
    assert table.catalog.get_table(table.identifier).table_schema.id == schema_id
    assert table.scan().select(["label"]).to_list() == [{"label": "old"}]
    table.merge_columns(pa.table({"id": pa.array([1.], type=pa.float64()), "label": ["exact"]}), on="id")
    assert table.scan().select(["label"]).to_list() == [{"label": "exact"}]
