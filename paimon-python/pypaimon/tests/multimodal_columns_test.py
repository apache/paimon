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


@pytest.mark.python_plan
@pytest.mark.python_write
@pytest.mark.parametrize("rolled_vectors", [False, True], ids=["aligned-input", "rolled-input"])
def test_compute_columns_preserves_vector_file_groups(tmp_path, rolled_vectors):
    vector_type = pa.list_(pa.float32(), 2)
    connection = pmm.connect(options={"warehouse": str(tmp_path)})
    table = connection.create_table(
        "frames",
        schema=pa.schema([("id", pa.int64()), ("embedding", vector_type)]),
        options={"file.format": "parquet", "vector.file.format": "parquet",
                 "vector.target-file-size": "1 b", "deletion-vectors.enabled": "true"},
    )
    append_table = table.copy({"vector.target-file-size": "16 b" if rolled_vectors else "1 mb"})
    for start in (0, 6):
        rows = [{"id": i, "embedding": [float(i), -1.]} for i in range(start, start + 6)]
        append_table.add_batches(rows[offset:offset + 2] for offset in range(0, 6, 2))

    table.delete("id = 3")
    source = _snapshot(table)
    files = _files(table)
    assert sorted((file.first_row_id, file.row_count) for file in files.values()
                  if not DataFileMeta.is_vector_file(file.file_name)) == [(0, 6), (6, 6)]
    expected_vector_ranges = [(i, 2) for i in range(0, 12, 2)] if rolled_vectors else [(0, 6), (6, 6)]
    assert sorted((file.first_row_id, file.row_count) for file in files.values()
                  if DataFileMeta.is_vector_file(file.file_name)) == expected_vector_ranges
    visible_ids = [i for i in range(12) if i != 3]
    assert _rows(table, ["id", "embedding"]) == [
        {"id": i, "embedding": [float(i), -1.]} for i in visible_ids]

    def recompute(batch):
        return pa.table({"embedding": pa.array(
            [[float(i), 1.] for i in batch["id"].to_pylist()], type=vector_type)})

    result = table.compute_columns("embedding", inputs=["id"], fn=recompute, batch_size=2)

    assert result.updated_rows == len(visible_ids)
    assert result.snapshot_id == source + 1
    assert _rows(table, ["id", "embedding"]) == [
        {"id": i, "embedding": [float(i), 1.]} for i in visible_ids]
    current_files = _files(table)
    assert set(files).issubset(current_files)
    new_files = [file for name, file in current_files.items() if name not in files]
    assert all(DataFileMeta.is_vector_file(file.file_name) for file in new_files)
    assert sorted((file.first_row_id, file.row_count) for file in new_files) == [(0, 6), (6, 6)]


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


@pytest.mark.parametrize("as_table", [False, True])
@pytest.mark.parametrize("canonical", [False, True])
def test_compute_columns_preserves_hidden_null_children(tmp_path, as_table, canonical):
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    data_type = pa.struct([pa.field("x", pa.int64(), nullable=False)])
    schema = pa.schema([("id", pa.int64()), ("value", data_type)])
    table = connection.create_table(
        "nested", schema=schema,
        data=pa.table({"id": [0, 1], "value": [{"x": 2}, None]}, schema=schema),
        options={"file.format": "parquet"})
    snapshot = _snapshot(table)

    def compute(batch):
        ids = batch["id"].to_pylist()
        array = pa.StructArray.from_arrays(
            [pa.array([2 if value == 0 else None for value in ids], type=pa.int64())],
            fields=[pa.field("x", pa.int64(), nullable=not canonical)],
            mask=pa.array([value != 0 for value in ids]))
        array.validate(full=True)
        factory = pa.Table if as_table else pa.RecordBatch
        return factory.from_arrays([array], names=["value"])

    result = table.compute_columns(["value"], inputs=["id"], fn=compute)
    assert result.source_snapshot_id == snapshot
    assert result.snapshot_id > snapshot
    assert result.updated_rows == 2
    assert result.added_columns == ()
    assert _rows(table, ["id", "value"]) == [{"id": 0, "value": {"x": 2}}, {"id": 1, "value": None}]


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


@pytest.mark.parametrize("method", ["merge_columns", "compute_columns", "update_columns_by_row_id"])
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
            elif method == "update_columns_by_row_id":
                frames.update_columns_by_row_id(pa.table({"_ROW_ID": [0], "label": ["committed"]}), columns=["label"])
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
    with pytest.raises(ValueError, match="current full-table"):
        historical.update_columns_by_row_id(pa.table({"_ROW_ID": [0], "label": ["old"]}), columns=["label"])


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


@pytest.mark.parametrize("id_type", [pa.int8(), pa.int64(), pa.uint64()])
def test_update_columns_by_row_id_matches_duplicate_keys_without_rewriting_media(frames, id_type):
    frames.add([{"id": 1, "label": "duplicate", "image": b"other"}])
    raw = frames.raw_table
    snapshot = _snapshot(frames)
    schema_id = raw.table_schema.id
    files, blobs = set(_files(frames)), _blobs(frames)
    data = pa.table({"label": ["second", "first"], "_ROW_ID": pa.chunked_array(
        [[5], [1]], type=id_type)})
    with patch.object(raw.file_io, "read_ranges_coalesced",
                      side_effect=AssertionError("must not fetch media")):
        result = frames.update_columns_by_row_id(data, columns="label")

    assert isinstance(result, pmm.ColumnUpdateResult)
    assert result.source_snapshot_id == snapshot
    assert result.snapshot_id == snapshot + 1
    assert result.schema_id == schema_id
    assert result.updated_rows == 2
    assert result.added_columns == ()
    assert frames.raw_table is raw
    actual = frames.scan().select(["id", "label"]).with_row_id().to_arrow().sort_by("_ROW_ID")
    assert actual["id"].to_pylist() == [0, 1, 2, 3, 4, 1]
    assert actual["label"].to_pylist() == ["old", "first", "old", "old", "old", "second"]
    assert _blobs(frames) == blobs
    assert files.issubset(_files(frames))
    assert all(file.write_cols == ["label"] for name, file in _files(frames).items() if name not in files)
    assert frames.scan(snapshot_id=snapshot).select(["label"]).to_arrow()["label"].to_pylist() == [
        "old", "old", "old", "old", "old", "duplicate"]


def test_update_columns_by_row_id_only_writes_explicit_columns(frames):
    frames.add_columns(pa.schema([("score", pa.int64())]))
    frames.update("id = 1", {"label": "current"})
    files, blobs = set(_files(frames)), _blobs(frames)
    payload = pa.table({
        "_ROW_ID": [1], "id": [999], "label": ["old"],
        "image": pa.array([b"stale media"], type=pa.large_binary()), "score": [42], "unused": ["ignored"]})
    with patch.object(frames.raw_table.file_io, "read_ranges_coalesced",
                      side_effect=AssertionError("must not fetch media")):
        result = frames.update_columns_by_row_id(payload, columns=["score"])
    assert result.updated_rows == 1
    assert result.added_columns == ()
    assert _rows(frames, ["id", "label", "score"]) == [
        {"id": i, "label": "current" if i == 1 else "old", "score": 42 if i == 1 else None} for i in range(5)]
    assert _blobs(frames) == blobs
    assert all(file.write_cols == ["score"] for name, file in _files(frames).items() if name not in files)


def test_update_columns_by_row_id_requires_explicit_columns(frames):
    snapshot = _snapshot(frames)
    with pytest.raises(TypeError, match="columns"):
        frames.update_columns_by_row_id(pa.table({"_ROW_ID": [0], "label": ["new"]}))
    assert _snapshot(frames) == snapshot


@pytest.mark.parametrize("row_count", [0, 1])
@pytest.mark.parametrize("method", ["batch", "predicate"])
def test_row_id_updates_reject_stale_schema_before_staging(frames, row_count, method):
    from pypaimon.schema.data_types import AtomicType
    from pypaimon.schema.schema_change import SchemaChange

    connection = pmm.MultimodalConnection(frames.catalog)
    table = connection.get_table(frames.identifier)
    raw = table.raw_table
    snapshot, files = _snapshot(table), set(_files(table))
    table.catalog.alter_table(table.identifier, [
        SchemaChange.drop_column("label"), SchemaChange.add_column("label", AtomicType("STRING"))])
    current = connection.get_table(table.identifier)
    assert current.raw_table.field_dict["label"].id != raw.field_dict["label"].id
    assert _snapshot(current) == snapshot
    assert current.raw_table.snapshot_manager().get_latest_snapshot().schema_id == raw.table_schema.id
    data = pa.table({"_ROW_ID": pa.array([0][:row_count], type=pa.int64()),
                     "label": pa.array(["new"][:row_count], type=pa.string())})
    with patch("pypaimon.table.file_store_table.FileStoreTable.new_batch_write_builder",
               side_effect=AssertionError("must not stage files")):
        with pytest.raises(ValueError, match="schema has changed.*Reopen"):
            if method == "batch":
                table.update_columns_by_row_id(data, columns=["label"])
            else:
                table.update("_ROW_ID = %s" % (0 if row_count else 999999), {"label": "new"})
    assert table.raw_table is raw
    assert _snapshot(current) == snapshot
    assert set(_files(current)) == files
    assert current.scan().select(["label"]).to_list() == [{"label": None}] * 5

    if method == "batch":
        result = current.update_columns_by_row_id(
            pa.table({"_ROW_ID": [0], "label": ["new"]}), columns=["label"])
        assert result.updated_rows == 1
        assert result.schema_id == current.raw_table.table_schema.id
    else:
        assert current.update("_ROW_ID = 0", {"label": "new"}) is current
    assert _rows(connection.get_table(table.identifier), ["id", "label"]) == [
        {"id": i, "label": "new" if i == 0 else None} for i in range(5)]


@pytest.mark.parametrize("method", ["batch", "predicate"])
def test_row_id_updates_do_not_write_if_schema_lookup_fails(frames, method):
    snapshot, files = _snapshot(frames), set(_files(frames))
    with patch.object(frames.catalog, "get_table", side_effect=OSError("schema lookup failed")):
        with patch("pypaimon.table.file_store_table.FileStoreTable.new_batch_write_builder",
                   side_effect=AssertionError("must not stage files")):
            with pytest.raises(OSError, match="schema lookup failed"):
                if method == "batch":
                    frames.update_columns_by_row_id(pa.table({"_ROW_ID": [0], "label": ["new"]}), columns=["label"])
                else:
                    frames.update("id = 1 OR (_ROW_ID = 0 AND label = 'old')", {"label": "new"})
    assert _snapshot(frames) == snapshot
    assert set(_files(frames)) == files


def test_update_columns_by_row_id_checks_current_branch_schema_and_keeps_dynamic_options(frames):
    from pypaimon.schema.data_types import AtomicType
    from pypaimon.schema.schema_change import SchemaChange

    connection = pmm.MultimodalConnection(frames.catalog)
    frames.catalog.create_tag(frames.identifier, "base", _snapshot(frames))
    frames.catalog.create_branch(frames.identifier, "dev", tag_name="base")
    frames.catalog.alter_table(frames.identifier, [SchemaChange.add_column("score", AtomicType("INT"))])
    table = connection.get_table(frames.identifier + "$branch_dev").copy({"write.native.enabled": "false"})
    raw = table.raw_table
    assert raw.table_schema.id != connection.get_table(frames.identifier).raw_table.table_schema.id
    result = table.update_columns_by_row_id(pa.table({"_ROW_ID": [0], "label": ["branch"]}), columns=["label"])
    assert result.updated_rows == 1
    assert result.schema_id == raw.table_schema.id
    assert table.raw_table is raw
    assert not raw.options.native_write_enabled()
    assert _rows(connection.get_table(table.identifier), ["id", "label"]) == [
        {"id": i, "label": "branch" if i == 0 else "old"} for i in range(5)]
    assert connection.get_table(frames.identifier).scan().select(["label"]).to_list() == [{"label": "old"}] * 5


@pytest.mark.parametrize("method", ["batch", "predicate"])
def test_row_id_updates_reject_branch_copy_with_same_schema_id_and_different_field_ids(frames, method):
    from pypaimon.schema.data_types import AtomicType
    from pypaimon.schema.schema_change import Move, SchemaChange

    connection = pmm.MultimodalConnection(frames.catalog)
    frames.catalog.create_tag(frames.identifier, "base", _snapshot(frames))
    frames.catalog.create_branch(frames.identifier, "dev", tag_name="base")
    frames.catalog.alter_table(frames.identifier, [SchemaChange.set_option("read.batch-size", "3")])
    identifier = frames.identifier + "$branch_dev"
    frames.catalog.alter_table(identifier, [
        SchemaChange.drop_column("label"),
        SchemaChange.add_column("label", AtomicType("STRING"), move=Move.before("label", "image"))])
    copied = connection.get_table(frames.identifier).copy({"branch": "dev"})
    current = connection.get_table(identifier)
    assert copied.raw_table.table_schema.id == current.raw_table.table_schema.id
    assert copied.raw_table.field_names == current.raw_table.field_names
    assert [field.type for field in copied.raw_table.fields] == [field.type for field in current.raw_table.fields]
    assert copied.raw_table.field_dict["label"].id != current.raw_table.field_dict["label"].id
    snapshot, files = _snapshot(current), set(_files(current))
    with patch("pypaimon.table.file_store_table.FileStoreTable.new_batch_write_builder",
               side_effect=AssertionError("must not stage files")):
        with pytest.raises(ValueError, match="schema has changed.*Reopen"):
            if method == "batch":
                copied.update_columns_by_row_id(pa.table({"_ROW_ID": [0], "label": ["new"]}), columns=["label"])
            else:
                copied.update("_ROW_ID = 0", {"label": "new"})
    assert _snapshot(current) == snapshot
    assert set(_files(current)) == files
    assert current.scan().select(["label"]).to_list() == [{"label": None}] * 5
    if method == "batch":
        current.update_columns_by_row_id(pa.table({"_ROW_ID": [0], "label": ["new"]}), columns=["label"])
    else:
        current.update("_ROW_ID = 0", {"label": "new"})
    assert _rows(connection.get_table(identifier), ["id", "label"]) == [
        {"id": i, "label": "new" if i == 0 else None} for i in range(5)]


def test_business_predicate_with_row_id_string_literal_does_not_check_schema(frames):
    snapshot = _snapshot(frames)
    with patch.object(frames.catalog, "get_table", side_effect=AssertionError("must not check schema")):
        assert frames.update("label = '_ROW_ID'", {"label": "new"}) is frames
    assert _snapshot(frames) == snapshot


@pytest.mark.parametrize("row_order", [
    pytest.param([0, 1, 2, 3, 4, 5], id="a-first"),
    pytest.param([3, 4, 5, 0, 1, 2], id="b-first"),
])
def test_update_columns_by_row_id_preserves_deletions_and_crosses_partitions(tmp_path, row_order):
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = connection.create_table(
        "partitioned", schema=pa.schema([("part", pa.string()), ("id", pa.int64()), ("label", pa.string())]),
        partitioned=["part"],
        data=[{"part": "a" if i < 3 else "b", "id": i, "label": "old"} for i in row_order],
        options={"file.format": "parquet", "deletion-vectors.enabled": "true"})
    table.delete("id = 1 OR id = 4")
    row_ids = {row["id"]: row["_ROW_ID"] for row in table.scan().select(["id"]).with_row_id().to_list()}
    snapshot = _snapshot(table)
    commit = BatchTableCommit.commit
    with patch.object(BatchTableCommit, "commit", autospec=True, side_effect=commit) as publish:
        result = table.update_columns_by_row_id(pa.table({
            "_ROW_ID": [row_ids[5], row_ids[0]], "label": ["last", None]}), columns=["label"])
    publish.assert_called_once()
    messages = publish.call_args[0][1]
    assert len(messages) == 2
    assert all(message.check_from_snapshot == snapshot for message in messages)
    assert result.source_snapshot_id == snapshot
    assert result.snapshot_id == snapshot + 1
    assert result.updated_rows == 2
    assert _rows(table, ["id", "label"]) == [
        {"id": 0, "label": None}, {"id": 2, "label": "old"},
        {"id": 3, "label": "old"}, {"id": 5, "label": "last"}]


@pytest.mark.parametrize("ids, error, message", [
    (pa.array([1.0]), TypeError, "integer Arrow"),
    (pa.array([1.5]), TypeError, "integer Arrow"),
    (pa.array([True]), TypeError, "integer Arrow"),
    (pa.array(["1"]), TypeError, "integer Arrow"),
    (pa.nulls(1), TypeError, "integer Arrow"),
    (pa.array([None], type=pa.int64()), ValueError, "must not be null"),
    (pa.chunked_array([[1], [1]], type=pa.int64()), ValueError, "duplicate _ROW_ID"),
    (pa.array([-1]), ValueError, "non-negative"),
    (pa.array([2 ** 63], type=pa.uint64()), ValueError, "range"),
])
def test_update_columns_by_row_id_rejects_invalid_ids_before_writing(frames, ids, error, message):
    from pypaimon.write.table_update import BatchTableUpdate

    snapshot, files = _snapshot(frames), set(_files(frames))
    with patch.object(BatchTableUpdate, "update_by_arrow_with_row_id",
                      side_effect=AssertionError("must not write")):
        with pytest.raises(error, match=message):
            frames.update_columns_by_row_id(pa.table({"_ROW_ID": ids, "label": ["new"] * len(ids)}), columns=["label"])
    assert _snapshot(frames) == snapshot
    assert set(_files(frames)) == files


@pytest.mark.parametrize("data, columns, message", [
    ([{"_ROW_ID": 0, "label": "new"}], ["label"], "pyarrow.Table"),
    (pa.record_batch([pa.array([0]), pa.array(["new"])], names=["_ROW_ID", "label"]), ["label"], "pyarrow.Table"),
    (pa.table({"label": ["new"]}), ["label"], "exactly one _ROW_ID"),
    (pa.table({"_ROW_ID": [0]}), [], "unique column names"),
    (pa.table({"_ROW_ID": [0], "absent": ["new"]}), ["absent"], "Output columns do not exist"),
    (pa.table({"_ROW_ID": [0], "_SEQUENCE_NUMBER": [1]}), ["_SEQUENCE_NUMBER"], "system columns"),
    (pa.table({"_ROW_ID": [0], "label": [123]}), ["label"], "Incompatible type"),
    (pa.table({"_ROW_ID": [0], "label": ["new"]}), ["id"], "exactly one id column"),
    (pa.table({"_ROW_ID": [0], "label": ["new"]}), ["label", "label"], "unique column names"),
    (pa.table({"_ROW_ID": [0], "label": ["new"]}), ["_ROW_ID"], "system columns"),
    (pa.table([pa.array([0]), pa.array([0]), pa.array(["new"])],
              names=["_ROW_ID", "_ROW_ID", "label"]), ["label"], "exactly one _ROW_ID"),
    (pa.table([pa.array([0]), pa.array(["a"]), pa.array(["b"])],
              names=["_ROW_ID", "label", "label"]), ["label"], "exactly one label column"),
])
def test_update_columns_by_row_id_validates_input_without_schema_changes(frames, data, columns, message):
    snapshot, schema_id = _snapshot(frames), frames.raw_table.table_schema.id
    with patch.object(frames.catalog, "alter_table", side_effect=AssertionError("must not alter schema")):
        with pytest.raises(ValueError, match=message):
            frames.update_columns_by_row_id(data, columns=columns)
    assert _snapshot(frames) == snapshot
    assert frames.catalog.get_table(frames.identifier).table_schema.id == schema_id


@pytest.mark.parametrize("options, message", [
    ({"data-evolution.enabled": "false"}, "Data Evolution"),
    ({"row-tracking.enabled": "false"}, "row tracking"),
    ({"scan.mode": "from-snapshot"}, "current full-table"),
    ({"incremental-between": "1,2"}, "current full-table"),
    ({"scan.file-creation-time-millis": "1"}, "current full-table"),
])
def test_update_columns_by_row_id_rejects_unsupported_views(frames, options, message):
    snapshot = _snapshot(frames)
    with pytest.raises(ValueError, match=message):
        frames.copy(options).update_columns_by_row_id(pa.table({"_ROW_ID": [0], "label": ["new"]}), columns=["label"])
    assert _snapshot(frames) == snapshot


def test_update_columns_by_row_id_rejects_partition_and_video_outputs(tmp_path):
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = connection.create_table(
        "videos", schema=pa.schema([("part", pa.string()), ("video", pa.large_binary())]),
        partitioned=["part"], options={"video-frame-field": "video"})
    with pytest.raises(ValueError, match="partition columns"):
        table.update_columns_by_row_id(pa.table({"_ROW_ID": [0], "part": ["new"]}), columns=["part"])
    with pytest.raises(ValueError, match="replace_video"):
        table.update_columns_by_row_id(pa.table({"_ROW_ID": [0], "video": [b"new"]}), columns=["video"])
    assert table.raw_table.snapshot_manager().get_latest_snapshot() is None


def test_update_columns_by_row_id_enforces_existing_nullability(tmp_path):
    connection = pmm.connect(options={"warehouse": str(tmp_path / "warehouse")})
    table = connection.create_table(
        "required", schema=pa.schema([pa.field("value", pa.int64(), nullable=False)]),
        data=[{"value": 1}], options={"file.format": "parquet"})
    snapshot = _snapshot(table)
    with pytest.raises(ValueError, match="non-null column value"):
        table.update_columns_by_row_id(pa.table({"_ROW_ID": [0], "value": [None]}), columns=["value"])
    assert _snapshot(table) == snapshot
    assert table.scan().to_list() == [{"value": 1}]


def test_update_columns_by_row_id_backfills_vectors_and_replaces_blobs(frames):
    vector_type = pa.list_(pa.float32(), 2)
    frames.add_columns(pa.schema([("embedding", vector_type)]))
    blobs = _blobs(frames)
    result = frames.update_columns_by_row_id(pa.table({
        "_ROW_ID": [3, 1], "embedding": pa.array([[3., 4.], [1., 2.]], type=vector_type)}), columns=["embedding"])
    assert result.added_columns == ()
    assert result.updated_rows == 2
    assert _blobs(frames) == blobs
    frames.update_columns_by_row_id(pa.table({
        "_ROW_ID": [1, 3], "image": pa.array([b"replacement", None], type=pa.large_binary())}), columns=["image"])
    assert _rows(frames, ["id", "embedding"]) == [
        {"id": i, "embedding": [float(i), float(i + 1)] if i in (1, 3) else None} for i in range(5)]
    _, actual = frames.scan().select(["id", "image"]).read_blobs()
    assert actual["image"] == [bytes([0]) * 100, b"replacement", bytes([2]) * 100, None, bytes([4]) * 100]


@pytest.mark.parametrize("invalid, deleted", [(99, False), (1, True), (3, True)])
def test_update_columns_by_row_id_rejects_all_targets_before_writing(frames, invalid, deleted):
    from pypaimon.write.table_update import BatchTableUpdate

    if deleted:
        frames.delete("id = %s" % invalid)
        # Validate against a group that already has a partial-column version.
        frames.merge_columns(pa.table({"id": [2], "label": ["prior"]}), on="id")
    snapshot, files = _snapshot(frames), set(_files(frames))
    before = _rows(frames, ["id", "label"])
    with patch.object(BatchTableUpdate, "update_by_arrow_with_row_id",
                      side_effect=AssertionError("must not write")):
        with pytest.raises(pmm.ColumnUpdateError) as failure:
            frames.update_columns_by_row_id(
                pa.table({"_ROW_ID": [0, invalid], "label": ["new", "bad"]}), columns=["label"])
    assert failure.value.phase == "validate"
    assert failure.value.schema_changed is False
    assert failure.value.commit_outcome == "not_started"
    assert isinstance(failure.value.__cause__, ValueError)
    assert "not found or deleted: [%s]" % invalid in str(failure.value.__cause__)
    assert _snapshot(frames) == snapshot
    assert set(_files(frames)) == files
    assert _rows(frames, ["id", "label"]) == before


@pytest.mark.parametrize("empty_table", [False, True])
def test_update_columns_by_row_id_empty_input_does_not_commit(frames, tmp_path, empty_table):
    table = frames
    if empty_table:
        connection = pmm.connect(options={"warehouse": str(tmp_path / "empty-warehouse")})
        table = connection.create_table("empty", schema=pa.schema([("label", pa.string())]))
    snapshot = table.raw_table.snapshot_manager().get_latest_snapshot()
    with patch.object(BatchTableCommit, "commit", side_effect=AssertionError("must not commit")):
        result = table.update_columns_by_row_id(pa.table({
            "_ROW_ID": pa.array([], type=pa.int64()), "label": pa.array([], type=pa.string())}), columns=["label"])
    assert result.source_snapshot_id == (None if snapshot is None else snapshot.id)
    assert result.snapshot_id == result.source_snapshot_id
    assert result.updated_rows == 0
    assert result.added_columns == ()


@pytest.mark.parametrize("append_after_capture", [False, True])
def test_update_columns_by_row_id_rejects_ids_in_an_empty_source_snapshot(tmp_path, append_after_capture):
    from pypaimon.multimodal.columns import _ColumnUpdate

    table = pmm.connect(options={"warehouse": str(tmp_path)}).create_table(
        "empty", schema=pa.schema([("label", pa.string())]))
    enter = _ColumnUpdate.__enter__

    def maybe_append(operation):
        if append_after_capture:
            table.add([{"label": "appended"}])
        return enter(operation)

    with patch.object(_ColumnUpdate, "__enter__", maybe_append):
        with pytest.raises(pmm.ColumnUpdateError) as failure:
            table.update_columns_by_row_id(pa.table({"_ROW_ID": [0], "label": ["new"]}), columns=["label"])
    assert failure.value.source_snapshot_id is None
    assert failure.value.phase == "validate"
    assert "not found or deleted: [0]" in str(failure.value.__cause__)
    assert table.scan().to_list() == ([{"label": "appended"}] if append_after_capture else [])


def test_update_columns_by_row_id_pins_validation_and_writing_to_one_snapshot(frames):
    from pypaimon.multimodal.columns import _ColumnUpdate
    from pypaimon.write.table_update import BatchTableUpdate

    snapshot = _snapshot(frames)
    enter, update = _ColumnUpdate.__enter__, BatchTableUpdate.update_by_arrow_with_row_id
    write_snapshots = []

    def append_after_capture(operation):
        frames.add([{"id": 5, "label": "appended", "image": b"new"}])
        return enter(operation)

    def capture_write(self, data):
        write_snapshots.append(self.table._read_snapshot.id)
        messages = update(self, data)
        assert all(message.check_from_snapshot == snapshot for message in messages)
        return messages

    with patch.object(_ColumnUpdate, "__enter__", append_after_capture), patch.object(
            BatchTableUpdate, "update_by_arrow_with_row_id", capture_write):
        result = frames.update_columns_by_row_id(pa.table({"_ROW_ID": [0], "label": ["new"]}), columns=["label"])
    assert result.source_snapshot_id == snapshot
    assert write_snapshots == [snapshot]
    assert _rows(frames, ["id", "label"])[-1] == {"id": 5, "label": "appended"}

    with patch.object(_ColumnUpdate, "__enter__", append_after_capture):
        with pytest.raises(pmm.ColumnUpdateError, match="validate"):
            frames.update_columns_by_row_id(pa.table({"_ROW_ID": [6], "label": ["not in snapshot"]}), columns=["label"])


@pytest.mark.python_write
def test_update_columns_by_row_id_late_write_failure_does_not_publish(frames):
    from pypaimon.execution.python.write.table_update_by_row_id import PythonTableUpdateByRowId

    frames = frames.copy({"write.native.enabled": "false"})
    frames.add([{"id": 5, "label": "old", "image": b"other"}])
    snapshot = _snapshot(frames)
    write_group = PythonTableUpdateByRowId._write_group
    written = []

    def fail_second(self, *args, **kwargs):
        if written:
            raise OSError("second group failed")
        result = write_group(self, *args, **kwargs)
        written.append(args[1])
        return result

    with patch.object(PythonTableUpdateByRowId, "_write_group", fail_second):
        with pytest.raises(pmm.ColumnUpdateError) as failure:
            frames.update_columns_by_row_id(
                pa.table({"_ROW_ID": [0, 5], "label": ["first", "second"]}), columns=["label"])
    assert len(written) == 1
    assert failure.value.phase == "write"
    assert failure.value.commit_outcome == "not_started"
    assert _snapshot(frames) == snapshot
    assert all(row["label"] == "old" for row in _rows(frames, ["id", "label"]))
    result = frames.update_columns_by_row_id(
        pa.table({"_ROW_ID": [0, 5], "label": ["first", "second"]}), columns=["label"])
    assert result.updated_rows == 2
    assert _snapshot(frames) == snapshot + 1


@pytest.mark.native_plan
@pytest.mark.parametrize("native_read", [False, True])
def test_update_columns_by_row_id_uses_native_update(frames, native_read):
    from pypaimon.execution.python.write.table_update import PythonTableUpdate

    frames.delete("id = 4")
    table = frames.copy({"write.native.enabled": "true", "read.native.enabled": str(native_read).lower()})
    snapshot = _snapshot(table)
    with patch.object(PythonTableUpdate, "update_by_arrow_with_row_id",
                      side_effect=AssertionError("must not write invalid IDs")):
        with pytest.raises(pmm.ColumnUpdateError) as failure:
            table.update_columns_by_row_id(
                pa.table({"_ROW_ID": [0, 4], "label": ["new", "deleted"]}), columns=["label"])
    assert failure.value.phase == "validate"
    assert failure.value.commit_outcome == "not_started"
    assert "not found or deleted: [4]" in str(failure.value.__cause__)
    assert _snapshot(table) == snapshot
    with patch.object(PythonTableUpdate, "update_by_arrow_with_row_id",
                      side_effect=AssertionError("must use native update")):
        result = table.update_columns_by_row_id(
            pa.table({"_ROW_ID": [3, 1], "label": ["three", "one"]}), columns=["label"])
    assert result.updated_rows == 2
    assert _rows(table, ["id", "label"]) == [
        {"id": i, "label": "one" if i == 1 else "three" if i == 3 else "old"} for i in range(4)]


def test_update_columns_by_row_id_validation_read_failure_never_writes(frames):
    from pypaimon.read.table_read import TableRead
    from pypaimon.write.table_update import BatchTableUpdate

    snapshot, files = _snapshot(frames), set(_files(frames))
    read_error = OSError("validation read failed")
    with patch.object(TableRead, "to_arrow", side_effect=read_error), patch.object(
            BatchTableUpdate, "update_by_arrow_with_row_id", side_effect=AssertionError("must not write")):
        with pytest.raises(pmm.ColumnUpdateError) as failure:
            frames.update_columns_by_row_id(pa.table({"_ROW_ID": [0], "label": ["new"]}), columns=["label"])
    assert failure.value.phase == "validate"
    assert failure.value.commit_outcome == "not_started"
    assert failure.value.__cause__ is read_error
    assert _snapshot(frames) == snapshot
    assert set(_files(frames)) == files


@pytest.mark.parametrize("lose_response", [False, True])
def test_update_columns_by_row_id_preserves_commit_state_when_close_fails(frames, lose_response):
    snapshot = _snapshot(frames)
    commit, close = BatchTableCommit.commit, BatchTableCommit.close
    commit_error, close_error = OSError("lost commit response"), OSError("commit close failed")

    def maybe_lose_response(self, messages, *args, **kwargs):
        commit(self, messages, *args, **kwargs)
        if lose_response:
            raise commit_error

    def failed_close(self):
        close(self)
        raise close_error

    with patch.object(BatchTableCommit, "commit", maybe_lose_response), patch.object(
            BatchTableCommit, "close", failed_close), patch.object(
            BatchTableCommit, "abort", side_effect=AssertionError("must preserve committed files")):
        with pytest.raises(pmm.ColumnUpdateError) as failure:
            frames.update_columns_by_row_id(
                pa.table({"_ROW_ID": [3, 0], "label": ["three", "zero"]}), columns=["label"])
    assert failure.value.phase == ("commit" if lose_response else "close")
    assert failure.value.commit_outcome == ("unknown" if lose_response else "committed")
    assert failure.value.__cause__ is (commit_error if lose_response else close_error)
    assert _snapshot(frames) == snapshot + 1
    assert _rows(frames, ["id", "label"]) == [
        {"id": i, "label": "zero" if i == 0 else "three" if i == 3 else "old"} for i in range(5)]


def test_update_columns_by_row_id_does_not_replay_failed_native_execution(frames):
    from unittest.mock import Mock

    backend = Mock()
    backend.update_by_arrow_with_row_id.side_effect = OSError("native execution failed")
    snapshot = _snapshot(frames)
    with patch("pypaimon.write.update_dispatch.prepare_native_write_operation", return_value=backend) as prepare:
        with patch("pypaimon.write.update_dispatch.create_python_update_operation",
                   side_effect=AssertionError("must not replay after execution")):
            with pytest.raises(pmm.ColumnUpdateError) as failure:
                frames.update_columns_by_row_id(pa.table({"_ROW_ID": [1], "label": ["new"]}), columns=["label"])
    prepare.assert_called_once()
    assert prepare.call_args[0][0] == "update"
    assert prepare.call_args[0][1]._read_snapshot.id == snapshot
    backend.update_by_arrow_with_row_id.assert_called_once()
    assert failure.value.phase == "write"
    assert failure.value.commit_outcome == "not_started"
    assert str(failure.value.__cause__) == "native execution failed"
    assert _snapshot(frames) == snapshot
    assert all(row["label"] == "old" for row in _rows(frames, ["id", "label"]))
