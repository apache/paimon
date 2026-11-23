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

from collections import Counter
import json
import subprocess
import sys
import textwrap
from types import SimpleNamespace
from unittest.mock import patch

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.ray.indexed_lookup_join import (
    _LookupBatch, _output_schema, _prepare_lookup, indexed_lookup_join,
)


SCHEMA = pa.schema([("col0", pa.int64()), ("value", pa.string())])
SOURCE_SCHEMA = pa.schema([("col00", pa.int64()), ("label", pa.string())])
ROWS = [{"col0": 1, "value": "a"}, {"col0": 1, "value": "b"},
        {"col0": 2, "value": "c"}, {"col0": None, "value": "null"}]
SOURCE = pa.Table.from_pylist([
    {"col00": 1, "label": "x"}, {"col00": 1, "label": "y"},
    {"col00": 2, "label": "z"}, {"col00": 99, "label": "missing"},
    {"col00": None, "label": "null"}], schema=SOURCE_SCHEMA)


def write_rows(table, rows):
    builder = table.new_batch_write_builder()
    writer, committer = builder.new_write(), builder.new_commit()
    try:
        writer.write_arrow(pa.Table.from_pylist(rows, schema=SCHEMA))
        committer.commit(writer.prepare_commit())
    finally:
        writer.close()
        committer.close()


@pytest.fixture
def lookup(tmp_path, request):
    options = {"warehouse": str(tmp_path / "warehouse")}
    catalog = CatalogFactory.create(options)
    catalog.create_database("default", True)
    table_options = {
        "data-evolution.enabled": "true", "row-tracking.enabled": "true",
        "file.format": "parquet", "read.batch-size": "2",
    }
    if getattr(request, "param", False):
        pytest.importorskip("pypaimon_rust")
    else:
        table_options.update({
            "read.native.enabled": "false", "scan.native-plan.enabled": "false",
            "write.native.enabled": "false", "commit.native.enabled": "false",
        })
    catalog.create_table("default.A", Schema.from_pyarrow_schema(SCHEMA, options=table_options), False)
    table = catalog.get_table("default.A")
    write_rows(table, ROWS)
    return table, options


def worker(table, **kwargs):
    return _LookupBatch(_prepare_lookup(table, "col0"), SOURCE_SCHEMA,
                        "col00", "col0", ["col0", "value"], None, 2, **kwargs)


def records(batches):
    return Counter(tuple(row.values()) for batch in batches for row in batch.to_pylist())


@pytest.mark.parametrize("kind", ["btree", "bitmap"])
@pytest.mark.parametrize("lookup", [False, True], indirect=True, ids=["python", "native"])
def test_index_lookup_preserves_duplicates_and_nulls(lookup, kind):
    table, _ = lookup
    table.create_global_index("col0", kind)
    lookup_batch = worker(table)
    try:
        original = lookup_batch.scanner.scan
        predicates = []

        def record(predicate):
            predicates.append(predicate)
            return original(predicate)

        with patch.object(lookup_batch.scanner, "scan", record):
            batches = list(lookup_batch(SOURCE))
        assert predicates[0].literals == [1, 2, 99]
        assert all(batch.num_rows <= 2 for batch in batches)
        assert records(batches) == Counter({
            (1, "x", "a"): 1, (1, "x", "b"): 1,
            (1, "y", "a"): 1, (1, "y", "b"): 1, (2, "z", "c"): 1})
        with patch.object(lookup_batch.scanner, "scan", side_effect=AssertionError("empty lookup")):
            assert records(lookup_batch(SOURCE.slice(0, 0))) == Counter()
            assert records(lookup_batch(SOURCE.slice(4, 1))) == Counter()
    finally:
        lookup_batch.close()


def test_rejects_missing_partial_and_disabled_index(lookup):
    table, _ = lookup
    with pytest.raises(ValueError, match="ordinary.*join"):
        _prepare_lookup(table, "col0")
    table.create_global_index("col0", "btree")
    with pytest.raises(ValueError, match="disabled"):
        _prepare_lookup(table.copy({"global-index.enabled": "false"}), "col0")
    write_rows(table, [{"col0": 3, "value": "new"}])
    with pytest.raises(ValueError, match="complete"):
        _prepare_lookup(table, "col0")


def test_rejects_stale_index_after_ignore_update(lookup):
    from pypaimon.multimodal.table import MultimodalTable
    table, options = lookup
    table.create_global_index("col0", "btree")
    table = table.copy({"global-index.column-update-action": "IGNORE"})
    MultimodalTable(CatalogFactory.create(options), "default.A", table).update("col0 = 1", {"col0": 5})
    with pytest.raises(ValueError, match="stale"):
        _prepare_lookup(table, "col0")
    # IGNORE can be a writer-only override; a newly opened table must still reject it.
    with pytest.raises(ValueError, match="stale"):
        _prepare_lookup(CatalogFactory.create(options).get_table("default.A"), "col0")


def test_deletions_and_unprojected_filter_column(lookup):
    from pypaimon.multimodal.table import MultimodalTable
    table, options = lookup
    table.create_global_index("col0", "btree")
    table = table.copy({"deletion-vectors.enabled": "true"})
    MultimodalTable(CatalogFactory.create(options), "default.A", table).delete("value = 'a'")
    predicate = table.new_read_builder().new_predicate_builder().equal("value", "b")
    lookup_batch = _LookupBatch(_prepare_lookup(table, "col0"), SOURCE_SCHEMA,
                                "col00", "col0", ["col0"], predicate, 2)
    try:
        assert records(lookup_batch(SOURCE)) == Counter({(1, "x"): 1, (1, "y"): 1})
    finally:
        lookup_batch.close()


def test_projection_type_and_auth_validation(lookup):
    table, _ = lookup
    table.create_global_index("col0", "btree")
    for projection in (["value"], ["col0", "col0"], ["col0", "missing"]):
        with pytest.raises(ValueError, match="projection"):
            _output_schema(SOURCE_SCHEMA, table, "col00", "col0", projection)
    for dtype in (pa.int32(), pa.float64(), pa.list_(pa.int64())):
        with pytest.raises(ValueError, match="types"):
            _output_schema(pa.schema([("col00", dtype)]), table, "col00", "col0", SCHEMA.names)
    with pytest.raises(ValueError, match="collide"):
        _output_schema(pa.schema([("col00", pa.int64()), ("value", pa.string())]),
                       table, "col00", "col0", SCHEMA.names)
    with pytest.raises(ValueError, match="authorization"):
        _prepare_lookup(table.copy({"query-auth.enabled": "true"}), "col0")


def test_unsupported_indexes_do_not_satisfy_preflight(lookup):
    from pypaimon.index.index_file_handler import IndexFileHandler
    table, _ = lookup
    entries = [SimpleNamespace(index_file=SimpleNamespace(index_type="fm", global_index_meta=object()),
                               partition=None)]

    def scan(handler, snapshot, entry_filter=None):
        return [entry for entry in entries if entry_filter is None or entry_filter(entry)]

    with patch.object(IndexFileHandler, "scan", scan):
        with pytest.raises(ValueError, match="BTree or Bitmap"):
            _prepare_lookup(table, "col0")


@pytest.mark.parametrize("dtype", [pa.float64(), pa.list_(pa.int64())])
def test_unsupported_lookup_key_types_rejected(lookup, dtype):
    _, options = lookup
    catalog = CatalogFactory.create(options)
    catalog.create_table("default.other", Schema.from_pyarrow_schema(
        pa.schema([("key", dtype)]), options={
            "data-evolution.enabled": "true", "row-tracking.enabled": "true"}), False)
    with pytest.raises(ValueError, match="integer or string"):
        _prepare_lookup(catalog.get_table("default.other"), "key")


@pytest.mark.parametrize("kind", ["btree", "bitmap"])
def test_string_key_and_nested_payload(lookup, kind):
    table, _ = lookup
    table.create_global_index("value", kind)
    batch = pa.table({"key": ["a", "a", None, "absent"], "nested": [[1], [2], [3], [4]]})
    lookup_batch = _LookupBatch(_prepare_lookup(table, "value"), batch.schema,
                                "key", "value", ["value", "col0"], None, 1)
    try:
        actual = [row for chunk in lookup_batch(batch) for row in chunk.to_pylist()]
        assert sorted(actual, key=lambda row: row["nested"]) == [
            {"key": "a", "nested": [1], "col0": 1},
            {"key": "a", "nested": [2], "col0": 1}]
    finally:
        lookup_batch.close()


@pytest.mark.parametrize("lookup", [True], indirect=True, ids=["native"])
@pytest.mark.execution_defaults
def test_default_backend_reaches_native_reader(lookup):
    from pypaimon.execution.native.read.table_read import NativeTableRead
    table, _ = lookup
    assert table.options.native_read_enabled()
    assert table.options.native_plan_enabled()
    table.create_global_index("col0", "btree")
    native_reads = []
    original = NativeTableRead.read_batches

    def record(reader, *args, **kwargs):
        result = original(reader, *args, **kwargs)
        assert result is not None
        native_reads.append(True)
        return result

    lookup_batch = worker(table)
    try:
        with patch.object(NativeTableRead, "read_batches", record):
            assert sum(records(lookup_batch(SOURCE)).values()) == 5
        assert native_reads
    finally:
        lookup_batch.close()


def test_snapshot_and_original_filter(lookup):
    table, _ = lookup
    table.create_global_index("col0", "btree")
    pinned = _prepare_lookup(table, "col0")
    predicate = table.new_read_builder().new_predicate_builder().equal("value", "a")
    write_rows(table, [{"col0": 1, "value": "later"}])
    lookup_batch = _LookupBatch(pinned, SOURCE_SCHEMA, "col00", "col0", SCHEMA.names, predicate, 2)
    try:
        assert records(lookup_batch(SOURCE)) == Counter({(1, "x", "a"): 1, (1, "y", "a"): 1})
    finally:
        lookup_batch.close()


def test_existing_time_travel_view_is_preserved(lookup):
    table, _ = lookup
    table.create_global_index("col0", "btree")
    snapshot = table.snapshot_manager().get_latest_snapshot()
    historical = table.copy({"scan.snapshot-id": str(snapshot.id)})
    write_rows(table, [{"col0": 1, "value": "later"}])
    pinned = _prepare_lookup(historical, "col0")
    assert pinned._read_snapshot.id == snapshot.id
    lookup_batch = _LookupBatch(pinned, SOURCE_SCHEMA, "col00", "col0", SCHEMA.names, None, 2)
    try:
        assert sum(records(lookup_batch(SOURCE)).values()) == 5
    finally:
        lookup_batch.close()


@pytest.mark.parametrize("kind", ["btree", "bitmap"])
def test_lookup_accepts_historical_data_schema(lookup, kind):
    from pypaimon.schema.schema_change import SchemaChange
    table, options = lookup
    catalog = CatalogFactory.create(options)
    catalog.alter_table("default.A", [SchemaChange.update_column_comment("value", "payload")])
    table = catalog.get_table("default.A")
    table.create_global_index("col0", kind)
    lookup_batch = worker(table)
    try:
        assert sum(records(lookup_batch(SOURCE)).values()) == 5
    finally:
        lookup_batch.close()


@pytest.mark.parametrize("kind", ["btree", "bitmap"])
def test_lookup_bounds_index_file_descriptors(lookup, kind):
    pytest.importorskip("resource")
    table, options = lookup
    catalog = CatalogFactory.create(options)
    catalog.create_table("default.partitioned", Schema.from_pyarrow_schema(
        SCHEMA, partition_keys=["value"], options={
            **table.options.options.to_map(), "global-index.thread-num": "1", "read.parallelism": "1"}), False)
    table = catalog.get_table("default.partitioned")
    write_rows(table, [{"col0": key, "value": str(key)} for key in range(80)])
    table.create_global_index("col0", kind)
    code = textwrap.dedent('''
        import resource
        import sys
        import pyarrow as pa
        from pypaimon import CatalogFactory
        from pypaimon.ray.indexed_lookup_join import _LookupBatch, _prepare_lookup
        table = CatalogFactory.create({"warehouse": sys.argv[1]}).get_table("default.partitioned")
        source = pa.table({"col00": range(80)})
        actor = _LookupBatch(_prepare_lookup(table, "col0"), source.schema,
                             "col00", "col0", ["col0", "value"], None, 100)
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(resource.RLIMIT_NOFILE, (64, hard))
        try:
            for key in range(80):
                assert sum(batch.num_rows for batch in actor(source.slice(key, 1))) == 1
            assert sum(batch.num_rows for batch in actor(source)) == 80
        finally:
            resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))
            actor.close()
    ''')
    result = subprocess.run([sys.executable, "-c", code, options["warehouse"]],
                            capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stdout + result.stderr


def test_key_type_mismatch_rejected_before_lookup(lookup):
    table, _ = lookup
    table.create_global_index("col0", "btree")
    lookup_batch = worker(table)
    try:
        with patch.object(lookup_batch.scanner, "scan", side_effect=AssertionError("bad lookup")):
            with pytest.raises(ValueError, match="type"):
                list(lookup_batch(SOURCE.cast(pa.schema([("col00", pa.int32()), ("label", pa.string())]))))
    finally:
        lookup_batch.close()


@pytest.fixture(scope="module")
def ray_cluster():
    ray = pytest.importorskip("ray")
    started = not ray.is_initialized()
    if started:
        ray.init(address="local", num_cpus=2, include_dashboard=False,
                 object_store_memory=100 * 1024 * 1024)
    yield ray
    if started:
        ray.shutdown()


@pytest.mark.parametrize("lookup", [False, True], indirect=True, ids=["python", "native"])
def test_ray_join_uses_index_and_pins_snapshot(lookup, ray_cluster, tmp_path, monkeypatch):
    import importlib
    module = importlib.import_module("pypaimon.ray.indexed_lookup_join")
    table, options = lookup
    table.create_global_index("col0", "btree")
    calls = str(tmp_path / "index-calls")

    class ObservedLookup(_LookupBatch):
        def __call__(self, batch):
            original = self.scanner.scan

            def record(predicate):
                result = original(predicate)
                with open(calls, "a") as output:
                    output.write(json.dumps({"keys": predicate.literals,
                                             "rows": result.results().to_list()}) + "\n")
                return result

            with patch.object(self.scanner, "scan", record):
                yield from super().__call__(batch)

    monkeypatch.setattr(module, "_LookupBatch", ObservedLookup)
    source = ray_cluster.data.from_arrow(SOURCE)
    result = indexed_lookup_join(source, "default.A", options,
                                 source_on="col00", lookup_on="col0", batch_size=2,
                                 concurrency=1, output_batch_size=2)
    assert not (tmp_path / "index-calls").exists(), "constructing the join must not execute lookups"
    write_rows(table, [{"col0": 1, "value": "later"}])
    expected = SOURCE.join(pa.Table.from_pylist(ROWS, schema=SCHEMA),
                           keys="col00", right_keys="col0", join_type="inner")
    assert Counter(tuple(row.values()) for row in result.take_all()) == records([expected])
    calls = [json.loads(line) for line in (tmp_path / "index-calls").read_text().splitlines()]
    assert calls
    expected_rows = {1: [0, 1], 2: [2], 99: []}
    for call in calls:
        assert call["rows"] == sorted({row for key in call["keys"] for row in expected_rows[key]})


def test_ray_empty_source_has_schema(lookup, ray_cluster):
    table, options = lookup
    table.create_global_index("col0", "bitmap")
    result = indexed_lookup_join(ray_cluster.data.from_arrow(SOURCE.slice(0, 0)),
                                 "default.A", options, source_on="col00", lookup_on="col0")
    assert result.take_all() == []
    assert result.schema().names == ["col00", "label", "value"]


@pytest.mark.parametrize("version", ["2.10.0", "2.20.0"])
def test_rejects_ray_with_eager_union(ray_cluster, monkeypatch, version):
    source = ray_cluster.data.from_arrow(SOURCE)
    monkeypatch.setattr(ray_cluster, "__version__", version)
    with pytest.raises(ValueError, match="Ray 2.50"):
        indexed_lookup_join(source, "missing", {}, source_on="col00", lookup_on="col0")


@pytest.mark.parametrize("empty", [False, True])
def test_ray_pandas_source(lookup, ray_cluster, empty):
    import pandas as pd
    table, options = lookup
    table.create_global_index("col0", "btree")
    frame = pd.DataFrame({"col00": pd.Series([] if empty else [1, 1, None, 99], dtype="Int64")})
    source = ray_cluster.data.from_pandas(frame)
    result = indexed_lookup_join(source, "default.A", options, source_on="col00", lookup_on="col0")
    assert Counter(tuple(row.values()) for row in result.take_all()) == (
        Counter() if empty else Counter({(1, "a"): 2, (1, "b"): 2}))
    assert result.schema().names == ["col00", "value"]


@pytest.mark.parametrize("reverse", [False, True])
def test_ray_sparse_pandas_payload_across_blocks(lookup, ray_cluster, reverse):
    import pandas as pd
    table, options = lookup
    table.create_global_index("col0", "btree")
    blocks = [
        pd.DataFrame({"col00": [1], "label": ["x"]}),
        pd.DataFrame({"col00": [2], "label": [None]}),
    ]
    source = ray_cluster.data.from_pandas(blocks[::-1] if reverse else blocks)
    result = indexed_lookup_join(source, "default.A", options,
                                 source_on="col00", lookup_on="col0", batch_size=1)
    assert Counter(tuple(row.values()) for row in result.take_all()) == Counter({
        (1, "x", "a"): 1, (1, "x", "b"): 1, (2, None, "c"): 1})


@pytest.mark.parametrize("kind", ["btree", "bitmap"])
@pytest.mark.parametrize("shards, key, expected", [(8, 1, 8), (80, 2, 0)])
@pytest.mark.parametrize("cache_bytes", [128, 32 * 1024 * 1024], ids=["eviction", "resident"])
def test_repeated_lookup_reuses_index_blocks(lookup, kind, shards, key, expected, cache_bytes, monkeypatch):
    import importlib
    from pypaimon.common.file_io import pread
    from pypaimon.utils.file_type import FileType
    module = importlib.import_module("pypaimon.ray.indexed_lookup_join")
    monkeypatch.setattr(module, "_INDEX_CACHE_BYTES", cache_bytes)
    table, options = lookup
    catalog = CatalogFactory.create(options)
    catalog.create_table("default.cached", Schema.from_pyarrow_schema(
        SCHEMA, partition_keys=["value"], options=table.options.options.to_map()), False)
    table = catalog.get_table("default.cached")
    write_rows(table, [{"col0": value, "value": str(partition)}
                       for partition in range(shards) for value in [1, 3]])
    table.create_global_index("col0", kind)
    actor = worker(table)
    source = pa.table({"col00": [key], "label": ["source"]})
    original = table.file_io.new_input_stream
    opened = []
    reads = []

    def count_open(path):
        stream = original(path)
        if FileType.classify(path) == FileType.GLOBAL_INDEX:
            opened.append(path)

            def read_at(size, offset):
                reads.append((path, offset, size))
                return pread(stream, size, offset)

            return SimpleNamespace(read_at=read_at, close=stream.close)
        return stream

    try:
        with patch.object(table.file_io, "new_input_stream", count_open):
            for _ in range(30):
                assert sum(batch.num_rows for batch in actor(source)) == expected
        if cache_bytes == 128:
            assert len(opened) > shards
        else:
            assert len(opened) == shards, (len(opened), shards)
            assert len(reads) == shards, (len(reads), shards)
        assert actor.scanner.table.file_io._cache._current_size <= cache_bytes
    finally:
        actor.close()


@pytest.mark.parametrize("source", [
    pa.table({"col00": pa.array([None]), "label": ["x"]}),
    pa.table({"col00": pa.array([1], type=pa.int32()), "label": ["x"]}),
    pa.table({"col00": [1], "label": [42]}),
])
def test_null_payload_normalization_does_not_coerce_keys_or_other_types(lookup, source):
    table, _ = lookup
    table.create_global_index("col0", "btree")
    actor = worker(table)
    try:
        with patch.object(actor.scanner, "scan", side_effect=AssertionError("invalid schema reached index")):
            with pytest.raises(ValueError, match="schema or key type"):
                list(actor(source))
    finally:
        actor.close()
