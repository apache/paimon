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

"""Correctness tests for distributed Daft writes to primary-key tables."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import textwrap
from unittest.mock import Mock

import pyarrow as pa
import pytest

pypaimon = pytest.importorskip("pypaimon")
daft = pytest.importorskip("daft")

from daft.recordbatch.micropartition import MicroPartition
from pypaimon.daft import daft_datasink
from pypaimon.daft.daft_paimon import _read_table, _write_table
from pypaimon.daft.daft_datasink import PaimonDataSink
from pypaimon.index.index_file_handler import IndexFileHandler


@pytest.fixture
def catalog(tmp_path):
    result = pypaimon.CatalogFactory.create({"warehouse": str(tmp_path)})
    result.create_database("test_db", ignore_if_exists=True)
    return result


def _create_pk_table(catalog, name, *, partitioned=False, options=None):
    fields = [
        pa.field("id", pa.int64()),
        pa.field("value", pa.string()),
    ]
    partition_keys = []
    primary_keys = ["id"]
    if partitioned:
        fields.append(pa.field("dt", pa.string()))
        partition_keys = ["dt"]
        primary_keys.append("dt")

    schema = pypaimon.Schema.from_pyarrow_schema(
        pa.schema(fields),
        partition_keys=partition_keys,
        primary_keys=primary_keys,
        options={"file.format": "parquet", **(options or {})},
    )
    identifier = f"test_db.{name}"
    catalog.create_table(identifier, schema, ignore_if_exists=False)
    return catalog.get_table(identifier)


def test_hash_fixed_pk_uses_one_writer_per_partition_bucket(catalog):
    table = _create_pk_table(
        catalog,
        "fixed_pk",
        partitioned=True,
        options={"bucket": "1"},
    )
    data = daft.from_pydict(
        {
            "id": [1, 1],
            "value": ["old", "new"],
            "dt": ["2026-07-20", "2026-07-20"],
        }
    ).into_partitions(2)

    summary = _write_table(data, table).to_pydict()

    # One complete (partition, bucket) group must be consumed by one
    # BatchTableWrite. Two independent writers would produce two files with
    # overlapping sequence-number ranges for this primary key.
    assert len(summary["file_name"]) == 1
    assert sum(summary["rows"]) == 1


def test_bare_datasink_supports_single_partition_primary_key_write(catalog):
    table = _create_pk_table(
        catalog,
        "bare_sink_pk",
        options={"bucket": "1"},
    )

    summary = daft.from_pydict(
        {"id": [1, 2], "value": ["a", "b"]}
    ).write_sink(PaimonDataSink(table)).to_pydict()

    assert sum(summary["rows"]) == 2
    assert _read_table(table).sort("id").to_pydict() == {
        "id": [1, 2],
        "value": ["a", "b"],
    }


def test_bare_datasink_rejects_multiple_primary_key_write_tasks(catalog):
    table = _create_pk_table(
        catalog,
        "bare_sink_distributed_pk",
        options={"bucket": "1"},
    )
    sink = PaimonDataSink(table)
    results = []
    for value in (1, 2):
        micropartition = MicroPartition.from_arrow(
            pa.table({"id": [value], "value": [f"v-{value}"]})
        )
        results.extend(sink.write(iter([micropartition])))

    with pytest.raises(
        ValueError, match="require a single non-empty Daft write task"
    ):
        sink.finalize(results)

    assert table.snapshot_manager().get_latest_snapshot() is None


def test_bare_datasink_supports_single_task_dynamic_bucket(catalog):
    table = _create_pk_table(
        catalog,
        "bare_sink_dynamic_pk",
        options={"bucket": "-1"},
    )
    sink = PaimonDataSink(table)
    micropartition = MicroPartition.from_arrow(
        pa.table({"id": [1], "value": ["v-1"]})
    )
    results = list(sink.write(iter([micropartition])))

    summary = sink.finalize(results).to_pydict()

    assert sum(summary["rows"]) == 1
    assert _read_table(table).to_pydict() == {
        "id": [1],
        "value": ["v-1"],
    }


def test_bare_datasink_rejects_multi_task_dynamic_bucket(catalog):
    table = _create_pk_table(
        catalog,
        "bare_sink_distributed_dynamic_pk",
        options={"bucket": "-1"},
    )
    sink = PaimonDataSink(table)
    results = []
    for value in (1, 2):
        micropartition = MicroPartition.from_arrow(
            pa.table({"id": [value], "value": [f"v-{value}"]})
        )
        results.extend(sink.write(iter([micropartition])))

    with pytest.raises(
        ValueError, match="require a single non-empty Daft write task"
    ):
        sink.finalize(results)

    assert table.snapshot_manager().get_latest_snapshot() is None


def test_commit_exception_does_not_abort_possibly_committed_files(catalog):
    table = _create_pk_table(
        catalog,
        "uncertain_commit_pk",
        options={"bucket": "1"},
    )
    sink = PaimonDataSink(table)
    micropartition = MicroPartition.from_arrow(
        pa.table({"id": [1], "value": ["committed"]})
    )
    write_results = list(sink.write(iter([micropartition])))

    real_commit = sink._write_builder.new_commit()

    def commit_then_raise(messages):
        real_commit.commit(messages)
        raise RuntimeError("callback failed after snapshot commit")

    uncertain_commit = Mock()
    uncertain_commit.commit.side_effect = commit_then_raise
    uncertain_commit.close.side_effect = real_commit.close
    sink._write_builder.new_commit = Mock(return_value=uncertain_commit)

    with pytest.raises(RuntimeError, match="callback failed"):
        sink.finalize(write_results)

    uncertain_commit.abort.assert_not_called()
    uncertain_commit.close.assert_called_once_with()
    assert _read_table(table).to_pydict() == {
        "id": [1],
        "value": ["committed"],
    }


def test_postpone_mode_primary_key_write_preserves_existing_path(catalog):
    table = _create_pk_table(
        catalog,
        "postpone_pk",
        options={"bucket": "-2"},
    )

    summary = _write_table(
        daft.from_pydict(
            {"id": [1, 2], "value": ["a", "b"]}
        ).into_partitions(2),
        table,
    ).to_pydict()

    assert sum(summary["rows"]) == 2
    assert table.snapshot_manager().get_latest_snapshot() is not None


def test_group_udfs_reuse_worker_table_metadata(catalog, monkeypatch):
    table = _create_pk_table(
        catalog,
        "worker_table_cache_pk",
        options={"bucket": "1"},
    )
    catalog_options = daft_datasink._extract_catalog_options(table)
    identifier = daft_datasink._extract_identifier(table)
    schema_token = (
        table.table_schema.id,
        table.table_schema.time_millis,
    )
    loader = Mock(wraps=daft_datasink._load_table)
    monkeypatch.setattr(daft_datasink, "_load_table", loader)
    daft_datasink._WORKER_TABLE_CACHE.clear()

    first = daft_datasink._load_worker_table(
        catalog_options, identifier, table.table_path, schema_token
    )
    second = daft_datasink._load_worker_table(
        catalog_options, identifier, table.table_path, schema_token
    )

    assert first is second
    loader.assert_called_once_with(
        catalog_options, identifier, table.table_path
    )
    daft_datasink._WORKER_TABLE_CACHE.clear()


def test_hash_dynamic_pk_restores_bucket_mapping_across_commits(catalog):
    table = _create_pk_table(
        catalog,
        "dynamic_pk",
        options={
            "bucket": "-1",
            "dynamic-bucket.target-row-num": "1",
        },
    )

    _write_table(
        daft.from_pydict({"id": [1], "value": ["old"]}),
        table,
    )
    _write_table(
        daft.from_pydict(
            {
                # A fresh task-local SimpleHashBucketAssigner puts id=2 in
                # bucket 0 and incorrectly moves the existing id=1 to bucket 1.
                "id": [2, 1],
                "value": ["other", "new"],
            }
        ),
        table,
    )

    result = _read_table(table).sort("id").to_pydict()
    assert result == {"id": [1, 2], "value": ["new", "other"]}

    snapshot = table.snapshot_manager().get_latest_snapshot()
    hash_indexes = [
        entry
        for entry in IndexFileHandler(table).scan(snapshot)
        if entry.index_file.index_type == "HASH"
    ]
    assert hash_indexes
    assert len({(tuple(e.partition.values), e.bucket) for e in hash_indexes}) == len(
        hash_indexes
    )


def test_hash_dynamic_pins_one_driver_snapshot_for_all_group_udfs(
    catalog, monkeypatch
):
    table = _create_pk_table(
        catalog,
        "dynamic_pinned_snapshot",
        options={"bucket": "-1"},
    )
    _write_table(
        daft.from_pydict({"id": [1], "value": ["old"]}), table
    ).to_pydict()
    expected_snapshot_id = table.snapshot_manager().get_latest_snapshot().id
    assignment_factory = Mock(
        wraps=daft_datasink.make_dynamic_bucket_assignment_udf
    )
    group_write_factory = Mock(wraps=daft_datasink.make_group_write_udf)
    monkeypatch.setattr(
        daft_datasink,
        "make_dynamic_bucket_assignment_udf",
        assignment_factory,
    )
    monkeypatch.setattr(
        daft_datasink, "make_group_write_udf", group_write_factory
    )

    _write_table(
        daft.from_pydict({"id": [1], "value": ["new"]}), table
    ).to_pydict()

    assert (
        assignment_factory.call_args.kwargs["base_snapshot_id"]
        == expected_snapshot_id
    )
    assert (
        group_write_factory.call_args.kwargs["base_snapshot_id"]
        == expected_snapshot_id
    )


def test_hash_dynamic_partitioned_pk_preserves_partition_columns(catalog):
    table = _create_pk_table(
        catalog,
        "dynamic_partitioned_pk",
        partitioned=True,
        options={
            "bucket": "-1",
            "dynamic-bucket.target-row-num": "1",
        },
    )

    _write_table(
        daft.from_pydict(
            {
                "id": [1, 1],
                "value": ["old-a", "old-b"],
                "dt": ["a", "b"],
            }
        ),
        table,
    )
    _write_table(
        daft.from_pydict(
            {
                "id": [2, 1, 2, 1],
                "value": ["other-a", "new-a", "other-b", "new-b"],
                "dt": ["a", "a", "b", "b"],
            }
        ).into_partitions(2),
        table,
    )

    rows = _read_table(table).to_pydict()
    assert set(zip(rows["id"], rows["value"], rows["dt"])) == {
        (1, "new-a", "a"),
        (2, "other-a", "a"),
        (1, "new-b", "b"),
        (2, "other-b", "b"),
    }


def test_hash_dynamic_pk_overwrite_replaces_hash_index(catalog):
    table = _create_pk_table(
        catalog,
        "dynamic_pk_overwrite",
        options={
            "bucket": "-1",
            "dynamic-bucket.target-row-num": "10",
        },
    )
    _write_table(
        daft.from_pydict({"id": [1, 2], "value": ["old-1", "old-2"]}),
        table,
    )

    _write_table(
        daft.from_pydict({"id": [3], "value": ["first"]}),
        table,
        mode="overwrite",
    )
    snapshot = table.snapshot_manager().get_latest_snapshot()
    hash_indexes = [
        entry
        for entry in IndexFileHandler(table).scan(snapshot)
        if entry.index_file.index_type == "HASH"
    ]
    assert sum(entry.index_file.row_count for entry in hash_indexes) == 1

    _write_table(
        daft.from_pydict(
            {"id": [4, 3], "value": ["other", "updated"]}
        ),
        table,
    )
    assert _read_table(table).sort("id").to_pydict() == {
        "id": [3, 4],
        "value": ["updated", "other"],
    }


def test_cross_partition_pk_write_requires_global_index(catalog):
    schema = pypaimon.Schema.from_pyarrow_schema(
        pa.schema(
            [
                pa.field("id", pa.int64()),
                pa.field("value", pa.string()),
                pa.field("dt", pa.string()),
            ]
        ),
        partition_keys=["dt"],
        primary_keys=["id"],
        options={"bucket": "-1", "file.format": "parquet"},
    )
    catalog.create_table("test_db.cross_partition_pk", schema, False)
    table = catalog.get_table("test_db.cross_partition_pk")
    data = daft.from_pydict({
        "id": [1, 2],
        "value": ["a", "b"],
        "dt": ["2026-07-20", "2026-07-21"],
    }).into_partitions(2)

    with pytest.raises(
        ValueError, match="CROSS_PARTITION.*global primary-key index"
    ):
        _write_table(data, table)


@pytest.mark.skipif(
    importlib.util.find_spec("ray") is None,
    reason="Distributed Daft correctness test requires Ray",
)
def test_pk_distributed_write_on_ray():
    """Exercise real multi-task ownership and compatible PK bucket modes."""
    python_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "..")
    )
    script = textwrap.dedent(
        r'''
        import os
        import shutil
        import tempfile

        os.environ.setdefault("RAY_TMPDIR", "/tmp")

        import daft
        import pyarrow as pa
        import ray
        from daft import runners
        from pypaimon import CatalogFactory, Schema
        from pypaimon.daft.daft_paimon import _read_table, _write_table
        from pypaimon.write.row_key_extractor import DynamicBucketRowKeyExtractor

        root = tempfile.mkdtemp(prefix="paimon-daft-pk-ray-")
        try:
            catalog = CatalogFactory.create({"warehouse": root})
            catalog.create_database("test_db", False)
            ray.init(num_cpus=2, include_dashboard=False)
            runners.set_runner_ray()

            fixed_schema = Schema.from_pyarrow_schema(
                pa.schema([
                    pa.field("id", pa.int64()),
                    pa.field("value", pa.string()),
                    pa.field("dt", pa.string()),
                ]),
                partition_keys=["dt"],
                primary_keys=["id", "dt"],
                options={"bucket": "1", "file.format": "parquet"},
            )
            catalog.create_table("test_db.fixed_pk", fixed_schema, False)
            fixed = catalog.get_table("test_db.fixed_pk")
            fixed_summary = _write_table(
                daft.from_pydict({
                    "id": [1, 1],
                    "value": ["old", "new"],
                    "dt": ["2026-07-20", "2026-07-20"],
                }).into_partitions(2),
                fixed,
            ).to_pydict()
            assert len(fixed_summary["file_name"]) == 1
            assert sum(fixed_summary["rows"]) == 1

            postpone_schema = Schema.from_pyarrow_schema(
                pa.schema([
                    pa.field("id", pa.int64()),
                    pa.field("value", pa.string()),
                ]),
                primary_keys=["id"],
                options={"bucket": "-2", "file.format": "parquet"},
            )
            catalog.create_table(
                "test_db.postpone_pk", postpone_schema, False
            )
            postpone = catalog.get_table("test_db.postpone_pk")
            postpone_summary = _write_table(
                daft.from_pydict({
                    "id": [1, 2, 3, 4],
                    "value": ["a", "b", "c", "d"],
                }).into_partitions(2),
                postpone,
            ).to_pydict()
            assert sum(postpone_summary["rows"]) == 4

            cross_partition_schema = Schema.from_pyarrow_schema(
                pa.schema([
                    pa.field("id", pa.int64()),
                    pa.field("value", pa.string()),
                    pa.field("dt", pa.string()),
                ]),
                partition_keys=["dt"],
                primary_keys=["id"],
                options={"bucket": "-1", "file.format": "parquet"},
            )
            catalog.create_table(
                "test_db.cross_partition_pk",
                cross_partition_schema,
                False,
            )
            cross_partition = catalog.get_table(
                "test_db.cross_partition_pk"
            )
            try:
                _write_table(
                    daft.from_pydict({
                        "id": [1, 1],
                        "value": ["old", "new"],
                        "dt": ["p1", "p2"],
                    }).into_partitions(2),
                    cross_partition,
                )
            except ValueError as error:
                assert "global primary-key index" in str(error)
            else:
                raise AssertionError("CROSS_PARTITION write was not rejected")

            dynamic_schema = Schema.from_pyarrow_schema(
                pa.schema([
                    pa.field("id", pa.int64()),
                    pa.field("value", pa.string()),
                ]),
                primary_keys=["id"],
                options={
                    "bucket": "-1",
                    "dynamic-bucket.target-row-num": "100",
                    "file.format": "parquet",
                },
            )
            catalog.create_table("test_db.dynamic_pk", dynamic_schema, False)
            dynamic = catalog.get_table("test_db.dynamic_pk")
            _write_table(
                daft.from_pydict({
                    "id": list(range(1, 9)),
                    "value": [f"old-{i}" for i in range(1, 9)],
                }),
                dynamic,
            )
            assigner_probe = pa.RecordBatch.from_pydict({
                "id": list(range(1, 9)),
                "value": [f"new-{i}" for i in range(1, 9)],
            })
            assigners = DynamicBucketRowKeyExtractor(
                dynamic.table_schema
            ).extract_assigners_batch(assigner_probe, 2, 2)
            assert set(assigners) == {0, 1}
            dynamic_summary = _write_table(
                daft.from_pydict({
                    "id": list(range(1, 9)),
                    "value": [f"new-{i}" for i in range(1, 9)],
                }).into_partitions(2),
                dynamic,
            ).to_pydict()
            # The first commit used one assigner and placed every key in
            # bucket 0. The second uses two assigners; both can resolve old
            # keys to bucket 0, but the final bucket shuffle must still leave
            # exactly one writer for it.
            assert len(dynamic_summary["file_name"]) == 1
            actual = _read_table(dynamic).sort("id").to_pydict()
            assert actual == {
                "id": list(range(1, 9)),
                "value": [f"new-{i}" for i in range(1, 9)],
            }
        finally:
            if ray.is_initialized():
                ray.shutdown()
            shutil.rmtree(root, ignore_errors=True)
        '''
    )
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(
        [python_root, env.get("PYTHONPATH", "")]
    ).rstrip(os.pathsep)
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        env=env,
        text=True,
        timeout=180,
    )
    assert result.returncode == 0, (
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
