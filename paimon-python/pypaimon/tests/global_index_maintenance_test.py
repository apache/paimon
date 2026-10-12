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

from unittest.mock import Mock, patch

import pyarrow as pa
import pytest

import pypaimon.multimodal as pm
from pypaimon.globalindex.create_global_index import GlobalIndexBuilder
from pypaimon.index.index_file_handler import IndexFileHandler

SPEC = {"column": "embedding", "type": "ivf-flat", "options": {
    "ivf-flat.nlist": "1", "global-index.row-count-per-shard": "2"}}


@pytest.fixture
def docs(tmp_path):
    pytest.importorskip("paimon_vindex")
    table = pm.connect(options={"warehouse": str(tmp_path)}).create_table(
        "docs", schema=pa.schema([("id", pa.int64()), ("pt", pa.string()),
                                  ("embedding", pa.list_(pa.float32(), 2))]), partitioned=["pt"],
        options={"file.format": "parquet", "vector.file.format": "parquet",
                 "vector-index.search-mode": "full", "global-index.column-update-action": "DROP_PARTITION_INDEX"})
    table.add([{"id": i, "pt": "a" if i < 2 else "b", "embedding": [float(i), 1.]}
               for i in range(4)])
    return table


def entries(docs, snapshot=None):
    table = docs.raw_table
    return IndexFileHandler(table).scan(snapshot or table.snapshot_manager().get_latest_snapshot())


def names(docs, snapshot=None):
    return {entry.index_file.file_name for entry in entries(docs, snapshot)}


def test_catch_up_is_idempotent_and_restores_dropped_partition(docs):
    assert docs.maintain_indexes([SPEC]) == 2
    original = names(docs)
    snapshot = docs.raw_table.snapshot_manager().get_latest_snapshot()
    assert docs.maintain_indexes([SPEC]) == 0
    assert docs.raw_table.snapshot_manager().get_latest_snapshot().id == snapshot.id
    docs.add([{"id": 4, "pt": "b", "embedding": [4., 1.]}])
    assert docs.maintain_indexes([SPEC]) == 1
    assert names(docs).issuperset(original)
    docs.update("id = 0", {"embedding": [100., 1.]})
    preserved = names(docs)
    assert len(preserved) == 2  # Unmodified partition b keeps both files.
    assert docs.maintain_indexes([SPEC]) == 1
    assert names(docs).issuperset(preserved)
    assert docs.search([100., 1.]).select(["id"]).limit(1).to_list() == [{"id": 0}]


def test_partition_replacement_is_atomic_and_old_snapshot_remains_readable(docs):
    docs.maintain_indexes([SPEC])
    before = docs.raw_table.snapshot_manager().get_latest_snapshot()
    old = names(docs)
    untouched = {entry.index_file.file_name for entry in entries(docs) if entry.partition.values == ["b"]}
    assert docs.maintain_indexes([SPEC], rebuild=True, partitions={"pt": "a"}) == 1
    assert docs.raw_table.snapshot_manager().get_latest_snapshot().id == before.id + 1
    assert names(docs) & old == untouched
    assert names(docs, before) == old
    for name in old:
        assert docs.raw_table.file_io.exists(docs.raw_table.path_factory().global_index_path_factory().to_path(name))
    # Inner product with [0, 1] ties every row. Row IDs depend on partition
    # write order, so use a query whose unique best match is id=0.
    assert docs.search([-1., 1.], snapshot_id=before.id).select(["id"]).limit(1).to_list() == [{"id": 0}]


def test_concurrent_commit_rejects_replacement(docs):
    docs.maintain_indexes([SPEC])
    old = names(docs)
    original = GlobalIndexBuilder.build

    def append_after_build(builder):
        messages = original(builder)
        docs.add([{"id": 5, "pt": "a", "embedding": [5., 1.]}])
        return messages

    with patch.object(GlobalIndexBuilder, "build", append_after_build):
        with pytest.raises(RuntimeError, match="Global index maintenance conflict"):
            docs.maintain_indexes([SPEC], rebuild=True)
    assert names(docs) == old
    assert docs.scan().to_arrow().num_rows == 5
    assert docs.maintain_indexes([SPEC], rebuild=True) == 3


@pytest.mark.parametrize("rebuild", [False, True])
@pytest.mark.parametrize("during_retry", [False, True])
@pytest.mark.parametrize("legacy_snapshot", [False, True])
def test_rollback_and_reappend_rejects_reused_snapshot(tmp_path, rebuild, during_retry, legacy_snapshot):
    from pypaimon.common.json_util import JSON
    from pypaimon.globalindex.data_evolution_global_index_scanner import DataEvolutionGlobalIndexScanner
    from pypaimon.snapshot.renaming_snapshot_commit import RenamingSnapshotCommit

    docs = pm.connect(options={"warehouse": str(tmp_path)}).create_table(
        "docs", schema=pa.schema([("id", pa.int64())]),
        options={"file.format": "parquet", "write.native.enabled": "false",
                 "commit.native.enabled": "false"})
    docs.add([{"id": 0}])
    spec = {"column": "id", "type": "btree"}
    if rebuild:
        docs.maintain_indexes([spec])
    table = docs.raw_table

    def latest_snapshot():
        manager = table.snapshot_manager()
        snapshot = manager.get_latest_snapshot()
        if legacy_snapshot:
            # Older snapshot files have no UUID. Preserve that representation
            # for both versions of the reused snapshot ID.
            snapshot.uuid = None
            table.file_io.write_file(manager.get_snapshot_path(snapshot.id),
                                     JSON.to_json(snapshot), overwrite=True)
        return snapshot

    rollback_snapshot = table.snapshot_manager().get_latest_snapshot()
    old_indexes = names(docs)
    docs.add([{"id": 10}])
    planned = latest_snapshot()
    replacements = []

    def replace_snapshot():
        table.rollback_to(rollback_snapshot.id)
        docs.add([{"id": 20}])
        replacement = latest_snapshot()
        assert (replacement.id, replacement.schema_id, replacement.next_row_id) == (
            planned.id, planned.schema_id, planned.next_row_id)
        if legacy_snapshot:
            assert replacement.uuid is planned.uuid is None
        else:
            assert replacement.uuid != planned.uuid
        assert replacement.delta_manifest_list != planned.delta_manifest_list
        replacements.append(replacement)

    original_build = GlobalIndexBuilder.build
    original_commit = RenamingSnapshotCommit.commit

    def build(builder):
        messages = original_build(builder)
        if not during_retry:
            replace_snapshot()
        return messages

    def commit(committer, previous_uuid, snapshot, statistics):
        if during_retry and snapshot.index_manifest and not replacements:
            # Simulate a failed CAS after the first check; the retry must
            # still compare against the original plan, not the reused ID.
            with patch.object(RenamingSnapshotCommit, "commit", original_commit):
                replace_snapshot()
            return False
        return original_commit(committer, previous_uuid, snapshot, statistics)

    with patch.object(GlobalIndexBuilder, "build", build), \
            patch.object(RenamingSnapshotCommit, "commit", commit):
        with pytest.raises(RuntimeError, match="Global index maintenance conflict"):
            docs.maintain_indexes([spec], rebuild=rebuild)

    assert len(replacements) == 1
    assert table.snapshot_manager().get_latest_snapshot() == replacements[0]
    assert names(docs) == old_indexes
    assert docs.scan().to_arrow().sort_by("id").to_pylist() == [{"id": 0}, {"id": 20}]
    assert docs.scan().where("id = 20").to_list() == [{"id": 20}]
    assert docs.maintain_indexes([spec], rebuild=rebuild) > 0
    predicate = table.new_read_builder().new_predicate_builder().equal("id", 20)
    with DataEvolutionGlobalIndexScanner.create(
            table, predicate=predicate,
            snapshot=table.snapshot_manager().get_latest_snapshot()) as scanner:
        result = scanner.scan(predicate)
    assert result is not None
    rb = table.new_read_builder().with_filter(predicate)
    splits = rb.new_scan().with_global_index_result(result).plan().splits()
    assert rb.new_read().to_arrow(splits).to_pylist() == [{"id": 20}]


@pytest.mark.parametrize("rebuild", [False, True])
def test_concurrent_compaction_is_not_rolled_back(tmp_path, rebuild):
    docs = pm.connect(options={"warehouse": str(tmp_path)}).create_table(
        "docs", schema=pa.schema([("id", pa.int64())]),
        options={"file.format": "parquet", "commit.native.enabled": "false",
                 "write.native.enabled": "false"})
    docs.add([{"id": 0}, {"id": 1}])
    spec = {"column": "id", "type": "btree"}
    if rebuild:
        docs.maintain_indexes([spec])
    table = docs.raw_table
    old_indexes = names(docs)
    old_files = {file.file_name for split in table.new_read_builder().new_scan().plan().splits()
                 for file in split.files}
    original = GlobalIndexBuilder.build
    compacted = []

    def compact_after_build(builder):
        messages = original(builder)
        read_builder = table.new_read_builder()
        splits = read_builder.new_scan().plan_for_write().splits()
        current = read_builder.new_read().to_arrow(splits).sort_by("id")
        wb = table.new_batch_write_builder()
        writer = wb.new_write()
        commit = wb.new_commit()
        try:
            writer.write_arrow(current)
            compact_messages = writer.prepare_commit()
            assert len(compact_messages) == 1
            message = compact_messages[0]
            assert len(message.new_files) == 1
            message.new_files = [message.new_files[0].assign_first_row_id(0)]
            message.deleted_files.extend(file for split in splits for file in split.files)
            # There is no public compaction API; publish the physical file
            # replacement through the production committer as a COMPACT snapshot.
            store_commit = commit.file_store_commit
            try_commit = store_commit._try_commit
            with patch.object(store_commit, "_try_commit",
                              side_effect=lambda commit_kind, **kwargs: try_commit("COMPACT", **kwargs)):
                commit.commit(compact_messages)
            compacted.append((table.snapshot_manager().get_latest_snapshot(), message.new_files[0].file_name))
        finally:
            writer.close()
            commit.close()
        return messages

    def rollback_to(instant, from_snapshot):
        assert table.snapshot_manager().get_latest_snapshot().id == from_snapshot
        # Exercise real rollback if the regression returns: only the catalog
        # endpoint is replaced, not conflict detection or the commit retry loop.
        table.rollback_helper().clean_larger_than(
            table.snapshot_manager().get_snapshot_by_id(instant.snapshot_id))

    rollback = Mock()
    rollback.rollback_to.side_effect = rollback_to
    with patch.object(table.catalog_environment, "catalog_table_rollback", return_value=rollback), \
            patch.object(GlobalIndexBuilder, "build", compact_after_build):
        with pytest.raises(RuntimeError, match="Global index maintenance conflict"):
            docs.maintain_indexes([spec], rebuild=rebuild)
    rollback.rollback_to.assert_not_called()
    snapshot, compacted_file = compacted[0]
    latest = table.snapshot_manager().get_latest_snapshot()
    assert (latest.id, latest.commit_kind) == (snapshot.id, "COMPACT")
    assert compacted_file not in old_files
    assert {file.file_name for split in table.new_read_builder().new_scan().plan().splits()
            for file in split.files} == {compacted_file}
    assert names(docs) == old_indexes
    assert docs.scan().to_arrow().sort_by("id").to_pylist() == [{"id": 0}, {"id": 1}]
    assert docs.maintain_indexes([spec], rebuild=rebuild) == 1
    assert table.snapshot_manager().get_latest_snapshot().id == snapshot.id + 1


def test_build_failure_keeps_indexes_and_cleans_completed_outputs(docs):
    docs.maintain_indexes([SPEC])
    old = names(docs)
    original = GlobalIndexBuilder.build
    outputs = []

    def fail_second(builder):
        if outputs:
            raise RuntimeError("second index failed")
        messages = original(builder)
        outputs.extend(entry.index_file.file_name for message in messages for entry in message.index_adds)
        return messages

    with patch.object(GlobalIndexBuilder, "build", fail_second):
        with pytest.raises(RuntimeError, match="second index failed"):
            docs.maintain_indexes([SPEC, {"column": "id", "type": "btree"}], rebuild=True)
    assert names(docs) == old
    paths = docs.raw_table.path_factory().global_index_path_factory()
    assert all(not docs.raw_table.file_io.exists(paths.to_path(name)) for name in outputs)


@pytest.mark.parametrize("specs", [[], [{}], [SPEC, SPEC], [{"column": "missing", "type": "ivf-flat"}]])
def test_validates_all_definitions_before_building(docs, specs):
    with patch.object(GlobalIndexBuilder, "build", side_effect=AssertionError("premature build")):
        with pytest.raises(ValueError):
            docs.maintain_indexes(specs)


def test_multiple_definitions_publish_once(docs):
    before = docs.raw_table.snapshot_manager().get_latest_snapshot().id
    assert docs.maintain_indexes([SPEC, {"column": "id", "type": "btree"}]) == 4
    assert docs.raw_table.snapshot_manager().get_latest_snapshot().id == before + 1
    assert {entry.index_file.index_type for entry in entries(docs)} == {"ivf-flat", "btree"}


@pytest.mark.parametrize("rebuild", [False, True])
def test_maintenance_keeps_python_planning_with_native_writes(docs, rebuild):
    spec = {"column": "id", "type": "btree"}
    docs.maintain_indexes([spec])
    old = names(docs)
    docs.add([{"id": 4, "pt": "a", "embedding": [4., 1.]}])
    before = docs.raw_table.snapshot_manager().get_latest_snapshot().id
    docs.raw_table = docs.raw_table.copy({
        "write.native.enabled": "true", "deletion-vectors.enabled": "false"})
    # Maintenance must use its pinned snapshot and Python rebuild ranges even
    # when the table is eligible for the new native index-build entry point.
    with patch("pypaimon.globalindex.native_index_build.native_method_available",
               side_effect=AssertionError("maintenance delegated to native index planning")):
        assert docs.maintain_indexes([spec], rebuild=rebuild) == (3 if rebuild else 1)
    assert docs.raw_table.snapshot_manager().get_latest_snapshot().id == before + 1
    if rebuild:
        assert names(docs).isdisjoint(old)
    else:
        assert names(docs).issuperset(old)
    assert docs.raw_table.options.native_write_enabled()


def test_rebuild_repairs_ignored_updates(docs):
    docs.maintain_indexes([SPEC])
    docs.raw_table = docs.raw_table.copy({"global-index.column-update-action": "IGNORE"})
    docs.update("id = 0", {"embedding": [100., 1.]})
    assert docs.maintain_indexes([SPEC]) == 0  # IGNORE has not invalidated coverage.
    assert docs.maintain_indexes([SPEC], rebuild=True) == 2
    assert docs.search([100., 1.]).select(["id"]).limit(1).to_list() == [{"id": 0}]
