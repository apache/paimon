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

from unittest.mock import Mock

import pyarrow as pa
import pytest

from pypaimon import CatalogFactory, Schema
from pypaimon.execution.python.write.file_store_commit import FileStoreCommit


@pytest.fixture
def table(tmp_path):
    catalog = CatalogFactory.create({"warehouse": str(tmp_path)})
    catalog.create_database("default", False)
    catalog.create_table("default.frames", Schema.from_pyarrow_schema(
        pa.schema([("episode_id", pa.string())])), False)
    return catalog.get_table("default.frames")


def prepare(table, value):
    writer = table.new_batch_write_builder().new_write()
    writer.write_arrow(pa.table({"episode_id": [value]}))
    messages = writer.prepare_commit()
    writer.close()
    return messages


def committer(table, callback=None):
    return FileStoreCommit(
        table.new_snapshot_commit(), table, "batch-001",
        commit_callbacks=[] if callback is None else [callback])


def test_fresh_committer_recovers_published_batch(table):
    messages = prepare(table, "ep-1")
    properties = {"write_id": "write-001", "batch_id": "batch-001"}
    first = committer(table)
    try:
        first.commit(messages, 1, snapshot_properties=properties,
                     recovery_snapshot_id=0)
    finally:
        first.close()
    callback = Mock()
    resumed = committer(table, callback)
    try:
        resumed.commit(messages, 1, snapshot_properties=properties,
                       recovery_snapshot_id=0)
    finally:
        resumed.close()
    latest = table.snapshot_manager().get_latest_snapshot()
    assert latest.id == 1
    assert latest.total_record_count == 1
    assert callback.call.call_args[0][0].snapshot.id == 1


def test_recovery_rejects_replaced_base_snapshot(table):
    original = committer(table)
    original.commit(prepare(table, "ep-0"), 0)
    original.close()
    messages = prepare(table, "ep-1")
    resumed = committer(table)
    try:
        with pytest.raises(RuntimeError, match="base snapshot"):
            resumed.commit(
                messages, 1, snapshot_properties={"batch_id": "batch-001"},
                recovery_snapshot_id=1, recovery_snapshot_uuid="wrong-uuid")
    finally:
        resumed.close()
    assert table.snapshot_manager().get_latest_snapshot().id == 1


def test_empty_recoverable_batch_has_a_durable_receipt(table):
    callback = Mock()
    commit = committer(table, callback)
    try:
        commit.commit([], 1, snapshot_properties={"batch_id": "empty"},
                      recovery_snapshot_id=0)
    finally:
        commit.close()
    snapshot = table.snapshot_manager().get_latest_snapshot()
    assert snapshot is not None
    assert snapshot.total_record_count == 0
    assert callback.call.call_args[0][0].snapshot.id == snapshot.id


@pytest.mark.parametrize("base_id,base_uuid,properties", [
    (-1, None, {"batch_id": "b"}),
    (True, None, {"batch_id": "b"}),
    (1, None, {"batch_id": "b"}),
    (0, "unexpected", {"batch_id": "b"}),
    (0, None, None),
    (0, None, {}),
])
def test_recovery_rejects_incomplete_identity(table, base_id, base_uuid, properties):
    commit = committer(table)
    try:
        with pytest.raises(ValueError, match="Recovery"):
            commit.commit([], 1, snapshot_properties=properties,
                          recovery_snapshot_id=base_id,
                          recovery_snapshot_uuid=base_uuid)
    finally:
        commit.close()
    assert table.snapshot_manager().get_latest_snapshot() is None


def test_recovery_after_lost_publish_response(table):
    messages = prepare(table, "ep-1")
    props = {"batch_id": "batch-001"}
    first = committer(table)
    original_commit = first.snapshot_commit.commit

    def publish_then_disconnect(*args, **kwargs):
        assert original_commit(*args, **kwargs)
        raise OSError("response lost")

    first.snapshot_commit.commit = publish_then_disconnect
    first.commit_max_retries = 0
    with pytest.raises(RuntimeError, match="Commit failed"):
        first.commit(messages, 1, snapshot_properties=props,
                     recovery_snapshot_id=0)
    first.close()

    callback = Mock()
    resumed = committer(table, callback)
    resumed.commit(messages, 1, snapshot_properties=props,
                   recovery_snapshot_id=0)
    resumed.close()
    assert table.snapshot_manager().get_latest_snapshot().total_record_count == 1
    assert callback.call.call_args[0][0].snapshot.id == 1


def test_competing_committer_rechecks_identity_after_cas_conflict(table):
    messages = prepare(table, "ep-1")
    props = {"batch_id": "batch-001"}
    first = committer(table)
    second = committer(table)
    original_commit = first.snapshot_commit.commit
    raced = []

    def race_before_publish(*args, **kwargs):
        if not raced:
            raced.append(True)
            second.commit(messages, 1, snapshot_properties=props,
                          recovery_snapshot_id=0)
        return original_commit(*args, **kwargs)

    first.snapshot_commit.commit = race_before_publish
    first._commit_retry_wait = lambda _: None
    try:
        first.commit(messages, 1, snapshot_properties=props,
                     recovery_snapshot_id=0)
    finally:
        first.close()
        second.close()
    latest = table.snapshot_manager().get_latest_snapshot()
    assert latest.id == 1
    assert latest.total_record_count == 1


def test_recovery_rejects_expired_intervening_history(table):
    for identifier in (1, 2):
        commit = committer(table)
        commit.commit(prepare(table, "existing-{}".format(identifier)), identifier)
        commit.close()
    manager = table.snapshot_manager()
    table.file_io.delete(manager.get_snapshot_path(1))
    commit = committer(table)
    try:
        with pytest.raises(RuntimeError, match="snapshot 1 cannot be found"):
            commit.commit(prepare(table, "new"), 3,
                          snapshot_properties={"batch_id": "batch-003"},
                          recovery_snapshot_id=0)
    finally:
        commit.close()
    assert manager.get_latest_snapshot().total_record_count == 2


def test_recovery_rejects_same_identity_with_changed_members(table):
    messages = prepare(table, "ep-1")
    first = committer(table)
    first.commit(messages, 1, snapshot_properties={"members": "task-1"},
                 recovery_snapshot_id=0)
    first.close()
    resumed = committer(table)
    try:
        with pytest.raises(RuntimeError, match="different properties"):
            resumed.commit(messages, 1, snapshot_properties={"members": "task-2"},
                           recovery_snapshot_id=0)
    finally:
        resumed.close()
    assert table.snapshot_manager().get_latest_snapshot().id == 1


def test_recovery_rejects_delete_messages(table):
    from pypaimon.write.commit_message import CommitMessage

    messages = prepare(table, "ep-1")
    first = committer(table)
    first.commit(messages, 0)
    first.close()
    base = table.snapshot_manager().get_latest_snapshot()
    deletion = CommitMessage((), messages[0].bucket, [], deleted_files=messages[0].new_files)
    resumed = committer(table)
    try:
        with pytest.raises(ValueError, match="append-only"):
            resumed.commit([deletion], 1, snapshot_properties={"batch_id": "delete"},
                           recovery_snapshot_id=base.id,
                           recovery_snapshot_uuid=base.uuid)
    finally:
        resumed.close()
    assert table.snapshot_manager().get_latest_snapshot().total_record_count == 1


def test_two_simultaneous_recovery_calls_publish_once(table):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    messages = prepare(table, "ep-1")
    properties = {"batch_id": "batch-001", "tasks": "task-001"}
    barrier = Barrier(2, timeout=10)
    callbacks = [Mock(), Mock()]
    committers = [committer(table, callback) for callback in callbacks]
    for instance in committers:
        original = instance.snapshot_commit.commit

        def publish(*args, original=original, **kwargs):
            barrier.wait()
            return original(*args, **kwargs)

        instance.snapshot_commit.commit = publish
        instance._commit_retry_wait = lambda _: None

    def run(instance):
        try:
            instance.commit(messages, 1, snapshot_properties=properties,
                            recovery_snapshot_id=0)
        finally:
            instance.close()

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(run, committers))
    latest = table.snapshot_manager().get_latest_snapshot()
    assert latest.id == 1
    assert latest.total_record_count == 1
    assert [callback.call.call_args[0][0].snapshot.id for callback in callbacks] == [1, 1]


def test_expired_positive_anchor_fails_closed(table):
    initial = committer(table)
    initial.commit(prepare(table, "ep-0"), 0)
    initial.close()
    manager = table.snapshot_manager()
    base = manager.get_latest_snapshot()
    unrelated = FileStoreCommit(table.new_snapshot_commit(), table, "other-job")
    unrelated.commit(prepare(table, "other"), 0)
    unrelated.close()
    table.file_io.delete(manager.get_snapshot_path(base.id))

    resumed = committer(table)
    try:
        with pytest.raises(RuntimeError, match="base snapshot is missing"):
            resumed.commit(prepare(table, "ep-1"), 1,
                           snapshot_properties={"batch_id": "batch-001"},
                           recovery_snapshot_id=base.id,
                           recovery_snapshot_uuid=base.uuid)
    finally:
        resumed.close()
    assert manager.get_latest_snapshot().total_record_count == 2


@pytest.mark.parametrize("damage", ["missing", "truncated"])
def test_recovery_does_not_publish_damaged_prepared_file(table, damage):
    from pathlib import Path
    from pypaimon.utils.file_store_path_factory import canonical_data_file_path

    messages = prepare(table, "ep-1")
    message = messages[0]
    file = message.new_files[0]
    path = Path(canonical_data_file_path(
        table, message.partition, message.bucket, file.file_name))
    if damage == "missing":
        path.unlink()
    else:
        path.write_bytes(b"truncated")
    commit = committer(table)
    try:
        with pytest.raises((FileNotFoundError, RuntimeError), match="[Ff]ile|size"):
            commit.commit(messages, 1, snapshot_properties={"batch_id": "batch-001"},
                          recovery_snapshot_id=0)
    finally:
        commit.close()
    assert table.snapshot_manager().get_latest_snapshot() is None


def test_recovered_snapshot_does_not_require_original_data_files(table):
    from pathlib import Path
    from pypaimon.utils.file_store_path_factory import canonical_data_file_path

    messages = prepare(table, "ep-1")
    first = committer(table)
    first.commit(messages, 1, snapshot_properties={"batch_id": "batch-001"},
                 recovery_snapshot_id=0)
    first.close()
    # A later compaction can remove the original data while its commit snapshot
    # remains retained. A lost acknowledgement must still recover that receipt.
    message = messages[0]
    Path(canonical_data_file_path(
        table, message.partition, message.bucket, message.new_files[0].file_name)).unlink()
    callback = Mock()
    replay = committer(table, callback)
    try:
        replay.commit(messages, 1, snapshot_properties={"batch_id": "batch-001"},
                      recovery_snapshot_id=0)
    finally:
        replay.close()
    assert callback.call.call_args[0][0].snapshot.id == 1
    assert table.snapshot_manager().get_latest_snapshot().id == 1


def test_recovery_validates_external_file_at_physical_location(table, tmp_path):
    from pathlib import Path
    from pypaimon.utils.file_store_path_factory import canonical_data_file_path

    messages = prepare(table, "ep-1")
    message = messages[0]
    file = message.new_files[0]
    original = Path(canonical_data_file_path(
        table, message.partition, message.bucket, file.file_name))
    external = tmp_path / "external.parquet"
    original.rename(external)
    file.external_path = external.as_uri()
    commit = committer(table)
    try:
        commit.commit(messages, 1, snapshot_properties={"batch_id": "batch-001"},
                      recovery_snapshot_id=0)
    finally:
        commit.close()
    assert table.snapshot_manager().get_latest_snapshot().total_record_count == 1


@pytest.mark.parametrize("missing", [False, True])
def test_recovery_requires_prepared_row_sidecar(table, missing):
    from pathlib import Path
    from pypaimon.utils.file_store_path_factory import canonical_data_file_path

    table = table.copy({'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true',
                        'data-evolution.row-sidecar.enabled': 'true'})
    messages = prepare(table, "ep-1")
    message = messages[0]
    file = message.new_files[0]
    assert len(file.extra_files) == 1
    path = Path(canonical_data_file_path(
        table, message.partition, message.bucket, file.file_name))
    sidecar = path.parent / file.extra_files[0]
    assert sidecar.exists()
    if missing:
        sidecar.unlink()
    # Checkpoints deserialize metadata without the transient absolute file path.
    file.file_path = None
    commit = committer(table)
    try:
        if missing:
            with pytest.raises(FileNotFoundError, match="sidecar"):
                commit.commit(messages, 1, snapshot_properties={"batch_id": "batch-001"},
                              recovery_snapshot_id=0)
            assert table.snapshot_manager().get_latest_snapshot() is None
        else:
            commit.commit(messages, 1, snapshot_properties={"batch_id": "batch-001"},
                          recovery_snapshot_id=0)
            assert table.snapshot_manager().get_latest_snapshot().total_record_count == 1
    finally:
        commit.close()


def test_recovery_does_not_publish_missing_input_changelog(table):
    from pypaimon.utils.file_store_path_factory import canonical_data_file_path

    table = table.copy({'changelog-producer': 'input'})
    messages = prepare(table, "ep-1")
    message = messages[0]
    assert len(message.changelog_files) == 1
    file = message.changelog_files[0]
    path = file.physical_path() or canonical_data_file_path(
        table, message.partition, message.bucket, file.file_name)
    table.file_io.delete(path)
    commit = committer(table)
    try:
        with pytest.raises(FileNotFoundError, match="missing"):
            commit.commit(messages, 1, snapshot_properties={"batch_id": "batch-001"},
                          recovery_snapshot_id=0)
    finally:
        commit.close()
    assert table.snapshot_manager().get_latest_snapshot() is None
