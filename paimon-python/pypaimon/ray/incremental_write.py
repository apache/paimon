# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Periodic commits for Ray Dataset writes."""

import logging
import time

logger = logging.getLogger(__name__)


def incremental_write_paimon(
        dataset,
        target,
        catalog_options,
        *,
        commit_interval_seconds,
        update_cols,
        concurrency=None,
        ray_remote_args=None):
    """Commit a primary-key update Dataset in time-based windows."""
    from pypaimon.catalog.catalog_factory import CatalogFactory
    from pypaimon.write.ray_datasink import _prepare_incremental_update

    if (isinstance(commit_interval_seconds, bool)
            or not isinstance(commit_interval_seconds, (int, float))
            or commit_interval_seconds <= 0):
        raise ValueError("commit_interval_seconds must be positive.")

    table = CatalogFactory.create(catalog_options).get_table(target)
    to_write_batch = _prepare_incremental_update(table, target, update_cols)
    dataset = dataset.map_batches(to_write_batch, batch_format="pyarrow")
    _write_dataset_periodically(
        dataset,
        table,
        commit_interval_seconds,
        concurrency,
        ray_remote_args,
    )


def _write_dataset_periodically(
        dataset, table, commit_interval_seconds, concurrency,
        ray_remote_args):
    from pypaimon.write.ray_datasink import _write_primary_key_groups

    committer = _PeriodicDatasetCommitter(
        table,
        table.snapshot_manager().get_latest_snapshot(),
        table.table_schema.id,
    )
    windows = _dataset_windows(dataset, commit_interval_seconds)
    try:
        try:
            for window in windows:
                _write_primary_key_groups(
                    window,
                    table,
                    overwrite=False,
                    static_partition=None,
                    concurrency=concurrency,
                    ray_remote_args=ray_remote_args,
                    on_group_result=committer.add_group,
                )
                committer.commit()
        except Exception:
            # Preserve successful groups produced before a worker failure.
            committer.commit()
            raise
    except Exception:
        committer.abort_pending()
        raise
    finally:
        windows.close()
        committer.close()


def _dataset_windows(dataset, interval):
    """Yield time windows without copying Ray blocks through the driver."""
    import ray.data

    bundles = []
    source = dataset.iter_internal_ref_bundles()
    last_window = time.monotonic()
    try:
        for bundle in source:
            bundles.append(bundle)
            if time.monotonic() - last_window < interval:
                continue
            current, bundles = bundles, []
            try:
                yield ray.data.from_arrow_refs([
                    ref for item in current for ref in item.block_refs])
            finally:
                for item in current:
                    item.destroy_if_owned()
            last_window = time.monotonic()
        if bundles:
            current, bundles = bundles, []
            try:
                yield ray.data.from_arrow_refs([
                    ref for item in current for ref in item.block_refs])
            finally:
                for item in current:
                    item.destroy_if_owned()
    finally:
        for item in bundles:
            item.destroy_if_owned()
        close = getattr(source, "close", None)
        if close is not None:
            close()


class _PeriodicDatasetCommitter:

    def __init__(self, table, base_snapshot, schema_id):
        self._table = table
        self._base_snapshot = base_snapshot
        self._schema_id = schema_id
        builder = table.new_stream_write_builder()
        self._commit_user = builder.commit_user
        self._commit = builder.new_commit()
        self._pending = []
        self._next_commit_id = 1

    def add_group(self, messages):
        self._pending.extend(
            message for message in messages if not message.is_empty())

    def commit(self):
        if not self._pending:
            return
        messages = self._pending
        self._pending = []
        commit_id = self._next_commit_id
        base_id = self._base_snapshot.id if self._base_snapshot else 0
        self._commit.protect_from_schema_changes(self._schema_id)
        self._commit.commit(messages, commit_id)
        committed = _find_committed_snapshot(
            self._table, self._commit_user, commit_id, base_id)
        if committed is None:
            raise RuntimeError("Committed periodic write snapshot is missing.")
        self._base_snapshot = committed
        _validate_schema(self._table, self._schema_id)
        self._next_commit_id += 1

    def abort_pending(self):
        if not self._pending:
            return
        messages = self._pending
        self._pending = []
        _abort_messages(self._table, messages)

    def close(self):
        try:
            self._commit.close()
        except Exception:
            logger.warning("Failed to close periodic write commit.", exc_info=True)


def _find_committed_snapshot(table, commit_user, commit_id, after_id):
    manager = table.snapshot_manager()
    latest = manager.get_latest_snapshot()
    if latest is None:
        return None
    for snapshot_id in range(latest.id, after_id, -1):
        snapshot = manager.get_snapshot_by_id(snapshot_id)
        if (snapshot is not None
                and snapshot.commit_user == commit_user
                and snapshot.commit_identifier == commit_id):
            return snapshot
    return None


def _abort_messages(table, messages):
    commit = table.new_batch_write_builder().new_commit()
    try:
        commit.abort(messages)
    finally:
        commit.close()


def _validate_schema(table, schema_id):
    latest = table.schema_manager.latest()
    if latest is None or latest.id != schema_id:
        raise RuntimeError("Target schema changed during incremental write.")
