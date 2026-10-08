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

"""Opt-in, atomic maintenance passes over explicitly configured global indexes."""

from pypaimon.globalindex.create_global_index import GlobalIndexBuilder
from pypaimon.index.index_file_handler import IndexFileHandler
from pypaimon.utils.range import Range
from pypaimon.write.global_index_update_checker import build_index_delete_msgs


class _RebuildGlobalIndexBuilder(GlobalIndexBuilder):
    def _unindexed_ranges(self, snapshot, partition_filter, field_id):
        next_row_id = getattr(snapshot, "next_row_id", None)
        return [Range(0, next_row_id - 1)] if next_row_id else []


def maintain_global_indexes(table, indexes, *, rebuild=False, partitions=None):
    """Build missing coverage, or atomically replace selected index definitions.

    ``indexes`` is a nonempty sequence of dictionaries containing ``column``,
    ``type`` and optional ``options``. Keep these definitions in the scheduler:
    index manifests alone cannot describe an index after an update drops it.
    Run another pass after appends or DROP_PARTITION_INDEX updates. IGNORE does
    not mark stale coverage; use ``rebuild=True`` after such updates.

    All definitions use one snapshot and one commit. A concurrent commit aborts
    publication rather than exposing obsolete replacements. The caller owns
    scheduling/retry; this function does not install a background service.
    Returns the number of newly published index files.
    """
    from pypaimon.snapshot.time_travel_util import SCAN_KEYS

    if not table.options.data_evolution_enabled():
        raise ValueError("Global index maintenance requires a data-evolution table.")
    if any(table.options.options.contains_key(key) for key in SCAN_KEYS):
        raise ValueError("Global index maintenance requires the latest table, not time travel.")
    if not isinstance(rebuild, bool):
        raise ValueError("rebuild must be a boolean.")
    definitions = list(indexes)
    if not definitions:
        raise ValueError("indexes must be nonempty.")
    snapshot = table.snapshot_manager().get_latest_snapshot()
    # The strict snapshot guard currently belongs to the Python committer.
    table = table.copy({"commit.native.enabled": "false"})._copy_with_snapshot(snapshot)
    builders = []
    identities = set()
    for definition in definitions:
        if (not isinstance(definition, dict) or not {"column", "type"}.issubset(definition)
                or set(definition) - {"column", "type", "options"}):
            raise ValueError("Each index requires column/type and optional options.")
        cls = _RebuildGlobalIndexBuilder if rebuild else GlobalIndexBuilder
        builder = cls(table, definition["column"], index_type=definition["type"],
                      options=definition.get("options"), partitions=partitions)
        identity = (builder._index_columns[0], builder._index_type)
        if identity in identities:
            raise ValueError("Duplicate index definition: {}".format(identity))
        identities.add(identity)
        builders.append(builder)
    if snapshot is None:
        return 0

    messages = []
    try:
        for builder in builders:
            messages.extend(builder.build())
        if rebuild:
            entries = IndexFileHandler(table).scan(snapshot)
            deletes = []
            for builder in builders:
                field_id = table.field_dict[builder._index_columns[0]].id
                partition_filter = builder._resolve_partition_filter()
                for entry in entries:
                    meta = entry.index_file.global_index_meta
                    if (meta is not None and meta.index_field_id == field_id
                            and not meta.extra_field_ids and entry.index_file.index_type == builder._index_type
                            and (partition_filter is None or partition_filter.test(entry.partition))):
                        deletes.append(entry)
            messages.extend(build_index_delete_msgs(deletes))
    except BaseException:
        # No commit was attempted; only files from completed build messages are
        # ours to remove. The failing builder cleans its own incomplete shards.
        _cleanup_uncommitted(table, messages)
        raise
    if not messages:
        return 0
    for message in messages:
        message.check_from_snapshot = snapshot.id
    commit = table.new_batch_write_builder().new_commit()
    try:
        commit.commit(messages)
    finally:
        # A publication error can be ambiguous: never delete files here which a
        # successful snapshot might already reference. Orphan cleanup handles it.
        commit.close()
    return sum(len(message.index_adds) for message in messages)


def _cleanup_uncommitted(table, messages):
    paths = table.path_factory().global_index_path_factory()
    for message in messages:
        for entry in message.index_adds:
            file = entry.index_file
            table.file_io.delete_quietly(file.external_path or paths.to_path(file.file_name))
