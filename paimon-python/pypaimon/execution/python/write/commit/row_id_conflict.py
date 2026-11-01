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

"""Read-only logical conflict checks for the selected Python backend."""

from pypaimon.manifest.manifest_list_manager import ManifestListManager
from pypaimon.manifest.schema.manifest_entry import ManifestEntry
from pypaimon.execution.python.write.commit.commit_scanner import CommitScanner
from pypaimon.execution.python.write.commit.conflict_detection import ConflictDetection
from pypaimon.table.row.generic_row import GenericRow


def check_row_id_conflicts(table, messages, latest_snapshot_id, check_from_snapshot):
    if not table.options.data_evolution_enabled():
        raise NotImplementedError('Row-ID conflict checks require a data-evolution table')
    if any(message.compact_before or message.compact_after or message.compact_changelog_files
           or message.compact_index_adds or message.compact_index_deletes for message in messages):
        raise NotImplementedError('Row-ID conflict checks do not accept compact increments')
    manager = table.snapshot_manager()
    latest = manager.get_snapshot_by_id(latest_snapshot_id)
    baseline = manager.get_snapshot_by_id(check_from_snapshot)
    if latest is None or baseline is None:
        raise ValueError('Row-ID conflict check snapshots must exist in the current table')
    current_schema = table.schema_manager.latest()
    if (baseline.schema_id != latest.schema_id
            or current_schema is not None and current_schema.id != table.table_schema.id):
        raise ValueError('Row-ID conflict checks require an unchanged, current table schema')
    entries = []
    for message in messages:
        partition = GenericRow(list(message.partition), table.partition_keys_fields)
        for kind, files in ((0, message.new_files), (1, message.deleted_files)):
            entries.extend(ManifestEntry(
                kind, partition, message.bucket,
                table.options.bucket() if message.total_buckets is None else message.total_buckets, file)
                for file in files)
    manifests = ManifestListManager(table)
    detection = ConflictDetection(True, manager, manifests, table, CommitScanner(table, manifests))
    detection._row_id_check_from_snapshot = check_from_snapshot
    conflict = detection.check_row_id_from_snapshot(latest, entries, check_compaction=False)
    if conflict is not None:
        raise conflict
