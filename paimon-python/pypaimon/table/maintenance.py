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

from dataclasses import dataclass
from typing import Dict, Optional

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.predicate_builder import PredicateBuilder


@dataclass
class MaintenanceResult:
    """Summary for a table maintenance operation."""

    snapshot_id: Optional[int]
    rewritten_record_count: int
    rewritten_file_count: int


class TableMaintenance:
    """Maintenance operations for a file store table."""

    def __init__(self, table):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table

    def compact(self, partition: Optional[Dict[str, str]] = None) -> MaintenanceResult:
        """Rewrite current table data and commit it as a COMPACT snapshot.

        This native Python compaction rewrites current visible records for the whole table
        or one partition. Row tracking, data evolution, and deletion vectors need dedicated
        metadata handling and are intentionally rejected here.
        """
        self._check_supported("compact")
        before_snapshot = self.table.snapshot_manager().get_latest_snapshot()
        if before_snapshot is None:
            raise ValueError("Table has no snapshot. No need to compact.")

        commit_messages, rewritten_record_count, rewritten_file_count = self._rewrite(
            self.table,
            partition,
        )
        if not commit_messages:
            return MaintenanceResult(before_snapshot.id, 0, 0)

        table_commit = self.table.new_batch_write_builder().new_commit()
        try:
            table_commit.compact(partition, commit_messages)
        except Exception:
            table_commit.abort(commit_messages)
            raise
        finally:
            table_commit.close()

        latest_snapshot = self.table.snapshot_manager().get_latest_snapshot()
        return MaintenanceResult(
            latest_snapshot.id if latest_snapshot else None,
            rewritten_record_count,
            rewritten_file_count
        )

    def rescale_bucket(
            self,
            bucket_num: int,
            partition: Optional[Dict[str, str]] = None
    ) -> MaintenanceResult:
        """Rewrite data using a different bucket number and commit with overwrite."""
        if bucket_num <= 0:
            raise ValueError("bucket_num must be greater than 0.")
        self._check_supported("rescale_bucket")
        if self.table.partition_keys and partition is None:
            raise ValueError("partition must be specified for partitioned tables.")

        before_snapshot = self.table.snapshot_manager().get_latest_snapshot()
        if before_snapshot is None:
            raise ValueError("Table has no snapshot. No need to rescale.")

        rescaled_table = self.table.copy(
            {CoreOptions.BUCKET.key(): str(bucket_num)},
            allow_bucket_change=True
        )
        commit_messages, rewritten_record_count, rewritten_file_count = self._rewrite(
            rescaled_table,
            partition,
        )
        if not commit_messages:
            return MaintenanceResult(before_snapshot.id, 0, 0)

        table_commit = rescaled_table.new_batch_write_builder().overwrite(partition).new_commit()
        try:
            table_commit.commit(commit_messages)
        except Exception:
            table_commit.abort(commit_messages)
            raise
        finally:
            table_commit.close()

        latest_snapshot = self.table.snapshot_manager().get_latest_snapshot()
        return MaintenanceResult(
            latest_snapshot.id if latest_snapshot else None,
            rewritten_record_count,
            rewritten_file_count
        )

    def _rewrite(self, write_table, partition):
        read_builder = self.table.new_read_builder()
        predicate = self._partition_predicate(partition)
        if predicate is not None:
            read_builder = read_builder.with_filter(predicate)

        scan = read_builder.new_scan()
        splits = scan.plan().splits()
        if not splits:
            return [], 0, 0

        table_read = read_builder.new_read()
        table_write = write_table.new_batch_write_builder().new_write()

        rewritten_record_count = 0
        try:
            for split in splits:
                data = table_read.to_arrow([split])
                if data is None or data.num_rows == 0:
                    continue
                rewritten_record_count += data.num_rows
                table_write.write_arrow(data)
            commit_messages = table_write.prepare_commit()
        except Exception:
            table_write.close()
            raise

        rewritten_file_count = sum(len(msg.new_files) for msg in commit_messages)
        table_write.close()
        return commit_messages, rewritten_record_count, rewritten_file_count

    def _partition_predicate(self, partition):
        if not partition:
            return None

        partition_keys = set(self.table.partition_keys)
        for key in partition:
            if key not in partition_keys:
                raise ValueError(
                    "Partition spec key '{}' is not a partition column. Partition keys are: {}.".format(
                        key,
                        list(self.table.partition_keys)
                    )
                )

        predicate_builder = PredicateBuilder(self.table.fields)
        predicates = [
            predicate_builder.equal(key, value)
            for key, value in partition.items()
        ]
        return predicate_builder.and_predicates(predicates)

    def _check_supported(self, operation: str) -> None:
        if self.table.options.row_tracking_enabled():
            raise NotImplementedError(
                "{} does not support row-tracking tables yet.".format(operation)
            )
        if self.table.options.data_evolution_enabled():
            raise NotImplementedError(
                "{} does not support data-evolution tables yet.".format(operation)
            )
        if self.table.options.deletion_vectors_enabled():
            raise NotImplementedError(
                "{} does not support deletion-vector tables yet.".format(operation)
            )
