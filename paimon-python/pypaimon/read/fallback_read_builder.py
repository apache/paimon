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

"""Batch read builder honouring the ``scan.fallback-branch`` table option.

Mirrors Java's ``org.apache.paimon.table.FallbackReadFileStoreTable``:
partitions that exist in the main branch are read from main; partitions
that exist only in the fallback branch are read from the fallback branch.
All branches of a table share the same physical data files (only snapshot,
tag and schema metadata are branch scoped), and the fallback branch must
have the same row type as main, so a split planned on either branch can be
read with a single :class:`~pypaimon.read.table_read.TableRead`.
"""

from typing import Dict, List, TYPE_CHECKING, Union

from pypaimon.read.plan import Plan
from pypaimon.read.read_builder import ReadBuilder


if TYPE_CHECKING:
    from pypaimon.read.split import Split
    from pypaimon.read.table_read import TableRead
    from pypaimon.read.table_scan import TableScan
    from pypaimon.table.file_store_table import FileStoreTable


def _partition_key(split: "Split") -> tuple:
    """Return a hashable, branch-independent key for a split's partition."""
    partition = split.partition
    values = getattr(partition, "values", None)
    if values is None:
        return (partition,)
    return tuple(values)


class FallbackReadBuilder(ReadBuilder):
    """A :class:`ReadBuilder` that merges main and fallback-branch partitions.

    Configuration (filter, projection, read type, limit) is applied to both
    the main and the fallback builders so the fallback scan reads the same
    columns and rows as main.
    """

    def __init__(self, table: "FileStoreTable", fallback_branch: str):
        super().__init__(table)
        self._fallback_table = table.copy({"branch": fallback_branch})
        self._validate_same_row_type(table, self._fallback_table, fallback_branch)
        self._fallback_builder = ReadBuilder(self._fallback_table)

    @staticmethod
    def _validate_same_row_type(main_table, fallback_table, fallback_branch):
        main_fields = main_table.table_schema.fields
        fallback_fields = fallback_table.table_schema.fields
        same = len(main_fields) == len(fallback_fields) and all(
            m.name == f.name and str(m.type) == str(f.type)
            for m, f in zip(main_fields, fallback_fields)
        )
        if not same:
            raise ValueError(
                "The fallback branch must have the same schema as the main "
                "branch. This validation is triggered because "
                "'scan.fallback-branch' is configured to '{}'.".format(
                    fallback_branch))

    # -- configuration is mirrored onto the fallback builder ---------------

    def with_filter(self, predicate) -> "FallbackReadBuilder":
        super().with_filter(predicate)
        self._fallback_builder.with_filter(predicate)
        return self

    def with_partition_filter(self, partition_filter) -> "FallbackReadBuilder":
        super().with_partition_filter(partition_filter)
        self._fallback_builder.with_partition_filter(partition_filter)
        return self

    def with_projection(
        self, projection: Union[List[str], Dict[str, str]]
    ) -> "FallbackReadBuilder":
        super().with_projection(projection)
        self._fallback_builder.with_projection(projection)
        return self

    def with_read_type(self, read_type) -> "FallbackReadBuilder":
        super().with_read_type(read_type)
        self._fallback_builder.with_read_type(read_type)
        return self

    def with_limit(self, limit: int) -> "FallbackReadBuilder":
        super().with_limit(limit)
        self._fallback_builder.with_limit(limit)
        return self

    # -- planning merges the two branches ----------------------------------

    def new_scan(self) -> "TableScan":
        return _FallbackTableScan(
            super().new_scan(), self._fallback_builder.new_scan())

    def new_read(self) -> "TableRead":
        return super().new_read()


class _FallbackTableScan:
    """Wraps the main and fallback scans and merges their plans by partition."""

    def __init__(self, main_scan: "TableScan", fallback_scan: "TableScan"):
        self._main_scan = main_scan
        self._fallback_scan = fallback_scan

    def plan(self) -> Plan:
        main_plan = self._main_scan.plan()
        main_splits = list(main_plan.splits())
        main_partitions = {_partition_key(split) for split in main_splits}

        fallback_splits = [
            split
            for split in self._fallback_scan.plan().splits()
            if _partition_key(split) not in main_partitions
        ]

        return Plan(_splits=main_splits + fallback_splits,
                    snapshot_id=main_plan.snapshot_id)

    def __getattr__(self, name):
        # Forward any other scan API (shard/slice/stats/...) to the main scan.
        return getattr(self._main_scan, name)
