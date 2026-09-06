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

"""Sequential processing of row-id ranges at file-group boundaries."""

from typing import Callable, Dict, List

from pypaimon.utils.range import Range
from pypaimon.utils.range_helper import RangeHelper

__all__ = ["process_row_id_ranges"]


def process_row_id_ranges(
    target: str,
    catalog_options: Dict[str, str],
    *,
    rows_per_commit: int,
    processor: Callable[[List[Range]], None]
) -> None:
    """Process the target's row-id file groups in sequential batches.

    The latest snapshot is planned once before processing starts. Files with
    overlapping row-id ranges (for example, a base file and its data-evolution
    files) form one indivisible file group. Adjacent groups are accumulated
    until their row count reaches ``rows_per_commit``, then ``processor`` is
    called synchronously with their inclusive :class:`Range` objects.

    A batch may exceed ``rows_per_commit`` because file groups are never split.
    The processor owns reading, distributed execution, committing, retries, and
    cleanup. Its exception is propagated immediately and later batches are not
    processed.
    """
    _validate_arguments(rows_per_commit, processor)

    from pypaimon.catalog.catalog_factory import CatalogFactory

    table = CatalogFactory.create(catalog_options).get_table(target)
    if not table.options.row_tracking_enabled():
        raise ValueError(
            "process_row_id_ranges requires 'row-tracking.enabled'='true' "
            "on '{}'.".format(target)
        )

    pending_ranges = []
    pending_rows = 0
    for row_id_range in _file_group_ranges(table):
        pending_ranges.append(row_id_range)
        pending_rows += row_id_range.count()
        if pending_rows >= rows_per_commit:
            processor(pending_ranges)
            pending_ranges = []
            pending_rows = 0

    if pending_ranges:
        processor(pending_ranges)


def _validate_arguments(rows_per_commit, processor) -> None:
    if (
        isinstance(rows_per_commit, bool)
        or not isinstance(rows_per_commit, int)
        or rows_per_commit <= 0
    ):
        raise ValueError("rows_per_commit must be a positive integer.")
    if not callable(processor):
        raise ValueError("processor must be callable.")


def _file_group_ranges(table) -> List[Range]:
    plan = table.new_read_builder().new_scan().plan_for_write()
    files = [data_file for split in plan.splits() for data_file in split.files]
    file_groups = RangeHelper(
        lambda data_file: data_file.non_null_row_id_range()
    ).merge_overlapping_ranges(files)

    ranges = []
    for file_group in file_groups:
        group_ranges = [
            data_file.non_null_row_id_range() for data_file in file_group
        ]
        ranges.append(
            Range(
                min(row_range.from_ for row_range in group_ranges),
                max(row_range.to for row_range in group_ranges),
            )
        )
    return sorted(ranges, key=lambda row_range: row_range.from_)
