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

"""Structured explain result for Daft Paimon scans."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pypaimon.read.explain import ExplainResult


READER_MODE_NATIVE_PARQUET = "native_parquet"
READER_MODE_PYPAIMON_FALLBACK = "pypaimon_fallback"


@dataclass(frozen=True, slots=True)
class PaimonReaderSplitExplain:
    partition: dict[str, Any]
    bucket: int
    file_count: int
    row_count: int
    file_size: int
    reader_mode: str
    fallback_reason: str | None
    file_paths: list[str]


@dataclass(frozen=True, slots=True)
class PaimonScanExplain:
    paimon_scan: ExplainResult

    native_parquet_split_count: int = 0
    native_parquet_file_count: int = 0
    pypaimon_fallback_split_count: int = 0
    pypaimon_fallback_file_count: int = 0
    fallback_reasons: dict[str, int] = field(default_factory=dict)

    pushed_filters: list[str] = field(default_factory=list)
    remaining_filters: list[str] = field(default_factory=list)
    partition_filters: list[str] = field(default_factory=list)

    requested_columns: list[str] | None = None
    task_columns: list[str] | None = None
    fallback_read_columns: list[str] | None = None

    requested_limit: int | None = None
    source_limit: int | None = None
    limit_pushed: bool = False

    splits: list[PaimonReaderSplitExplain] | None = None

    @property
    def total_split_count(self) -> int:
        return self.native_parquet_split_count + self.pypaimon_fallback_split_count

    @property
    def total_file_count(self) -> int:
        return self.native_parquet_file_count + self.pypaimon_fallback_file_count

    def __str__(self) -> str:
        return render_daft_paimon_explain(self)


def render_daft_paimon_explain(result: PaimonScanExplain) -> str:
    out = []
    out.append("== Daft Paimon Scan ==")
    _line(out, "Native Parquet splits", _count_files(
        result.native_parquet_split_count,
        result.native_parquet_file_count,
    ))
    _line(out, "pypaimon fallback splits", _count_files(
        result.pypaimon_fallback_split_count,
        result.pypaimon_fallback_file_count,
    ))
    _line(out, "Fallback reasons", _format_reason_counts(result.fallback_reasons))
    _line(out, "Pushed filters", _format_list(result.pushed_filters))
    _line(out, "Remaining filters", _format_list(result.remaining_filters))
    _line(out, "Partition filters", _format_list(result.partition_filters))
    _line(out, "Requested columns", _format_optional_list(result.requested_columns, "<all columns>"))
    _line(out, "Task columns", _format_optional_list(result.task_columns, "<all columns>"))
    _line(out, "Fallback read columns", _format_optional_list(
        result.fallback_read_columns,
        "<all columns>",
    ))
    _line(out, "Limit", _format_limit(result))

    if result.splits is not None:
        out.append("")
        out.append("Splits:")
        for index, split in enumerate(result.splits):
            suffix = "" if split.fallback_reason is None else " ({})".format(split.fallback_reason)
            out.append(
                "  #{} bucket={} files={} rows={} size={} mode={}{}".format(
                    index,
                    split.bucket,
                    split.file_count,
                    split.row_count,
                    split.file_size,
                    split.reader_mode,
                    suffix,
                )
            )

    out.append("")
    out.append(str(result.paimon_scan).rstrip())
    return "\n".join(out)


def _line(out: list[str], key: str, value: str) -> None:
    out.append("{:<28} {}".format(key + ":", value))


def _count_files(split_count: int, file_count: int) -> str:
    return "{} ({} files)".format(split_count, file_count)


def _format_reason_counts(reasons: dict[str, int]) -> str:
    if not reasons:
        return "<none>"
    return ", ".join("{}: {}".format(reason, count) for reason, count in sorted(reasons.items()))


def _format_list(values: list[str]) -> str:
    if not values:
        return "<none>"
    return ", ".join(values)


def _format_optional_list(values: list[str] | None, empty: str) -> str:
    if values is None:
        return empty
    if not values:
        return "[]"
    return "[{}]".format(", ".join(values))


def _format_limit(result: PaimonScanExplain) -> str:
    if result.requested_limit is None:
        return "<none>"
    pushed = "pushed" if result.limit_pushed else "not pushed"
    source = "<none>" if result.source_limit is None else str(result.source_limit)
    return "requested {}, source {} ({})".format(result.requested_limit, source, pushed)
