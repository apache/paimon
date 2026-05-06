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

import json
from typing import Any, Dict, List

from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.schema.data_file_meta import (DataFileMeta, decode_value,
                                                      encode_value)
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.compact_increment import CompactIncrement
from pypaimon.write.data_increment import DataIncrement


class CommitMessageSerializer:
    """Cross-process serializer for CommitMessage payloads.

    JSON-based on purpose: human-debuggable, version-tolerant across worker
    Python versions, and avoids the security/compat pitfalls of pickle when
    shipping CompactTask outputs from Ray workers back to the driver.

    The wire shape mirrors org.apache.paimon.table.sink.CommitMessageImpl:
    every message is (partition, bucket, total_buckets, data_increment,
    compact_increment), with each increment carrying its own new/deleted/
    changelog file lists plus index file deltas. Today the index slots are
    populated only by tables that opt into them; the serializer round-trips
    them either way so adding deletion vectors / global index later does
    not need a new payload version.
    """

    VERSION = 2

    @classmethod
    def serialize(cls, message: CommitMessage) -> bytes:
        return json.dumps(cls.to_dict(message), separators=(",", ":")).encode("utf-8")

    @classmethod
    def deserialize(cls, payload: bytes) -> CommitMessage:
        return cls.from_dict(json.loads(payload.decode("utf-8")))

    @classmethod
    def to_dict(cls, message: CommitMessage) -> Dict[str, Any]:
        partition = message.partition if message.partition is not None else ()
        return {
            "version": cls.VERSION,
            "partition": [encode_value(v) for v in partition],
            "bucket": message.bucket,
            "total_buckets": message.total_buckets,
            "data_increment": cls._data_increment_to_dict(message.data_increment),
            "compact_increment": cls._compact_increment_to_dict(message.compact_increment),
            "check_from_snapshot": message.check_from_snapshot,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> CommitMessage:
        version = data.get("version", cls.VERSION)
        if version != cls.VERSION:
            raise ValueError(
                f"Unsupported CommitMessage payload version: {version} (expected {cls.VERSION})"
            )
        partition_values = data.get("partition") or []
        return CommitMessage(
            partition=tuple(decode_value(v) for v in partition_values),
            bucket=data["bucket"],
            total_buckets=data.get("total_buckets"),
            data_increment=cls._data_increment_from_dict(data.get("data_increment")),
            compact_increment=cls._compact_increment_from_dict(data.get("compact_increment")),
            check_from_snapshot=data.get("check_from_snapshot", -1),
        )

    @classmethod
    def serialize_list(cls, messages: List[CommitMessage]) -> List[bytes]:
        return [cls.serialize(m) for m in messages]

    @classmethod
    def deserialize_list(cls, payloads: List[bytes]) -> List[CommitMessage]:
        return [cls.deserialize(p) for p in payloads]

    # ---- Increment helpers -------------------------------------------------

    @classmethod
    def _data_increment_to_dict(cls, inc: DataIncrement) -> Dict[str, Any]:
        return {
            "new_files": [f.to_dict() for f in inc.new_files],
            "deleted_files": [f.to_dict() for f in inc.deleted_files],
            "changelog_files": [f.to_dict() for f in inc.changelog_files],
            "new_index_files": [_index_file_to_dict(i) for i in inc.new_index_files],
            "deleted_index_files": [_index_file_to_dict(i) for i in inc.deleted_index_files],
        }

    @classmethod
    def _data_increment_from_dict(cls, data) -> DataIncrement:
        if not data:
            return DataIncrement()
        return DataIncrement(
            new_files=[DataFileMeta.from_dict(f) for f in data.get("new_files") or []],
            deleted_files=[DataFileMeta.from_dict(f) for f in data.get("deleted_files") or []],
            changelog_files=[DataFileMeta.from_dict(f) for f in data.get("changelog_files") or []],
            new_index_files=[_index_file_from_dict(i) for i in data.get("new_index_files") or []],
            deleted_index_files=[_index_file_from_dict(i) for i in data.get("deleted_index_files") or []],
        )

    @classmethod
    def _compact_increment_to_dict(cls, inc: CompactIncrement) -> Dict[str, Any]:
        return {
            "compact_before": [f.to_dict() for f in inc.compact_before],
            "compact_after": [f.to_dict() for f in inc.compact_after],
            "changelog_files": [f.to_dict() for f in inc.changelog_files],
            "new_index_files": [_index_file_to_dict(i) for i in inc.new_index_files],
            "deleted_index_files": [_index_file_to_dict(i) for i in inc.deleted_index_files],
        }

    @classmethod
    def _compact_increment_from_dict(cls, data) -> CompactIncrement:
        if not data:
            return CompactIncrement()
        return CompactIncrement(
            compact_before=[DataFileMeta.from_dict(f) for f in data.get("compact_before") or []],
            compact_after=[DataFileMeta.from_dict(f) for f in data.get("compact_after") or []],
            changelog_files=[DataFileMeta.from_dict(f) for f in data.get("changelog_files") or []],
            new_index_files=[_index_file_from_dict(i) for i in data.get("new_index_files") or []],
            deleted_index_files=[_index_file_from_dict(i) for i in data.get("deleted_index_files") or []],
        )


# IndexFileMeta has richer payloads (deletion vector ranges, global index
# meta) that aren't relevant to the basic compaction path yet — round-trip
# only the scalar identity fields here. Phase 6/7 (deletion vectors,
# changelog producer) will extend this to cover dv_ranges and
# global_index_meta as the rewriter starts producing them.
def _index_file_to_dict(idx: IndexFileMeta) -> Dict[str, Any]:
    return {
        "index_type": idx.index_type,
        "file_name": idx.file_name,
        "file_size": idx.file_size,
        "row_count": idx.row_count,
        "external_path": idx.external_path,
    }


def _index_file_from_dict(data: Dict[str, Any]) -> IndexFileMeta:
    return IndexFileMeta(
        index_type=data["index_type"],
        file_name=data["file_name"],
        file_size=data["file_size"],
        row_count=data["row_count"],
        external_path=data.get("external_path"),
    )
