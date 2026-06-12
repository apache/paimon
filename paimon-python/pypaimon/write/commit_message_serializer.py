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

from pypaimon.globalindex.global_index_meta import GlobalIndexMeta
from pypaimon.index.deletion_vector_meta import DeletionVectorMeta
from pypaimon.index.index_file_meta import IndexFileMeta
from pypaimon.manifest.index_manifest_entry import IndexManifestEntry
from pypaimon.manifest.schema.data_file_meta import (DataFileMeta, _generic_row_from_dict,
                                                     _generic_row_to_dict, decode_value,
                                                     encode_value)
from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.compact_increment import CompactIncrement
from pypaimon.write.data_increment import DataIncrement


class CommitMessageSerializer:
    """JSON wire format for shipping CommitMessage between pypaimon processes
    (e.g. Ray driver ↔ workers).

    JSON on purpose: human-debuggable, tolerant of Python version drift, and
    avoids pickle's compat/security pitfalls when shipping CompactTask outputs
    from Ray workers back to the driver.

    Every message round-trips (partition, bucket, total_buckets, data_increment,
    compact_increment, check_from_snapshot, index_deletes), with each increment
    carrying its own new / deleted / changelog file lists plus index file deltas.
    index_deletes (the index manifest entries removed by a global-index update)
    is round-tripped too, so a message whose only content is index_deletes does
    not silently lose its deletions when shipped through this serializer.
    """

    # Wire format version; bump on incompatible payload changes.
    VERSION = 1

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
            "index_deletes": [cls._index_manifest_entry_to_dict(e) for e in message.index_deletes],
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
            index_deletes=[cls._index_manifest_entry_from_dict(e)
                           for e in (data.get("index_deletes") or [])],
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

    # ---- Index manifest entry helpers --------------------------------------

    @classmethod
    def _index_manifest_entry_to_dict(cls, entry: IndexManifestEntry) -> Dict[str, Any]:
        return {
            "kind": entry.kind,
            "partition": _generic_row_to_dict(entry.partition),
            "bucket": entry.bucket,
            "index_file": _index_file_to_dict(entry.index_file),
        }

    @classmethod
    def _index_manifest_entry_from_dict(cls, data: Dict[str, Any]) -> IndexManifestEntry:
        return IndexManifestEntry(
            kind=data["kind"],
            partition=_generic_row_from_dict(data.get("partition")),
            bucket=data["bucket"],
            index_file=_index_file_from_dict(data["index_file"]),
        )


# IndexFileMeta carries the scalar identity fields plus two richer payloads:
# dv_ranges (deletion-vector index) and global_index_meta (global index). All
# are round-tripped so a message carrying global-index deletes (or, later,
# deletion vectors) does not lose them when shipped through this serializer.
def _index_file_to_dict(idx: IndexFileMeta) -> Dict[str, Any]:
    return {
        "index_type": idx.index_type,
        "file_name": idx.file_name,
        "file_size": idx.file_size,
        "row_count": idx.row_count,
        "external_path": idx.external_path,
        "dv_ranges": _dv_ranges_to_list(idx.dv_ranges),
        "global_index_meta": _global_index_meta_to_dict(idx.global_index_meta),
    }


def _index_file_from_dict(data: Dict[str, Any]) -> IndexFileMeta:
    return IndexFileMeta(
        index_type=data["index_type"],
        file_name=data["file_name"],
        file_size=data["file_size"],
        row_count=data["row_count"],
        external_path=data.get("external_path"),
        dv_ranges=_dv_ranges_from_list(data.get("dv_ranges")),
        global_index_meta=_global_index_meta_from_dict(data.get("global_index_meta")),
    )


def _dv_ranges_to_list(dv_ranges):
    # In memory dv_ranges is keyed by data_file_name; the key is redundant with
    # DeletionVectorMeta.data_file_name, so serialize as a flat list and rebuild
    # the dict on the way back.
    if not dv_ranges:
        return None
    return [
        {
            "data_file_name": dv.data_file_name,
            "offset": dv.offset,
            "length": dv.length,
            "cardinality": dv.cardinality,
        }
        for dv in dv_ranges.values()
    ]


def _dv_ranges_from_list(data):
    if not data:
        return None
    ranges = {}
    for d in data:
        dv = DeletionVectorMeta(
            data_file_name=d["data_file_name"],
            offset=d["offset"],
            length=d["length"],
            cardinality=d.get("cardinality"),
        )
        ranges[dv.data_file_name] = dv
    return ranges


def _global_index_meta_to_dict(meta):
    if meta is None:
        return None
    return {
        "row_range_start": meta.row_range_start,
        "row_range_end": meta.row_range_end,
        "index_field_id": meta.index_field_id,
        "extra_field_ids": meta.extra_field_ids,
        # index_meta is raw bytes; encode_value tags it for JSON round-trip.
        "index_meta": encode_value(meta.index_meta),
    }


def _global_index_meta_from_dict(data):
    if data is None:
        return None
    return GlobalIndexMeta(
        row_range_start=data["row_range_start"],
        row_range_end=data["row_range_end"],
        index_field_id=data["index_field_id"],
        extra_field_ids=data.get("extra_field_ids"),
        index_meta=decode_value(data.get("index_meta")),
    )
