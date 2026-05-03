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

from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.write.commit_message import CommitMessage


class CommitMessageSerializer:
    """Cross-process serializer for CommitMessage payloads.

    JSON-based on purpose: human-debuggable, version-tolerant across worker
    Python versions, and avoids the security/compat pitfalls of pickle when
    shipping CompactTask outputs from Ray workers back to the driver.
    """

    VERSION = 1

    @classmethod
    def serialize(cls, message: CommitMessage) -> bytes:
        return json.dumps(cls.to_dict(message), separators=(",", ":")).encode("utf-8")

    @classmethod
    def deserialize(cls, payload: bytes) -> CommitMessage:
        return cls.from_dict(json.loads(payload.decode("utf-8")))

    @classmethod
    def to_dict(cls, message: CommitMessage) -> Dict[str, Any]:
        return {
            "version": cls.VERSION,
            "partition": list(message.partition) if message.partition is not None else [],
            "bucket": message.bucket,
            "new_files": [f.to_dict() for f in message.new_files],
            "compact_before": [f.to_dict() for f in message.compact_before],
            "compact_after": [f.to_dict() for f in message.compact_after],
            "check_from_snapshot": message.check_from_snapshot,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> CommitMessage:
        version = data.get("version", cls.VERSION)
        if version != cls.VERSION:
            raise ValueError(
                f"Unsupported CommitMessage payload version: {version} (expected {cls.VERSION})"
            )
        return CommitMessage(
            partition=tuple(data.get("partition") or ()),
            bucket=data["bucket"],
            new_files=[DataFileMeta.from_dict(f) for f in data.get("new_files", [])],
            compact_before=[DataFileMeta.from_dict(f) for f in data.get("compact_before", [])],
            compact_after=[DataFileMeta.from_dict(f) for f in data.get("compact_after", [])],
            check_from_snapshot=data.get("check_from_snapshot", -1),
        )

    @classmethod
    def serialize_list(cls, messages: List[CommitMessage]) -> List[bytes]:
        return [cls.serialize(m) for m in messages]

    @classmethod
    def deserialize_list(cls, payloads: List[bytes]) -> List[CommitMessage]:
        return [cls.deserialize(p) for p in payloads]
