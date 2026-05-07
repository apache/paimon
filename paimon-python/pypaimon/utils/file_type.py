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
#  limitations under the License.
################################################################################

"""Classification of Paimon files."""

import logging
import os
from enum import Enum

logger = logging.getLogger(__name__)


class FileType(Enum):
    """Classification of Paimon files.

    - META: snapshot, schema, manifest, statistics, tag, changelog metadata,
            hint files, _SUCCESS, consumer, service files
    - DATA: data files and any unrecognized files (default)
    - BUCKET_INDEX: bucket level index files (Hash, DV)
    - GLOBAL_INDEX: table level global index files (btree, bitmap, lumina, tantivy)
    - FILE_INDEX: data-file index files (bloom filter, bitmap, etc.)
    """
    META = "META"
    DATA = "DATA"
    BUCKET_INDEX = "BUCKET_INDEX"
    GLOBAL_INDEX = "GLOBAL_INDEX"
    FILE_INDEX = "FILE_INDEX"

    def is_index(self) -> bool:
        return self in (FileType.BUCKET_INDEX, FileType.GLOBAL_INDEX, FileType.FILE_INDEX)

    @staticmethod
    def is_mutable(file_path: str) -> bool:
        name = os.path.basename(file_path)
        return name in ("EARLIEST", "LATEST")

    @staticmethod
    def classify(file_path: str) -> 'FileType':
        name = os.path.basename(file_path)
        name = FileType._unwrap_temp_file_name(name)

        if (name.startswith("snapshot-")
                or name.startswith("schema-")
                or name.startswith("stat-")
                or name.startswith("tag-")
                or name.startswith("consumer-")
                or name.startswith("service-")):
            return FileType.META

        if name.endswith(".index"):
            if "global-index-" in name:
                return FileType.GLOBAL_INDEX
            return FileType.FILE_INDEX

        if "manifest" in name:
            return FileType.META

        if name.startswith("index-"):
            return FileType.BUCKET_INDEX

        if name in ("EARLIEST", "LATEST"):
            return FileType.META

        if name == "_SUCCESS" or name.endswith("_SUCCESS"):
            return FileType.META

        if name.startswith("changelog-"):
            parent = os.path.basename(os.path.dirname(file_path))
            if parent == "changelog":
                return FileType.META

        return FileType.DATA

    @staticmethod
    def parse_whitelist(whitelist_str: str) -> set:
        mapping = {
            "meta": FileType.META,
            "global-index": FileType.GLOBAL_INDEX,
            "bucket-index": FileType.BUCKET_INDEX,
            "data": FileType.DATA,
            "file-index": FileType.FILE_INDEX,
        }
        result = set()
        for name in whitelist_str.split(","):
            name = name.strip()
            if name in mapping:
                result.add(mapping[name])
            elif name:
                logger.warning(
                    "Unknown file-cache.whitelist value '%s'. "
                    "Supported values: meta, global-index, bucket-index, data, file-index.",
                    name,
                )
        return result

    @staticmethod
    def _unwrap_temp_file_name(name: str) -> str:
        # format: .{originalName}.{UUID}.tmp
        # suffix ".{UUID}.tmp" is 41 chars: 1(dot) + 36(UUID) + 4(.tmp)
        if len(name) < 43 or name[0] != '.' or not name.endswith('.tmp'):
            return name
        dot_before_uuid = len(name) - 41
        if name[dot_before_uuid] != '.':
            return name
        return name[1:dot_before_uuid]
