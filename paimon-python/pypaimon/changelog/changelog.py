################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and
#  limitations under the License.
################################################################################

import logging

from pypaimon.common.file_io import FileIO
from pypaimon.common.json_util import JSON
from pypaimon.snapshot.snapshot import Snapshot

logger = logging.getLogger(__name__)


class Changelog(Snapshot):
    """The metadata of changelog.

    Changelog extends Snapshot with the same fields. It generates from the snapshot
    file during expiration so that changelog of table can outlive snapshot's lifecycle.
    """

    @staticmethod
    def from_snapshot(snapshot: Snapshot) -> 'Changelog':
        """Create a Changelog from a Snapshot instance."""
        return Changelog(
            version=snapshot.version,
            id=snapshot.id,
            schema_id=snapshot.schema_id,
            base_manifest_list=snapshot.base_manifest_list,
            base_manifest_list_size=snapshot.base_manifest_list_size,
            delta_manifest_list=snapshot.delta_manifest_list,
            delta_manifest_list_size=snapshot.delta_manifest_list_size,
            changelog_manifest_list=snapshot.changelog_manifest_list,
            changelog_manifest_list_size=snapshot.changelog_manifest_list_size,
            index_manifest=snapshot.index_manifest,
            commit_user=snapshot.commit_user,
            commit_identifier=snapshot.commit_identifier,
            commit_kind=snapshot.commit_kind,
            time_millis=snapshot.time_millis,
            total_record_count=snapshot.total_record_count,
            delta_record_count=snapshot.delta_record_count,
            changelog_record_count=snapshot.changelog_record_count,
            watermark=snapshot.watermark,
            statistics=snapshot.statistics,
            properties=snapshot.properties,
            next_row_id=snapshot.next_row_id
        )

    @staticmethod
    def from_json(json_str: str) -> 'Changelog':
        """Create a Changelog from JSON string."""
        return JSON.from_json(json_str, Changelog)

    @staticmethod
    def from_path(file_io: FileIO, path: str) -> 'Changelog':
        """Create a Changelog from a file path. Raises RuntimeError if file doesn't exist."""
        try:
            return Changelog.try_from_path(file_io, path)
        except FileNotFoundError as e:
            raise RuntimeError(f"Failed to read changelog from path {path}") from e

    @staticmethod
    def try_from_path(file_io: FileIO, path: str) -> 'Changelog':
        """Create a Changelog from a file path. Raises FileNotFoundError if file doesn't exist."""
        try:
            json_str = file_io.read_file_utf8(path)
            return Changelog.from_json(json_str)
        except FileNotFoundError as e:
            raise e
        except Exception as e:
            raise RuntimeError(f"Failed to read changelog from path {path}") from e
