"""
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

from abc import ABC, abstractmethod


class Instant(ABC):
    """Table rollback instant.

    Supports polymorphic JSON serialization via to_dict/from_dict

    Serialization format:
        SnapshotInstant: {"type": "snapshot", "snapshotId": 123}
        TagInstant:      {"type": "tag", "tagName": "test_tag"}
    """

    FIELD_TYPE = "type"
    TYPE_SNAPSHOT = "snapshot"
    TYPE_TAG = "tag"

    @staticmethod
    def snapshot(snapshot_id):
        """Create a SnapshotInstant.

        Args:
            snapshot_id: The snapshot ID to rollback to.

        Returns:
            A SnapshotInstant instance.
        """
        return SnapshotInstant(snapshot_id)

    @staticmethod
    def tag(tag_name):
        """Create a TagInstant.

        Args:
            tag_name: The tag name to rollback to.

        Returns:
            A TagInstant instance.
        """
        return TagInstant(tag_name)

    @abstractmethod
    def to_dict(self):
        """Serialize this Instant to a dictionary for JSON output."""

    @staticmethod
    def from_dict(data):
        """Deserialize an Instant from a dictionary.

        Args:
            data: A dictionary with a 'type' field indicating the instant type.

        Returns:
            A SnapshotInstant or TagInstant instance.

        Raises:
            ValueError: If the type field is missing or unknown.
        """
        instant_type = data.get(Instant.FIELD_TYPE)
        if instant_type == Instant.TYPE_SNAPSHOT:
            return SnapshotInstant(data[SnapshotInstant.FIELD_SNAPSHOT_ID])
        elif instant_type == Instant.TYPE_TAG:
            return TagInstant(data[TagInstant.FIELD_TAG_NAME])
        else:
            raise ValueError("Unknown instant type: {}".format(instant_type))


class SnapshotInstant(Instant):
    """Snapshot instant for table rollback."""

    FIELD_SNAPSHOT_ID = "snapshotId"

    def __init__(self, snapshot_id):
        self.snapshot_id = snapshot_id

    def to_dict(self):
        return {
            Instant.FIELD_TYPE: Instant.TYPE_SNAPSHOT,
            self.FIELD_SNAPSHOT_ID: self.snapshot_id,
        }

    def __eq__(self, other):
        if not isinstance(other, SnapshotInstant):
            return False
        return self.snapshot_id == other.snapshot_id

    def __hash__(self):
        return hash(self.snapshot_id)

    def __repr__(self):
        return "SnapshotInstant(snapshot_id={})".format(self.snapshot_id)


class TagInstant(Instant):
    """Tag instant for table rollback."""

    FIELD_TAG_NAME = "tagName"

    def __init__(self, tag_name):
        self.tag_name = tag_name

    def to_dict(self):
        return {
            Instant.FIELD_TYPE: Instant.TYPE_TAG,
            self.FIELD_TAG_NAME: self.tag_name,
        }

    def __eq__(self, other):
        if not isinstance(other, TagInstant):
            return False
        return self.tag_name == other.tag_name

    def __hash__(self):
        return hash(self.tag_name)

    def __repr__(self):
        return "TagInstant(tag_name={})".format(self.tag_name)
