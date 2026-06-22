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

from typing import Dict, Optional


class Actions:
    """Actions for resource change."""
    FIELD_TYPE = "action"
    UPDATE_COMMENT_ACTION = "updateComment"
    UPDATE_URI_ACTION = "updateUri"


class ResourceChange:
    """Represents a change to a resource.

    Mirrors Java ``org.apache.paimon.resource.ResourceChange``.
    """

    def __init__(self, action: str):
        self._action = action

    @staticmethod
    def update_comment(comment: Optional[str]) -> "UpdateResourceComment":
        return UpdateResourceComment(comment)

    @staticmethod
    def update_uri(uri: str) -> "UpdateResourceUri":
        return UpdateResourceUri(uri)

    def to_dict(self) -> Dict:
        raise NotImplementedError

    @classmethod
    def from_dict(cls, data: Dict) -> "ResourceChange":
        action = data.get(Actions.FIELD_TYPE)
        if action == Actions.UPDATE_COMMENT_ACTION:
            return UpdateResourceComment(data.get("comment"))
        elif action == Actions.UPDATE_URI_ACTION:
            return UpdateResourceUri(data["uri"])
        else:
            raise ValueError("Unknown resource change action: {}".format(action))


class UpdateResourceComment(ResourceChange):
    """Update comment for resource change."""

    def __init__(self, comment: Optional[str]):
        super().__init__(Actions.UPDATE_COMMENT_ACTION)
        self.comment = comment

    def to_dict(self) -> Dict:
        return {Actions.FIELD_TYPE: self._action, "comment": self.comment}

    def __eq__(self, other):
        if not isinstance(other, UpdateResourceComment):
            return False
        return self.comment == other.comment

    def __hash__(self):
        return hash(self.comment)


class UpdateResourceUri(ResourceChange):
    """Update URI for resource change."""

    def __init__(self, uri: str):
        super().__init__(Actions.UPDATE_URI_ACTION)
        self.uri = uri

    def to_dict(self) -> Dict:
        return {Actions.FIELD_TYPE: self._action, "uri": self.uri}

    def __eq__(self, other):
        if not isinstance(other, UpdateResourceUri):
            return False
        return self.uri == other.uri

    def __hash__(self):
        return hash(self.uri)

