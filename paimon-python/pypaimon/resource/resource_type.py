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

from enum import Enum


class ResourceType(Enum):
    """Enumeration of resource types supported by Paimon.

    Mirrors Java ``org.apache.paimon.resource.ResourceType``.
    """

    #: A general file resource.
    FILE = "file"

    #: An archive resource (e.g., zip, tar).
    ARCHIVE = "archive"

    #: A JAR resource.
    JAR = "jar"

    #: A Python resource.
    PY = "py"

    def get_value(self) -> str:
        return self.value

    @staticmethod
    def from_value(value: str) -> "ResourceType":
        """Parse a string value to ``ResourceType``, case-insensitive."""
        if value is not None:
            for resource_type in ResourceType:
                if resource_type.value.lower() == value.lower():
                    return resource_type
        raise ValueError("Unknown resource type: {}".format(value))

    def __str__(self) -> str:
        return self.value

