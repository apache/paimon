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

from typing import Optional

from pypaimon.common.identifier import Identifier
from pypaimon.resource.resource_type import ResourceType


class Resource:
    """A resource provides basic abstraction for external resources managed by
    Paimon, such as files, archives, JARs, and Python scripts.

    Mirrors Java ``org.apache.paimon.resource.Resource``.
    """

    def name(self) -> str:
        """A name to identify this resource."""
        raise NotImplementedError

    def full_name(self) -> str:
        """Full name of the resource, default is database.resourceName."""
        raise NotImplementedError

    def comment(self) -> Optional[str]:
        """Optional comment describing this resource."""
        raise NotImplementedError

    def uri(self) -> str:
        """The URI pointing to the location of this resource."""
        raise NotImplementedError

    def size(self) -> int:
        """The size of this resource in bytes."""
        raise NotImplementedError

    def last_modified_time(self) -> int:
        """The last modified time of this resource in milliseconds since epoch."""
        raise NotImplementedError

    def resource_type(self) -> ResourceType:
        """The type of this resource."""
        raise NotImplementedError

    def to_bytes(self) -> bytes:
        """Returns the contents of this resource as bytes."""
        raise NotImplementedError

    def new_input_stream(self):
        """Opens a new input stream for this resource."""
        raise NotImplementedError

    @staticmethod
    def to_resource(
            resource_type: ResourceType,
            identifier: Identifier,
            comment: Optional[str],
            uri: str,
            size: int,
            last_modified_time: int,
            file_io=None,
    ) -> "Resource":
        """Creates a ``Resource`` instance based on the given ``ResourceType``."""
        name = identifier.get_object_name()
        if resource_type == ResourceType.FILE:
            return FileResource(identifier, comment, uri, size, last_modified_time, file_io)
        elif resource_type == ResourceType.ARCHIVE:
            return ArchiveResource(identifier, comment, uri, size, last_modified_time, file_io)
        elif resource_type == ResourceType.JAR:
            if not name.endswith(".jar"):
                raise ValueError(
                    "JAR resource name must end with '.jar', but got: {}".format(name))
            return JarResource(identifier, comment, uri, size, last_modified_time, file_io)
        elif resource_type == ResourceType.PY:
            if not name.endswith(".py"):
                raise ValueError(
                    "PY resource name must end with '.py', but got: {}".format(name))
            return PyResource(identifier, comment, uri, size, last_modified_time, file_io)
        else:
            raise ValueError("Unknown resource type: {}".format(resource_type))


class AbstractResource(Resource):
    """Abstract base implementation of ``Resource`` with common fields and accessors."""

    def __init__(
            self,
            identifier: Identifier,
            comment: Optional[str],
            uri: str,
            size: int,
            last_modified_time: int,
            file_io=None,
    ):
        self._identifier = identifier
        self._comment = comment
        self._uri = uri
        self._size = size
        self._last_modified_time = last_modified_time
        self._file_io = file_io

    def name(self) -> str:
        return self._identifier.get_object_name()

    def full_name(self) -> str:
        return self._identifier.get_full_name()

    def identifier(self) -> Identifier:
        return self._identifier

    def comment(self) -> Optional[str]:
        return self._comment

    def uri(self) -> str:
        return self._uri

    def size(self) -> int:
        return self._size

    def last_modified_time(self) -> int:
        return self._last_modified_time

    def to_bytes(self) -> bytes:
        with self.new_input_stream() as stream:
            return stream.read()

    def new_input_stream(self):
        if self._file_io is None:
            raise RuntimeError("FileIO is not available for resource: {}".format(self.full_name()))
        return self._file_io.new_input_stream(self._uri)

    def __eq__(self, other):
        if not isinstance(other, AbstractResource):
            return False
        return (self._size == other._size
                and self._last_modified_time == other._last_modified_time
                and self._identifier == other._identifier
                and self._comment == other._comment
                and self._uri == other._uri)

    def __hash__(self):
        return hash((self._identifier, self._comment, self._uri,
                     self._size, self._last_modified_time))


class FileResource(AbstractResource):
    """A ``Resource`` implementation for general file resources."""

    def resource_type(self) -> ResourceType:
        return ResourceType.FILE


class ArchiveResource(AbstractResource):
    """A ``Resource`` implementation for archive resources."""

    def resource_type(self) -> ResourceType:
        return ResourceType.ARCHIVE


class JarResource(AbstractResource):
    """A ``Resource`` implementation for JAR resources."""

    def resource_type(self) -> ResourceType:
        return ResourceType.JAR


class PyResource(AbstractResource):
    """A ``Resource`` implementation for Python resources."""

    def resource_type(self) -> ResourceType:
        return ResourceType.PY

