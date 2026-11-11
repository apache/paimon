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
from typing import FrozenSet

from pypaimon.management.java_string import is_blank, java_length
from pypaimon.management.resource_type import ResourceType


class PermissionAccess:
    MAX_LENGTH = 32

    ALL = "ALL"
    CREATEDATABASE = "CREATEDATABASE"
    DESCRIBE = "DESCRIBE"
    ALTER = "ALTER"
    DROP = "DROP"
    CREATETABLE = "CREATETABLE"
    CREATEFUNCTION = "CREATEFUNCTION"
    CREATEVIEW = "CREATEVIEW"
    LIST = "LIST"
    SELECT = "SELECT"
    UPDATE = "UPDATE"
    GRANT = "GRANT"

    _BUILT_INS = {
        ResourceType.CATALOG: frozenset((ALL, ALTER, DROP, GRANT, CREATEDATABASE)),
        ResourceType.CATALOG_ALL: frozenset((
            ALL, DESCRIBE, ALTER, DROP, GRANT, CREATETABLE, CREATEVIEW, CREATEFUNCTION, LIST,
            SELECT, UPDATE)),
        ResourceType.DATABASE: frozenset((
            ALL, DESCRIBE, ALTER, DROP, GRANT, CREATETABLE, CREATEVIEW, CREATEFUNCTION, LIST)),
        ResourceType.DATABASE_ALL: frozenset((ALL, SELECT, UPDATE, ALTER, DROP, GRANT)),
        ResourceType.TABLE: frozenset((ALL, SELECT, UPDATE, ALTER, DROP, GRANT)),
        ResourceType.COLUMN: frozenset((SELECT,)),
        ResourceType.VIEW: frozenset((ALL, SELECT, ALTER, DROP, GRANT)),
        ResourceType.FUNCTION: frozenset((ALL, SELECT, ALTER, DROP, GRANT)),
    }

    @staticmethod
    def canonicalize(access: str) -> str:
        if is_blank(access):
            raise ValueError("access cannot be empty.")
        if java_length(access) > PermissionAccess.MAX_LENGTH:
            raise ValueError(
                "access must contain at most {} characters.".format(PermissionAccess.MAX_LENGTH))
        # Upper-cased but not trimmed, as in Java.
        canonical = access.upper()
        if java_length(canonical) > PermissionAccess.MAX_LENGTH:
            raise ValueError(
                "access must contain at most {} characters after canonicalization."
                .format(PermissionAccess.MAX_LENGTH))
        if any(canonical in accesses for accesses in PermissionAccess._BUILT_INS.values()):
            return canonical
        raise ValueError("Unknown access '{}'.".format(canonical))

    @staticmethod
    def canonicalize_for(resource, access: str) -> str:
        if resource is None:
            raise ValueError("resource cannot be null")
        canonical = PermissionAccess.canonicalize(access)
        if canonical not in PermissionAccess._BUILT_INS[resource.get_type()]:
            raise ValueError(
                "Access '{}' is not valid for {}.".format(canonical, resource.get_type().name))
        return canonical

    @staticmethod
    def built_ins(resource_type: ResourceType) -> FrozenSet[str]:
        if resource_type is None:
            raise ValueError("resource type cannot be null")
        return PermissionAccess._BUILT_INS[resource_type]
