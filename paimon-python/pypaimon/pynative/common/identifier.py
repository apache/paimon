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

from pypaimon.pynative.common.exception import PyNativeNotImplementedError

SYSTEM_TABLE_SPLITTER = '$'
SYSTEM_BRANCH_PREFIX = 'branch-'


class TableIdentifier:

    def __init__(self, full_name: str):
        self._full_name = full_name
        self._system_table = None
        self._branch = None

        parts = full_name.split('.')
        if len(parts) == 2:
            self._database = parts[0]
            self._object = parts[1]
        else:
            raise ValueError(f"Cannot get splits from '{full_name}' to get database and object")

        splits = self._object.split(SYSTEM_TABLE_SPLITTER)
        if len(splits) == 1:
            self._table = self._object
        elif len(splits) == 2:
            self._table = splits[0]
            if splits[1].startswith(SYSTEM_BRANCH_PREFIX):
                self._branch = splits[1][len(SYSTEM_BRANCH_PREFIX):]
            else:
                self._system_table = splits[1]
        elif len(splits) == 3:
            if not splits[1].startswith(SYSTEM_BRANCH_PREFIX):
                raise ValueError(f"System table can only contain one '{SYSTEM_TABLE_SPLITTER}' separator, "
                                 f"but this is: {self._object}")
            self._table = splits[0]
            self._branch = splits[1][len(SYSTEM_BRANCH_PREFIX):]
            self._system_table = splits[2]
        else:
            raise ValueError(f"Invalid object name: {self._object}")

        if self._system_table is not None:
            raise PyNativeNotImplementedError("SystemTable")

        elif self._branch is not None:
            raise PyNativeNotImplementedError("BranchTable")

    def get_database_name(self):
        return self._database

    def get_table_name(self):
        return self._table

    def get_full_name(self):
        return self._full_name
