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

from typing import Optional

from pypaimon.table.row.key_value import KeyValue


class FirstRowMergeFunction:
    """A MergeFunction where key is primary key (unique) and value is the
    full record, only keep the first one."""

    def __init__(self, ignore_delete: bool = False):
        self.ignore_delete = ignore_delete
        self.first: Optional[KeyValue] = None

    def reset(self) -> None:
        self.first = None

    def add(self, kv: KeyValue) -> None:
        if not kv.is_add():
            if self.ignore_delete:
                return
            raise ValueError(
                "By default, First row merge engine can not accept "
                "DELETE/UPDATE_BEFORE records.\n"
                "You can config 'ignore-delete' to ignore the "
                "DELETE/UPDATE_BEFORE records."
            )

        if self.first is None:
            self.first = kv

    def get_result(self) -> Optional[KeyValue]:
        return self.first
