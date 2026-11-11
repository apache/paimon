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
from typing import Optional


class PolicyType(Enum):
    ROW_FILTER = "ROW_FILTER"
    COLUMN_MASKING = "COLUMN_MASKING"

    @staticmethod
    def from_string(value: Optional[str]) -> Optional["PolicyType"]:
        if value is None:
            return None
        name = value.upper()
        if name not in PolicyType.__members__:
            raise ValueError("No enum constant PolicyType.{}".format(name))
        return PolicyType[name]
