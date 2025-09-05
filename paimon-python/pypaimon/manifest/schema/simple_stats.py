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

from dataclasses import dataclass
from typing import List, Optional

from pypaimon.table.row.binary_row import BinaryRow


@dataclass
class SimpleStats:
    min_values: BinaryRow
    max_values: BinaryRow
    null_counts: Optional[List[int]]


SIMPLE_STATS_SCHEMA = {
    "type": "record",
    "name": "SimpleStats",
    "fields": [
        {"name": "_MIN_VALUES", "type": "bytes"},
        {"name": "_MAX_VALUES", "type": "bytes"},
        {"name": "_NULL_COUNTS",
         "type": [
             "null",
             {
                 "type": "array",
                 "items": ["null", "long"]
             }
         ],
         "default": None},
    ]
}
