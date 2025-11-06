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
from typing import List
from typing import ClassVar

from pypaimon.table.row.generic_row import GenericRow
from pypaimon.table.row.internal_row import InternalRow


@dataclass
class SimpleStats:
    min_values: InternalRow
    max_values: InternalRow
    null_counts: List[int]

    _empty_stats: ClassVar[object] = None

    @classmethod
    def empty_stats(cls):
        if cls._empty_stats is None:
            min_values = GenericRow([], [])
            max_values = GenericRow([], [])
            cls._empty_stats = cls(min_values, max_values, None)
        return cls._empty_stats


SIMPLE_STATS_FIELDS = [
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

KEY_STATS_SCHEMA = {
    "type": "record",
    "name": "record_KEY_STATS",
    "fields": SIMPLE_STATS_FIELDS
}

VALUE_STATS_SCHEMA = {
    "type": "record",
    "name": "record_VALUE_STATS",
    "fields": SIMPLE_STATS_FIELDS
}

PARTITION_STATS_SCHEMA = {
    "type": "record",
    "name": "record_PARTITION_STATS",
    "fields": SIMPLE_STATS_FIELDS
}
