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

from typing import Any, List

from pypaimon.schema.data_types import DataField
from pypaimon.table.row.generic_row import GenericRow


class PartitionInfo:
    """
    Partition information about how the row mapping of outer row.
    """

    def __init__(self, mapping: List[int], partition: GenericRow):
        self.mapping = mapping
        self.partition_values = partition.values
        self.partition_fields = partition.fields

    def size(self) -> int:
        return len(self.mapping) - 1

    def is_partition_row(self, pos: int) -> bool:
        return self.mapping[pos] < 0

    def get_real_index(self, pos: int) -> int:
        return abs(self.mapping[pos]) - 1

    def get_partition_value(self, pos: int) -> (Any, DataField):
        real_index = self.get_real_index(pos)
        return self.partition_values[real_index], self.partition_fields[real_index]
