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

from pypaimon.read.plan import Plan
from pypaimon.table.format.format_table import FormatTable


class FormatTableScan:

    def __init__(
        self,
        table: FormatTable,
        partition_filter: Optional[Dict[str, str]] = None,
    ):
        self.table = table
        self.partition_filter = partition_filter  # optional equality filter

    def plan(self) -> Plan:
        from pypaimon.execution.python.read.format_table_scan import plan
        return plan(self)
