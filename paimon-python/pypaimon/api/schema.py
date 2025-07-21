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
#################################################################################

import pyarrow as pa

from typing import Optional, List


class Schema:
    """Schema of a table."""

    def __init__(self,
                 pa_schema: pa.Schema,
                 partition_keys: Optional[List[str]] = None,
                 primary_keys: Optional[List[str]] = None,
                 options: Optional[dict] = None,
                 comment: Optional[str] = None):
        self.pa_schema = pa_schema
        self.partition_keys = partition_keys
        self.primary_keys = primary_keys
        self.options = options
        self.comment = comment
