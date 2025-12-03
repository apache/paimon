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
import random
from typing import List


class ExternalPathProvider:
    def __init__(self, external_table_paths: List[str], relative_bucket_path: str):
        self.external_table_paths = external_table_paths
        self.relative_bucket_path = relative_bucket_path
        self.position = random.randint(0, len(external_table_paths) - 1) if external_table_paths else 0

    def get_next_external_data_path(self, file_name: str) -> str:
        """
        Get the next external data path using round-robin strategy.
        """
        if not self.external_table_paths:
            raise ValueError("No external paths available")

        self.position += 1
        if self.position == len(self.external_table_paths):
            self.position = 0

        external_base = self.external_table_paths[self.position]
        if self.relative_bucket_path:
            return f"{external_base.rstrip('/')}/{self.relative_bucket_path.strip('/')}/{file_name}"
        else:
            return f"{external_base.rstrip('/')}/{file_name}"
