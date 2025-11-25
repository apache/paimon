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

        external_base = self.external_table_paths[self.position].rstrip('/')
        relative_path = self.relative_bucket_path.strip('/')
        
        # Construct full path
        if relative_path:
            full_path = f"{external_base}/{relative_path}/{file_name}"
        else:
            full_path = f"{external_base}/{file_name}"
        
        # Normalize path separators - handle scheme:// correctly
        if '://' in full_path:
            # For URLs with scheme, normalize after scheme
            parts = full_path.split('://', 1)
            scheme = parts[0]
            path_part = parts[1].replace('//', '/')
            full_path = f"{scheme}://{path_part}"
        else:
            # For regular paths, just normalize slashes
            full_path = full_path.replace('//', '/')
        
        return full_path
