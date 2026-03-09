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
#  Unless required by applicable law or agreed to in writing,
#  distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
################################################################################

import json
import time
from dataclasses import dataclass
from typing import Optional

from pypaimon.common.file_io import FileIO


@dataclass
class Consumer:
    """
    Consumer which contains next snapshot.
    """

    next_snapshot: int

    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps({"nextSnapshot": self.next_snapshot})

    @classmethod
    def from_json(cls, json_str: str) -> 'Consumer':
        """Create instance from JSON string"""
        data = json.loads(json_str)
        return cls(next_snapshot=data["nextSnapshot"])

    @classmethod
    def from_path(cls, file_io: FileIO, path: str) -> Optional['Consumer']:
        """
        Read consumer from path with retry mechanism.

        Args:
            file_io: FileIO instance
            path: Path to consumer file

        Returns:
            Consumer if exists, None otherwise
        """
        retry_number = 0
        exception = None
        while retry_number < 10:
            try:
                content = file_io.read_file_utf8(path)
            except FileNotFoundError:
                return None
            except Exception as e:
                # For concurrent write or other IO errors, retry
                exception = e
                time.sleep(0.2)
                retry_number += 1
                continue

            try:
                return cls.from_json(content)
            except Exception as e:
                # JSON parsing error or other content issues - should retry
                # as the file might be in the middle of being written
                exception = e
                time.sleep(0.2)
                retry_number += 1
                continue

        raise RuntimeError(f"Retry fail after 10 times: {exception}")
