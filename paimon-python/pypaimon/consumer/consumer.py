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
"""Consumer dataclass for streaming read progress."""

import json
import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class Consumer:
    """Consumer which contains the next snapshot to be read."""

    next_snapshot: int

    def to_json(self) -> str:
        """Serialize to JSON."""
        return json.dumps({"nextSnapshot": self.next_snapshot})

    @staticmethod
    def from_json(json_str: str) -> 'Consumer':
        """Deserialize from JSON."""
        data = json.loads(json_str)
        return Consumer(next_snapshot=data["nextSnapshot"])

    @classmethod
    def from_path(cls, file_io, path: str) -> Optional['Consumer']:
        """
        Load consumer from file path with retry mechanism.

        Args:
            file_io: FileIO instance for reading files
            path: Path to consumer file

        Returns:
            Consumer instance if file exists, None otherwise

        Raises:
            RuntimeError: If retry fails after 10 attempts
        """
        retry_number = 0
        exception = None
        while retry_number < 10:
            try:
                content = file_io.read_file_utf8(path)
            except FileNotFoundError:
                return None
            except Exception as e:
                exception = e
                time.sleep(0.2)
                retry_number += 1
                continue
            try:
                return cls.from_json(content)
            except Exception as e:
                exception = e
                time.sleep(0.2)
                retry_number += 1
                continue
        raise RuntimeError(f"Retry fail after 10 times: {exception}")
