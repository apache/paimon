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
"""
Consumer class for tracking streaming read progress.

A Consumer contains the next snapshot ID to be read. This is persisted to
the table's consumer directory to track progress across restarts and to
inform snapshot expiration which snapshots are still needed.
"""

import json
from dataclasses import dataclass


@dataclass
class Consumer:
    """
    Consumer which contains next snapshot.

    This is the Python equivalent of Java's Consumer class. It stores the
    next snapshot ID that should be read by this consumer.
    """

    next_snapshot: int

    def to_json(self) -> str:
        """
        Serialize the consumer to JSON.

        Returns:
            JSON string with nextSnapshot field
        """
        return json.dumps({"nextSnapshot": self.next_snapshot})

    @staticmethod
    def from_json(json_str: str) -> 'Consumer':
        """
        Deserialize a consumer from JSON.

        Args:
            json_str: JSON string with nextSnapshot field

        Returns:
            Consumer instance
        """
        data = json.loads(json_str)
        return Consumer(next_snapshot=data["nextSnapshot"])
