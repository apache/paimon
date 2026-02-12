#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.

"""The util class of resolve snapshot from scan params for time travel."""

from typing import Optional

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.snapshot.snapshot import Snapshot
from pypaimon.tag.tag_manager import TagManager

SCAN_KEYS = [
    CoreOptions.SCAN_TAG_NAME.key()
]


class TimeTravelUtil:
    """The util class of resolve snapshot from scan params for time travel."""

    @staticmethod
    def try_travel_to_snapshot(
            options: Options,
            tag_manager: TagManager
    ) -> Optional[Snapshot]:
        """
        Try to travel to a snapshot based on the options.
        
        Supports the following time travel options:
        - scan.tag-name: Travel to a specific tag
        
        Args:
            options: The options containing time travel parameters
            tag_manager: The tag manager
            
        Returns:
            The Snapshot to travel to, or None if no time travel option is set.
            
        Raises:
            ValueError: If more than one time travel option is set
        """

        scan_handle_keys = [key for key in SCAN_KEYS if options.contains_key(key)]

        if not scan_handle_keys:
            return None

        if len(scan_handle_keys) > 1:
            raise ValueError(f"Only one of the following parameters may be set: {SCAN_KEYS}")

        key = scan_handle_keys[0]
        core_options = CoreOptions(options)

        if key == CoreOptions.SCAN_TAG_NAME.key():
            tag_name = core_options.scan_tag_name()
            tag = tag_manager.get(tag_name)
            if tag is None:
                raise ValueError(f"Tag '{tag_name}' doesn't exist.")
            return tag.trim_to_snapshot()
        else:
            raise ValueError(f"Unsupported time travel mode: {key}")
