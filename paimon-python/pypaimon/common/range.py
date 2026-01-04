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
from typing import Optional

import portion


class Range:
    """
    A range class based on the portion library for interval operations.

    This class wraps portion.Interval to provide range operations like
    intersection checking for row ID ranges.
    """

    def __init__(self, start, end):
        """
        Create a closed range [start, end].

        Args:
            start: The start value of the range (inclusive)
            end: The end value of the range (inclusive)
        """
        self.start = start
        self.end = end
        # Create a closed interval [start, end]
        self._interval = portion.closed(start, end)

    @staticmethod
    def intersection(range1: 'Range', range2: 'Range') -> Optional['Range']:
        """
        Calculate the intersection of two ranges.

        Args:
            range1: The first Range object
            range2: The second Range object

        Returns:
            A new Range object representing the intersection, or None if ranges don't overlap
        """
        if range1 is None or range2 is None:
            return None

        # Calculate intersection using portion
        intersect = range1._interval & range2._interval

        # If intersection is empty, return None
        if intersect.empty:
            return None

        # Extract the bounds from the intersection
        # portion returns an Interval which may contain multiple atomic intervals
        # For our use case, we expect a single atomic interval
        if len(intersect) > 0:
            atomic = list(intersect)[0]
            return Range(atomic.lower, atomic.upper)

        return None

    def __repr__(self):
        return f"Range({self.start}, {self.end})"

    def __str__(self):
        return f"[{self.start}, {self.end}]"
