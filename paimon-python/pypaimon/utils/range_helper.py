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

"""A helper class to handle ranges.

Follows the design of Java's org.apache.paimon.utils.RangeHelper.
"""


class RangeHelper:
    """A helper class to handle ranges.

    Provides methods to check if all ranges are the same and to merge
    overlapping ranges into groups, preserving original order within groups.

    Args:
        range_function: A callable that extracts a Range from an element T.
    """

    def __init__(self, range_function):
        self._range_function = range_function

    def are_all_ranges_same(self, items):
        """Check if all items have the same range.

        Args:
            items: List of items to check.

        Returns:
            True if all items have the same range, False otherwise.
        """
        if not items:
            return True

        first = items[0]
        first_range = self._range_function(first)
        if first_range is None:
            return False

        for item in items[1:]:
            if item is None:
                return False
            current_range = self._range_function(item)
            if current_range is None:
                return False
            if current_range.from_ != first_range.from_ or current_range.to != first_range.to:
                return False

        return True

    def merge_overlapping_ranges(self, items):
        """Merge items with overlapping ranges into groups.

        Sorts items by range start, then merges overlapping groups.
        Within each group, items are sorted by their original index.

        Args:
            items: List of items with non-null ranges.

        Returns:
            List of groups, where each group is a list of items
            with overlapping ranges.
        """
        if not items:
            return []

        # Create indexed values to track original indices
        indexed = []
        for original_index, item in enumerate(items):
            item_range = self._range_function(item)
            if item_range is not None:
                indexed.append(_IndexedValue(item, item_range, original_index))

        if not indexed:
            return []

        # Sort by range start, then by range end
        indexed.sort(key=lambda iv: (iv.start(), iv.end()))

        groups = []
        current_group = [indexed[0]]
        current_end = indexed[0].end()

        # Iterate through sorted ranges and merge overlapping ones
        for i in range(1, len(indexed)):
            current = indexed[i]
            if current.start() <= current_end:
                current_group.append(current)
                if current.end() > current_end:
                    current_end = current.end()
            else:
                groups.append(current_group)
                current_group = [current]
                current_end = current.end()

        # Add the last group
        groups.append(current_group)

        # Convert groups to result, sorting each group by original index
        result = []
        for group in groups:
            group.sort(key=lambda iv: iv.original_index)
            result.append([iv.value for iv in group])

        return result


class _IndexedValue:
    """A helper class to track original indices during range merging."""

    def __init__(self, value, item_range, original_index):
        self.value = value
        self.range = item_range
        self.original_index = original_index

    def start(self):
        return self.range.from_

    def end(self):
        return self.range.to
