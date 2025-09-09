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

import heapq
from typing import Any, Callable, List, Optional

from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.schema.data_types import DataField, Keyword
from pypaimon.schema.table_schema import TableSchema
from pypaimon.table.row.internal_row import InternalRow
from pypaimon.table.row.key_value import KeyValue


class SortMergeReaderWithMinHeap(RecordReader):
    """SortMergeReader implemented with min-heap."""

    def __init__(self, readers: List[RecordReader[KeyValue]], schema: TableSchema):
        self.next_batch_readers = list(readers)
        self.merge_function = DeduplicateMergeFunction()

        if schema.partition_keys:
            trimmed_primary_keys = [pk for pk in schema.primary_keys if pk not in schema.partition_keys]
            if not trimmed_primary_keys:
                raise ValueError(f"Primary key constraint {schema.primary_keys} same with partition fields")
        else:
            trimmed_primary_keys = schema.primary_keys
        field_map = {field.name: field for field in schema.fields}
        key_schema = [field_map[name] for name in trimmed_primary_keys if name in field_map]
        self.key_comparator = builtin_key_comparator(key_schema)

        self.min_heap = []
        self.polled = []

    def read_batch(self) -> Optional[RecordIterator]:
        for reader in self.next_batch_readers:
            while True:
                iterator = reader.read_batch()
                if iterator is None:
                    reader.close()
                    break

                kv = iterator.next()
                if kv is not None:
                    element = Element(kv, iterator, reader)
                    entry = HeapEntry(kv.key, element, self.key_comparator)
                    heapq.heappush(self.min_heap, entry)
                    break

        self.next_batch_readers.clear()

        if not self.min_heap:
            return None

        return SortMergeIterator(
            self,
            self.polled,
            self.min_heap,
            self.merge_function,
            self.key_comparator,
        )

    def close(self):
        for reader in self.next_batch_readers:
            reader.close()

        for entry in self.min_heap:
            entry.element.reader.close()

        for element in self.polled:
            element.reader.close()


class SortMergeIterator(RecordIterator):
    def __init__(self, reader, polled: List['Element'], min_heap, merge_function,
                 key_comparator):
        self.reader = reader
        self.polled = polled
        self.min_heap = min_heap
        self.merge_function = merge_function
        self.key_comparator = key_comparator
        self.released = False

    def next(self):
        while True:
            if not self._next_impl():
                return None
            result = self.merge_function.get_result()
            if result is not None:
                return result

    def _next_impl(self):
        for element in self.polled:
            if element.update():
                entry = HeapEntry(element.kv.key, element, self.key_comparator)
                heapq.heappush(self.min_heap, entry)
        self.polled.clear()

        if not self.min_heap:
            return False

        self.merge_function.reset()
        key = self.min_heap[0].key
        while self.min_heap and self.key_comparator(key, self.min_heap[0].key) == 0:
            entry = heapq.heappop(self.min_heap)
            self.merge_function.add(entry.element.kv)
            self.polled.append(entry.element)

        return True


class DeduplicateMergeFunction:
    """A MergeFunction where key is primary key (unique) and value is the full record, only keep the latest one."""

    def __init__(self):
        self.latest_kv = None

    def reset(self) -> None:
        self.latest_kv = None

    def add(self, kv: KeyValue):
        self.latest_kv = kv

    def get_result(self) -> Optional[KeyValue]:
        return self.latest_kv


class Element:
    def __init__(self, kv: KeyValue, iterator: RecordIterator[KeyValue], reader: RecordReader[KeyValue]):
        self.kv = kv
        self.iterator = iterator
        self.reader = reader

    def update(self) -> bool:
        next_kv = self.iterator.next()
        if next_kv is not None:
            self.kv = next_kv
            return True

        self.iterator = self.reader.read_batch()
        if self.iterator is None:
            self.reader.close()
            return False

        next_kv_from_new_batch = self.iterator.next()
        self.kv = next_kv_from_new_batch
        return True


class HeapEntry:
    def __init__(self, key: InternalRow, element: Element, key_comparator):
        self.key = key
        self.element = element
        self.key_comparator = key_comparator

    def __lt__(self, other):
        result = self.key_comparator(self.key, other.key)
        if result < 0:
            return True
        elif result > 0:
            return False

        return self.element.kv.sequence_number < other.element.kv.sequence_number


def builtin_key_comparator(key_schema: List[DataField]) -> Callable[[Any, Any], int]:
    # Precompute comparability flags to avoid repeated type checks
    comparable_types = {member.value for member in Keyword if member is not Keyword.VARIANT}
    comparable_flags = [field.type.type.split(' ')[0] in comparable_types for field in key_schema]

    def comparator(key1: InternalRow, key2: InternalRow) -> int:
        if key1 is None and key2 is None:
            return 0
        if key1 is None:
            return -1
        if key2 is None:
            return 1
        for i, comparable in enumerate(comparable_flags):
            val1 = key1.get_field(i)
            val2 = key2.get_field(i)

            if val1 is None and val2 is None:
                continue
            if val1 is None:
                return -1
            if val2 is None:
                return 1

            if not comparable:
                raise ValueError(f"Unsupported {key_schema[i].type} comparison")

            if val1 < val2:
                return -1
            elif val1 > val2:
                return 1
        return 0

    return comparator
