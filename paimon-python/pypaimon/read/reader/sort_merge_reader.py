# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import heapq
from typing import Any, Callable, List, Optional

from pypaimon.read.reader.deduplicate_merge_function import \
    DeduplicateMergeFunction
from pypaimon.read.reader.iface.record_iterator import RecordIterator
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.schema.data_types import AtomicType, DataField, Keyword
from pypaimon.schema.table_schema import TableSchema
from pypaimon.table.row.internal_row import InternalRow
from pypaimon.table.row.key_value import KeyValue


class SortMergeReaderWithMinHeap(RecordReader):
    """SortMergeReader implemented with min-heap."""

    def __init__(self, readers: List[RecordReader[KeyValue]], schema: TableSchema,
                 merge_function: Optional[Any] = None,
                 seq_comparator: Optional[Callable[[Any, Any], int]] = None):
        self.next_batch_readers = list(readers)
        # Default to dedupe so callers that don't pass a merge_function
        # keep their old behaviour. The merge engine dispatch lives in
        # ``MergeFileSplitRead.section_reader_supplier`` for the read
        # path; tests or other ad-hoc callers can pass a different
        # implementation here.
        self.merge_function = merge_function if merge_function is not None else DeduplicateMergeFunction()
        # Optional user-defined sequence comparator (``sequence.field``).
        # When set, it breaks key-ties on the value row before the
        # file-level sequence number, mirroring Java's
        # ``SortMergeReaderWithMinHeap`` + ``UserDefinedSeqComparator``.
        # Built by the caller, which knows the value-side schema.
        self.seq_comparator = seq_comparator

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
                    entry = HeapEntry(kv.key, element, self.key_comparator,
                                      self.seq_comparator)
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
            self.seq_comparator,
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
                 key_comparator, seq_comparator=None):
        self.reader = reader
        self.polled = polled
        self.min_heap = min_heap
        self.merge_function = merge_function
        self.key_comparator = key_comparator
        self.seq_comparator = seq_comparator
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
                entry = HeapEntry(element.kv.key, element, self.key_comparator,
                                  self.seq_comparator)
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
    def __init__(self, key: InternalRow, element: Element, key_comparator,
                 seq_comparator=None):
        self.key = key
        self.element = element
        self.key_comparator = key_comparator
        self.seq_comparator = seq_comparator

    def __lt__(self, other):
        # Heap order mirrors Java ``SortMergeReaderWithMinHeap``: user key
        # -> user-defined sequence comparator (``sequence.field``) on the
        # value row -> file-level sequence number.
        result = self.key_comparator(self.key, other.key)
        if result == 0 and self.seq_comparator is not None:
            result = self.seq_comparator(
                self.element.kv.value, other.element.kv.value)
        if result == 0:
            result = self.element.kv.sequence_number - other.element.kv.sequence_number
        return result < 0


def _base_type_name(field: DataField) -> str:
    """Base type keyword of a field, stripping any ``(precision[, scale])``
    parameters and the ``NOT NULL`` suffix. E.g. ``DECIMAL(10, 2)`` and
    ``TIMESTAMP(6)`` map to ``DECIMAL`` / ``TIMESTAMP``.
    """
    return field.type.type.split('(')[0].split(' ')[0]


# Atomic type keywords pypaimon can order with Python's native comparison
# operators. VARIANT is atomic but has no ordering, so it is excluded --
# matching Java, which has no VARIANT sequence-field support.
_COMPARABLE_TYPE_NAMES = frozenset(
    member.value for member in Keyword if member is not Keyword.VARIANT)


def is_comparable_seq_field(field: DataField) -> bool:
    """Whether ``field`` can serve as a ``sequence.field`` for pypaimon's
    atomic comparator: it must be an ``AtomicType`` whose base type name is
    orderable. Complex types (ARRAY / MAP / ROW / ...) and the atomic-but-
    unorderable VARIANT both return ``False``. Used by the read-builder
    guard to reject unsupported sequence fields up front.
    """
    return (isinstance(field.type, AtomicType)
            and _base_type_name(field) in _COMPARABLE_TYPE_NAMES)


def _row_field_comparator(
        fields: List[DataField],
        indices: List[int],
        ascending: bool = True) -> Callable[[Any, Any], int]:
    """Build a comparator over two rows on the given ``indices`` (positions
    in ``fields`` / the row's ``get_field``), compared left-to-right.

    Shared by :func:`builtin_key_comparator` (all key fields, ascending) and
    :func:`builtin_seq_comparator` (the configured sequence fields, with
    sort-order). Comparability is precomputed once. ``None`` rows/values
    always sort first, independent of ``ascending`` -- only the comparison
    of two non-null values is reversed when ``ascending=False``. This
    mirrors Java ``GenerateUtils.generateRowCompare`` built with
    ``nullIsLast=false`` (see ``CodeGeneratorImpl#getSortSpec``), where
    descending order flips only the non-null value comparison and leaves
    nulls sorting first.
    """
    comparable_flags = [_base_type_name(fields[idx]) in _COMPARABLE_TYPE_NAMES for idx in indices]
    sign = 1 if ascending else -1

    def comparator(row1: InternalRow, row2: InternalRow) -> int:
        if row1 is None and row2 is None:
            return 0
        if row1 is None:
            return -1
        if row2 is None:
            return 1
        for pos, idx in enumerate(indices):
            val1 = row1.get_field(idx)
            val2 = row2.get_field(idx)

            if val1 is None and val2 is None:
                continue
            if val1 is None:
                return -1
            if val2 is None:
                return 1

            if not comparable_flags[pos]:
                raise ValueError(f"Unsupported {fields[idx].type} comparison")

            if val1 < val2:
                return -sign
            elif val1 > val2:
                return sign
        return 0

    return comparator


def builtin_key_comparator(key_schema: List[DataField]) -> Callable[[Any, Any], int]:
    return _row_field_comparator(key_schema, list(range(len(key_schema))))


def builtin_seq_comparator(
        value_fields: List[DataField],
        sequence_field_names: List[str],
        ascending: bool) -> Optional[Callable[[Any, Any], int]]:
    """Build a comparator for the user-defined ``sequence.field`` option.

    Compares two *value* rows (the value side of a ``KeyValue``) on the
    configured sequence fields, in declaration order, returning a negative
    / zero / positive int. Mirrors Java ``UserDefinedSeqComparator``:

    - ``sequence_field_names`` empty -> ``None`` (no comparator; the caller
      falls back to the file-level sequence number).
    - field names resolve to indices within the value row
      (``value_fields`` is the value-side schema, == ``read_type``);
      ``get_field(idx)`` indexes the value ``OffsetRow``.
    - multiple fields compared left-to-right.
    - ``ascending=False`` reverses only the non-null value comparison for
      each field; null ordering stays nulls-first regardless of sort order
      (mirroring Java's ``nullIsLast=false``). The value rows here carry a
      homogeneous sort order, so reversing the final non-null comparison is
      equivalent to Java reversing each field.

    A name that does not resolve raises ``ValueError`` -- the read path
    injects missing sequence fields into the projection before this runs,
    so a miss indicates a wiring bug rather than user error.

    A sequence field whose type pypaimon cannot order raises
    ``NotImplementedError``: complex types (ARRAY / VECTOR / MAP / MULTISET /
    ROW), which Java handles via ``RecordComparator``, and the atomic-but-
    unorderable VARIANT. pypaimon only implements atomic-type comparison
    here, so reject these explicitly rather than failing later with an
    obscure error.
    """
    if not sequence_field_names:
        return None

    name_to_index = {field.name: i for i, field in enumerate(value_fields)}
    indices = []
    for name in sequence_field_names:
        if name not in name_to_index:
            raise ValueError(
                f"sequence.field '{name}' not found in value fields "
                f"{[f.name for f in value_fields]}")
        idx = name_to_index[name]
        if not is_comparable_seq_field(value_fields[idx]):
            raise NotImplementedError(
                f"sequence.field '{name}' has unsupported type "
                f"{value_fields[idx].type}; pypaimon only supports orderable "
                f"atomic sequence-field types. Complex types (ARRAY / MAP / "
                f"ROW etc., handled by Java via RecordComparator) and VARIANT "
                f"are not supported -- open an issue to track support.")
        indices.append(idx)

    return _row_field_comparator(value_fields, indices, ascending)
