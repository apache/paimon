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

"""Prunes sorted global index files by manifest min/max metadata."""

from typing import List, Optional, Tuple

from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.key_serializer import KeySerializer
from pypaimon.globalindex.sorted_index_file_meta import SortedIndexFileMeta


class SortedFileMetaSelector:
    """Selects candidate sorted global index files for a predicate."""

    def __init__(
        self,
        files: List[Tuple[GlobalIndexIOMeta, SortedIndexFileMeta]],
        key_serializer: KeySerializer,
    ):
        self._files = files
        self._key_serializer = key_serializer
        self._comparator = key_serializer.create_comparator()

    def _filter(self, predicate) -> List[GlobalIndexIOMeta]:
        return [io_meta for io_meta, idx_meta in self._files if predicate(idx_meta)]

    def select_is_null(self) -> List[GlobalIndexIOMeta]:
        return self._filter(lambda meta: meta.has_nulls)

    def select_is_not_null(self) -> List[GlobalIndexIOMeta]:
        return self._filter(lambda meta: not meta.only_nulls())

    def select_starts_with(self, literal) -> List[GlobalIndexIOMeta]:
        if literal is None:
            return []

        prefix = self._key_serializer.serialize(literal)
        if len(prefix) == 0:
            return self._filter(lambda meta: not meta.only_nulls())

        prefix_key = self._key_serializer.deserialize(prefix)
        upper_bound_bytes = self._prefix_upper_bound(prefix)
        upper_bound = (None if upper_bound_bytes is None
                       else self._key_serializer.deserialize(upper_bound_bytes))

        return self._filter(
            lambda meta: (
                not meta.only_nulls()
                and self._compare_last_key(meta, prefix_key) >= 0
                and (upper_bound is None
                     or self._compare_first_key(meta, upper_bound) < 0)))

    def select_ends_with(self, literal) -> List[GlobalIndexIOMeta]:
        return self._filter(lambda meta: literal is not None and not meta.only_nulls())

    def select_contains(self, literal) -> List[GlobalIndexIOMeta]:
        return self._filter(lambda meta: literal is not None and not meta.only_nulls())

    def select_like(self, literal) -> List[GlobalIndexIOMeta]:
        return self._filter(lambda meta: literal is not None and not meta.only_nulls())

    def select_less_than(self, literal) -> List[GlobalIndexIOMeta]:
        if literal is None:
            return []
        return self._filter(
            lambda meta: not meta.only_nulls()
            and self._compare_first_key(meta, literal) < 0)

    def select_greater_or_equal(self, literal) -> List[GlobalIndexIOMeta]:
        if literal is None:
            return []
        return self._filter(
            lambda meta: not meta.only_nulls()
            and self._compare_last_key(meta, literal) >= 0)

    def select_not_equal(self, literal) -> List[GlobalIndexIOMeta]:
        if literal is None:
            return []
        return self._filter(lambda meta: not meta.only_nulls())

    def select_less_or_equal(self, literal) -> List[GlobalIndexIOMeta]:
        if literal is None:
            return []
        return self._filter(
            lambda meta: not meta.only_nulls()
            and self._compare_first_key(meta, literal) <= 0)

    def select_equal(self, literal) -> List[GlobalIndexIOMeta]:
        if literal is None:
            return []
        return self._filter(
            lambda meta: not meta.only_nulls()
            and self._overlaps(meta, literal, literal))

    def select_greater_than(self, literal) -> List[GlobalIndexIOMeta]:
        if literal is None:
            return []
        return self._filter(
            lambda meta: not meta.only_nulls()
            and self._compare_last_key(meta, literal) > 0)

    def select_in(self, literals) -> List[GlobalIndexIOMeta]:
        return self._filter(lambda meta: self._matches_any_literal(meta, literals))

    def select_not_in(self, literals) -> List[GlobalIndexIOMeta]:
        if any(literal is None for literal in literals):
            return []
        return self._filter(lambda meta: not meta.only_nulls())

    def select_between(self, from_v, to_v) -> List[GlobalIndexIOMeta]:
        if from_v is None or to_v is None or self._comparator(from_v, to_v) > 0:
            return []
        return self._filter(
            lambda meta: not meta.only_nulls()
            and self._overlaps(meta, from_v, to_v))

    def select_or(self, children: List[Optional[List[GlobalIndexIOMeta]]]
                  ) -> Optional[List[GlobalIndexIOMeta]]:
        result = {}
        for child in children:
            if child is None:
                return None
            for io_meta in child:
                key = io_meta.external_path or io_meta.file_name
                result[key] = io_meta
        return list(result.values())

    def _matches_any_literal(self, meta: SortedIndexFileMeta, literals) -> bool:
        if meta.only_nulls():
            return False
        for literal in literals:
            if literal is not None and self._overlaps(meta, literal, literal):
                return True
        return False

    def _overlaps(self, meta: SortedIndexFileMeta, from_v, to_v) -> bool:
        return (self._comparator(from_v, self._key_serializer.deserialize(meta.last_key)) <= 0
                and self._comparator(to_v, self._key_serializer.deserialize(meta.first_key)) >= 0)

    def _compare_first_key(self, meta: SortedIndexFileMeta, literal) -> int:
        return self._comparator(self._key_serializer.deserialize(meta.first_key), literal)

    def _compare_last_key(self, meta: SortedIndexFileMeta, literal) -> int:
        return self._comparator(self._key_serializer.deserialize(meta.last_key), literal)

    @staticmethod
    def _prefix_upper_bound(prefix: bytes) -> Optional[bytes]:
        for index in range(len(prefix) - 1, -1, -1):
            unsigned_byte = prefix[index]
            if unsigned_byte != 0xFF:
                upper_bound = bytearray(prefix[:index + 1])
                upper_bound[index] = unsigned_byte + 1
                return bytes(upper_bound)
        return None
