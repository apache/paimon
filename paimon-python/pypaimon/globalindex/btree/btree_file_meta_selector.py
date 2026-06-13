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

"""Prunes BTree index files by comparing predicate literals against file min/max keys."""

from typing import List, Optional, Tuple

from pypaimon.globalindex.btree.btree_index_meta import BTreeIndexMeta
from pypaimon.globalindex.btree.key_serializer import KeySerializer
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta


class BTreeFileMetaSelector:
    """Selects which BTree index files may contain rows matching a predicate.

    Each select_* method returns:
      - None: cannot evaluate (caller must visit all files)
      - []: all files pruned (result is empty)
      - [...]: only these files need to be visited
    """

    def __init__(self, files: List[Tuple[GlobalIndexIOMeta, BTreeIndexMeta]],
                 key_serializer: KeySerializer):
        self._files = files
        self._key_serializer = key_serializer
        self._comparator = key_serializer.create_comparator()

    def _filter(self, predicate) -> List[GlobalIndexIOMeta]:
        return [io_meta for io_meta, idx_meta in self._files if predicate(idx_meta)]

    def _all_non_null_files(self) -> List[GlobalIndexIOMeta]:
        return self._filter(lambda m: not m.only_nulls())

    def select_is_null(self) -> List[GlobalIndexIOMeta]:
        return self._filter(lambda m: m.has_nulls)

    def select_is_not_null(self) -> List[GlobalIndexIOMeta]:
        return self._filter(lambda m: not m.only_nulls())

    def select_equal(self, literal) -> List[GlobalIndexIOMeta]:
        def pred(meta):
            if meta.only_nulls():
                return False
            first = self._key_serializer.deserialize(meta.first_key)
            last = self._key_serializer.deserialize(meta.last_key)
            return self._comparator(literal, first) >= 0 and self._comparator(literal, last) <= 0
        return self._filter(pred)

    def select_not_equal(self, literal) -> Optional[List[GlobalIndexIOMeta]]:
        return None

    def select_less_than(self, literal) -> List[GlobalIndexIOMeta]:
        def pred(meta):
            if meta.only_nulls():
                return False
            first = self._key_serializer.deserialize(meta.first_key)
            return self._comparator(first, literal) < 0
        return self._filter(pred)

    def select_less_or_equal(self, literal) -> List[GlobalIndexIOMeta]:
        def pred(meta):
            if meta.only_nulls():
                return False
            first = self._key_serializer.deserialize(meta.first_key)
            return self._comparator(first, literal) <= 0
        return self._filter(pred)

    def select_greater_than(self, literal) -> List[GlobalIndexIOMeta]:
        def pred(meta):
            if meta.only_nulls():
                return False
            last = self._key_serializer.deserialize(meta.last_key)
            return self._comparator(last, literal) > 0
        return self._filter(pred)

    def select_greater_or_equal(self, literal) -> List[GlobalIndexIOMeta]:
        def pred(meta):
            if meta.only_nulls():
                return False
            last = self._key_serializer.deserialize(meta.last_key)
            return self._comparator(last, literal) >= 0
        return self._filter(pred)

    def select_in(self, literals) -> List[GlobalIndexIOMeta]:
        def pred(meta):
            if meta.only_nulls():
                return False
            first = self._key_serializer.deserialize(meta.first_key)
            last = self._key_serializer.deserialize(meta.last_key)
            for lit in literals:
                if self._comparator(lit, first) >= 0 and self._comparator(lit, last) <= 0:
                    return True
            return False
        return self._filter(pred)

    def select_not_in(self, literals) -> Optional[List[GlobalIndexIOMeta]]:
        return None

    def select_between(self, from_v, to_v) -> List[GlobalIndexIOMeta]:
        def pred(meta):
            if meta.only_nulls():
                return False
            first = self._key_serializer.deserialize(meta.first_key)
            last = self._key_serializer.deserialize(meta.last_key)
            return self._comparator(from_v, last) <= 0 and self._comparator(to_v, first) >= 0
        return self._filter(pred)

    def select_starts_with(self, literal) -> Optional[List[GlobalIndexIOMeta]]:
        return None

    def select_ends_with(self, literal) -> Optional[List[GlobalIndexIOMeta]]:
        return None

    def select_contains(self, literal) -> Optional[List[GlobalIndexIOMeta]]:
        return None

    def select_like(self, literal) -> Optional[List[GlobalIndexIOMeta]]:
        return None
