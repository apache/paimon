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

"""File index predicate evaluation."""

import struct
from typing import Optional, Dict

from pypaimon.common.predicate import Predicate
from pypaimon.fileindex.file_index_format import FileIndexFormatReader
from pypaimon.fileindex.file_index_result import FileIndexResult, REMAIN, SKIP
from pypaimon.fileindex.bloom_filter import BloomFilter64, BitSet
from pypaimon.fileindex.fast_hash import get_hash_function


class FileIndexPredicate:
    """Evaluator for file index predicates."""

    def __init__(self, embedded_index: bytes, schema_fields: Dict):
        """
        Initialize file index predicate evaluator.

        Args:
            embedded_index: Embedded file index bytes from DataFileMeta
            schema_fields: Schema fields dictionary for type lookup
        """
        self.reader = FileIndexFormatReader(embedded_index)
        self.schema_fields = schema_fields

    def evaluate(self, predicate: Optional[Predicate]) -> FileIndexResult:
        """
        Evaluate predicate against file index.

        Args:
            predicate: Filter predicate to evaluate

        Returns:
            REMAIN if file might contain matching rows, SKIP if definitely not
        """
        if predicate is None:
            return REMAIN

        return self._evaluate_predicate(predicate)

    def _evaluate_predicate(self, predicate: Predicate) -> FileIndexResult:
        """Recursively evaluate predicate."""
        method = predicate.method

        # Handle AND
        if method == 'and':
            result = REMAIN
            for child in predicate.literals:
                child_result = self._evaluate_predicate(child)
                result = result.and_(child_result)
                if not result.remain():
                    return SKIP
            return result

        # Handle OR
        elif method == 'or':
            children = predicate.literals or []
            # An empty OR has no evidence to skip on; fail open. (Seeding with
            # SKIP and returning it for an empty list would be the only
            # non-fail-open path in this evaluator.)
            if not children:
                return REMAIN
            result = SKIP
            for child in children:
                child_result = self._evaluate_predicate(child)
                result = result.or_(child_result)
                if result.remain():
                    return REMAIN
            return result

        # Handle leaf predicates
        elif method in ('equal', 'in'):
            return self._evaluate_leaf(predicate)

        # Unsupported predicates: remain
        return REMAIN

    def _evaluate_leaf(self, predicate: Predicate) -> FileIndexResult:
        """Evaluate leaf predicate (equal, in)."""
        field_name = predicate.field
        literals = predicate.literals

        # Read bloom filter for this column
        bloom_bytes = self.reader.read_column_index(field_name, "bloom-filter")
        if bloom_bytes is None:
            return REMAIN

        # Parse bloom filter
        try:
            bloom_filter = self._parse_bloom_filter(bloom_bytes)
        except Exception:
            return REMAIN

        # Get hash function for field type
        field_type = self.schema_fields.get(field_name)
        if field_type is None:
            return REMAIN

        try:
            hash_fn = get_hash_function(field_type)
        except ValueError:
            return REMAIN

        # Check each literal
        if predicate.method == 'equal':
            # Equal: check single value
            if not literals:
                return REMAIN
            value = literals[0]
            if value is None:
                return REMAIN
            hash_val = self._safe_hash(hash_fn, value)
            if hash_val is None:
                # Could not hash the literal in a Java-compatible way (e.g. a
                # CLI string literal on a temporal column). Fail open.
                return REMAIN
            return REMAIN if bloom_filter.test_hash(hash_val) else SKIP

        elif predicate.method == 'in':
            # An empty IN has no value to probe; fail open rather than SKIP.
            if not literals:
                return REMAIN
            # IN: any literal matches -> REMAIN
            for value in literals:
                if value is None:
                    continue
                hash_val = self._safe_hash(hash_fn, value)
                if hash_val is None:
                    # Cannot evaluate this literal safely; keep the file.
                    return REMAIN
                if bloom_filter.test_hash(hash_val):
                    return REMAIN
            return SKIP

        return REMAIN

    @staticmethod
    def _safe_hash(hash_fn, value):
        """Hash ``value``, returning ``None`` if it cannot be hashed in a
        Java-compatible way.

        Keeping this fail-open inside the evaluator (rather than relying on
        ``FileScanner._test_file_index``'s broad ``except``) means a literal we
        cannot hash — for example a temporal column compared against a raw
        string literal from the CLI WHERE parser — degrades to REMAIN here,
        while genuine implementation bugs elsewhere still surface.
        """
        try:
            return hash_fn(value)
        except (TypeError, ValueError):
            return None

    def _parse_bloom_filter(self, bloom_bytes: bytes) -> BloomFilter64:
        """
        Parse bloom filter from bytes.

        Format: 4 bytes num_hash_functions (big-endian) + bitset bytes
        """
        # Read num_hash_functions (big-endian)
        num_hash = struct.unpack('>I', bloom_bytes[0:4])[0]

        # Create bitset from remaining bytes
        bitset_data = bytearray(bloom_bytes[4:])
        bitset = BitSet(bitset_data, 0)

        # Create bloom filter
        return BloomFilter64(num_hash, bitset)
