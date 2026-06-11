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

"""Type-cast support rules used to validate ``update column type`` schema
changes.

The rules mirror the engine-wide cast specification so a type change accepted
here is one the read path can also materialize: an *implicit* cast is a safe
widening (e.g. INT -> BIGINT, any numeric -> DECIMAL/DOUBLE), while an
*explicit* cast covers the broader, possibly lossy conversions a user opts into
(e.g. DOUBLE -> INT truncation, anything -> STRING). Read-time execution then
applies the conversion leniently.
"""

from pypaimon.schema.data_types import (ArrayType, AtomicType, MapType,
                                        MultisetType, RowType, VectorType)

# ---- Type roots --------------------------------------------------------------

CHAR = "CHAR"
VARCHAR = "VARCHAR"
BOOLEAN = "BOOLEAN"
BINARY = "BINARY"
VARBINARY = "VARBINARY"
DECIMAL = "DECIMAL"
TINYINT = "TINYINT"
SMALLINT = "SMALLINT"
INTEGER = "INTEGER"
BIGINT = "BIGINT"
FLOAT = "FLOAT"
DOUBLE = "DOUBLE"
DATE = "DATE"
TIME = "TIME"
TIMESTAMP = "TIMESTAMP"
TIMESTAMP_LTZ = "TIMESTAMP_LTZ"
ARRAY = "ARRAY"
MAP = "MAP"
MULTISET = "MULTISET"
ROW = "ROW"
VECTOR = "VECTOR"
VARIANT = "VARIANT"
BLOB = "BLOB"

# ---- Families ----------------------------------------------------------------

CHARACTER_STRING = {CHAR, VARCHAR}
BINARY_STRING = {BINARY, VARBINARY}
INTEGER_NUMERIC = {TINYINT, SMALLINT, INTEGER, BIGINT}
NUMERIC = INTEGER_NUMERIC | {FLOAT, DOUBLE, DECIMAL}
TIMESTAMP_FAMILY = {TIMESTAMP, TIMESTAMP_LTZ}
TIME_FAMILY = {TIME}
DATETIME = {DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ}
PREDEFINED = {
    CHAR, VARCHAR, BOOLEAN, BINARY, VARBINARY, DECIMAL,
    TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DOUBLE,
    DATE, TIME, TIMESTAMP, TIMESTAMP_LTZ,
}
CONSTRUCTED = {ARRAY, MAP, MULTISET, ROW, VECTOR}
# Constructed types the read path can render as a character string
# ('{v1, v2}' / '[e1, e2]' / '{k -> v}'). VECTOR and MULTISET have no string
# rendering, so a type change from them to CHAR/VARCHAR is rejected here
# rather than failing when an old file is read.
STRING_RENDERABLE_CONSTRUCTED = {ARRAY, MAP, ROW}


def _root(data_type) -> str:
    if isinstance(data_type, RowType):
        return ROW
    if isinstance(data_type, ArrayType):
        return ARRAY
    if isinstance(data_type, MapType):
        return MAP
    if isinstance(data_type, MultisetType):
        return MULTISET
    if isinstance(data_type, VectorType):
        return VECTOR
    if isinstance(data_type, AtomicType):
        t = data_type.type.upper()
        if t.startswith("DECIMAL") or t.startswith("NUMERIC") or t.startswith("DEC"):
            return DECIMAL
        if t in ("INT", "INTEGER"):
            return INTEGER
        if t in (TINYINT, SMALLINT, BIGINT, FLOAT, DOUBLE, BOOLEAN, DATE):
            return t
        if t == "STRING" or t.startswith("VARCHAR"):
            return VARCHAR
        if t.startswith("CHAR"):
            return CHAR
        if t == "BYTES" or t.startswith("VARBINARY"):
            return VARBINARY
        if t.startswith("BINARY"):
            return BINARY
        if t == "BLOB":
            return BLOB
        if t.startswith("TIMESTAMP_LTZ"):
            return TIMESTAMP_LTZ
        if t.startswith("TIMESTAMP"):
            return TIMESTAMP
        if t.startswith("TIME"):
            return TIME
        if t == "VARIANT":
            return VARIANT
    return None


def _build_rules():
    implicit = {}
    explicit = {}
    # Identity cast for every root.
    for root in (PREDEFINED | CONSTRUCTED | {VARIANT, BLOB}):
        implicit[root] = {root}
        explicit[root] = set()

    def rule(target, implicit_from=None, explicit_from=None):
        implicit[target] |= set(implicit_from or set())
        explicit[target] |= set(explicit_from or set())

    rule(CHAR, {CHAR}, PREDEFINED | STRING_RENDERABLE_CONSTRUCTED)
    rule(VARCHAR, CHARACTER_STRING, PREDEFINED | STRING_RENDERABLE_CONSTRUCTED)
    rule(BOOLEAN, {BOOLEAN}, CHARACTER_STRING | INTEGER_NUMERIC)
    rule(BINARY, {BINARY}, CHARACTER_STRING | {VARBINARY})
    rule(VARBINARY, BINARY_STRING, CHARACTER_STRING | {BINARY})
    rule(DECIMAL, NUMERIC, CHARACTER_STRING | {BOOLEAN, TIMESTAMP, TIMESTAMP_LTZ})
    int_explicit = NUMERIC | CHARACTER_STRING | {BOOLEAN, TIMESTAMP, TIMESTAMP_LTZ}
    rule(TINYINT, {TINYINT}, int_explicit)
    rule(SMALLINT, {TINYINT, SMALLINT}, int_explicit)
    rule(INTEGER, {TINYINT, SMALLINT, INTEGER}, int_explicit)
    rule(BIGINT, {TINYINT, SMALLINT, INTEGER, BIGINT}, int_explicit)
    rule(FLOAT, {TINYINT, SMALLINT, INTEGER, BIGINT, FLOAT, DECIMAL}, int_explicit)
    rule(DOUBLE, NUMERIC, CHARACTER_STRING | {BOOLEAN, TIMESTAMP, TIMESTAMP_LTZ})
    rule(DATE, {DATE, TIMESTAMP}, TIMESTAMP_FAMILY | CHARACTER_STRING)
    rule(TIME, {TIME, TIMESTAMP}, TIME_FAMILY | TIMESTAMP_FAMILY | CHARACTER_STRING)
    rule(TIMESTAMP, {TIMESTAMP, TIMESTAMP_LTZ}, DATETIME | CHARACTER_STRING | NUMERIC)
    rule(TIMESTAMP_LTZ, {TIMESTAMP_LTZ, TIMESTAMP}, DATETIME | CHARACTER_STRING | NUMERIC)
    return implicit, explicit


_IMPLICIT_RULES, _EXPLICIT_RULES = _build_rules()


def supports_cast(source_type, target_type, allow_explicit: bool = True) -> bool:
    """Whether ``source_type`` can be cast to ``target_type`` for a column type
    change. ``allow_explicit`` permits the broader (possibly lossy) conversions
    in addition to the safe widening ones."""
    source_root = _root(source_type)
    target_root = _root(target_type)
    if source_root is None or target_root is None:
        return False
    # A NOT NULL target cannot accept a nullable source unless explicitly allowed.
    if source_type.nullable and not target_type.nullable and not allow_explicit:
        return False
    if source_root == target_root:
        return True
    if source_root in _IMPLICIT_RULES.get(target_root, set()):
        return True
    if allow_explicit and source_root in _EXPLICIT_RULES.get(target_root, set()):
        return True
    return False
