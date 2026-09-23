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

import pyarrow as pa

from pypaimon.schema.data_types import (ArrayType, AtomicType, DataTypeParser,
                                        MapType, MultisetType,
                                        PyarrowFieldParser, RowType,
                                        VectorType)

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
        if source_root in CONSTRUCTED:
            # A constructed type is only castable to an (ignoring outer
            # nullability) identical constructed type. Reshaping is done
            # through sub-field / 'element' / 'value' paths instead: a whole
            # ROW replacement would carry caller-supplied nested field ids
            # that corrupt the id model, and there is no runtime conversion
            # between differently-shaped constructed values.
            return _equals_ignore_nullable(source_type, target_type)
        return True
    if source_root in _IMPLICIT_RULES.get(target_root, set()):
        return True
    if allow_explicit and source_root in _EXPLICIT_RULES.get(target_root, set()):
        return True
    return False


def _equals_ignore_nullable(source_type, target_type) -> bool:
    source_copy = DataTypeParser.parse_data_type(source_type.to_dict())
    target_copy = DataTypeParser.parse_data_type(target_type.to_dict())
    source_copy.nullable = True
    target_copy.nullable = True
    return source_copy == target_copy


# Caches the PyArrow cast-kernel probe per (source, target) pyarrow type so the
# alter-time check stays cheap. Keyed by the pyarrow type strings.
_EXECUTABLE_CAST_CACHE = {}


def can_execute_cast(source_type, target_type) -> bool:
    """Whether the Python read path can actually *materialize* a stored
    ``source_type`` value as ``target_type`` when reading a file written before
    the column type change.

    ``supports_cast`` only encodes the *logical* cast specification (mirroring
    Java ``DataTypeCasts``). This is the executable-cast counterpart of Java's
    ``CastExecutors.resolve(...) != null`` guard: some logically-valid casts
    (e.g. ``TIMESTAMP -> DECIMAL``, ``BOOLEAN -> DECIMAL``, ``TIME ->
    TIMESTAMP``) have no PyArrow cast kernel, so without this check the alter
    succeeds and the read later fails with ``ArrowNotImplementedError``.
    """
    source_root = _root(source_type)
    target_root = _root(target_type)
    if source_root is None or target_root is None:
        return False
    # Same root: identity, or a same-shape constructed type whose value is
    # rebuilt by the read path's field-id alignment rather than a value cast.
    if source_root == target_root:
        return True
    # Constructed -> character string is rendered by the read path's custom
    # ``_constructed_to_string_array`` (see DataFileBatchReader), not a cast.
    if (source_root in STRING_RENDERABLE_CONSTRUCTED
            and target_root in CHARACTER_STRING):
        return True
    # Any other conversion touching a constructed type has no runtime cast.
    if source_root in CONSTRUCTED or target_root in CONSTRUCTED:
        return False
    # Leaf-to-leaf: defer to PyArrow's cast-kernel availability, which is the
    # read path's actual cast executor (``array.cast(target, safe=False)``).
    return _pyarrow_cast_supported(source_type, target_type)


def _pyarrow_cast_supported(source_type, target_type) -> bool:
    source_pa = PyarrowFieldParser.from_paimon_type(source_type)
    target_pa = PyarrowFieldParser.from_paimon_type(target_type)
    if source_pa == target_pa:
        return True
    cache_key = (str(source_pa), str(target_pa))
    cached = _EXECUTABLE_CAST_CACHE.get(cache_key)
    if cached is not None:
        return cached
    # Probe a one-row (null-valued) array rather than an empty one. An empty
    # array only resolves the cast kernel; some kernels additionally reject the
    # target type parameters on any non-empty input -- e.g. INT -> DECIMAL(10,2)
    # has a kernel but needs precision >= 12 to hold an int's range at scale 2,
    # so an empty probe passes yet the read later fails with ArrowInvalid. A
    # single null row triggers that static type-parameter validation while
    # avoiding per-value parse/overflow errors (which ``safe=False`` -- matching
    # the read path -- tolerates anyway).
    try:
        pa.nulls(1, type=source_pa).cast(target_pa, safe=False)
        ok = True
    except (pa.lib.ArrowNotImplementedError, pa.lib.ArrowInvalid,
            pa.lib.ArrowTypeError):
        ok = False
    _EXECUTABLE_CAST_CACHE[cache_key] = ok
    return ok
