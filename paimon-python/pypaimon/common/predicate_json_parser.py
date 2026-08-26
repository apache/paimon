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

import json
import re
from typing import Callable

import pyarrow as pa
import pyarrow.compute as pc

# utf8_slice_codeunits needs an explicit integer stop on pyarrow 6
_MAX_STOP = 2 ** 31 - 1

_INT_MIN, _INT_MAX = -2 ** 31, 2 ** 31 - 1

# Integer.parseInt syntax: an optional sign and Unicode decimal digits, which
# Character.digit accepts, but no whitespace or underscore, which int() would.
# Java reads UTF-16 chars, so a supplementary-plane digit fails there.
_JAVA_INT = re.compile(r"[+-]?\d+\Z")

# an omitted third input, as opposed to one that is explicitly null
_ABSENT = object()

# per trimFlag: the Arrow kernel, and the str method for the per-row form
_TRIM_OPS = {
    "BOTH": (pc.utf8_trim, str.strip),
    "LEADING": (pc.utf8_ltrim, str.lstrip),
    "TRAILING": (pc.utf8_rtrim, str.rstrip),
}


def parse_predicate_to_batch_filter(json_str: str) -> Callable[[pa.RecordBatch], pa.Array]:
    data = json.loads(json_str)
    return _build_filter(data)


def _build_filter(data: dict) -> Callable[[pa.RecordBatch], pa.Array]:
    kind = data["kind"]
    if kind == "LEAF":
        return _build_leaf_filter(data)
    elif kind == "COMPOUND":
        return _build_compound_filter(data)
    raise ValueError(f"Unknown predicate kind: {kind}")


def _build_leaf_filter(data: dict) -> Callable:
    transform = data["transform"]
    function = data["function"]
    literals = data.get("literals", [])

    def filter_fn(batch: pa.RecordBatch) -> pa.Array:
        value_array = _apply_predicate_transform(transform, batch)
        return _apply_leaf_function(function, value_array, literals, len(batch))

    return filter_fn


def _build_compound_filter(data: dict) -> Callable:
    function = data["function"]
    child_filters = [_build_filter(child) for child in data["children"]]

    def filter_fn(batch: pa.RecordBatch) -> pa.Array:
        if function == "AND":
            result = child_filters[0](batch)
            for cf in child_filters[1:]:
                result = pc.and_(result, cf(batch))
            return result
        elif function == "OR":
            result = child_filters[0](batch)
            for cf in child_filters[1:]:
                result = pc.or_(result, cf(batch))
            return result
        raise ValueError(f"Unknown compound function: {function}")

    return filter_fn


def _apply_predicate_transform(transform: dict, batch: pa.RecordBatch,
                               null_type: pa.DataType = pa.bool_()) -> pa.Array:
    name = transform["name"]

    if name == "FIELD_REF":
        return batch.column(transform["fieldRef"]["name"])

    elif name == "CAST":
        col = batch.column(transform["fieldRef"]["name"])
        target_type = _paimon_type_to_arrow(transform["type"])
        return pc.cast(col, target_type, safe=False)

    elif name == "UPPER":
        input_col = _resolve_transform_input(transform["inputs"][0], batch)
        return pc.utf8_upper(input_col)

    elif name == "LOWER":
        input_col = _resolve_transform_input(transform["inputs"][0], batch)
        return pc.utf8_lower(input_col)

    elif name == "CONCAT":
        resolved = [_resolve_transform_input(inp, batch) for inp in transform["inputs"]]
        if not resolved:
            return pa.nulls(len(batch), type=pa.string())
        return pc.binary_join_element_wise(*resolved, "")

    elif name == "CONCAT_WS":
        sep = _resolve_transform_input(transform["inputs"][0], batch)
        values = [_resolve_transform_input(inp, batch) for inp in transform["inputs"][1:]]
        if not values:
            return pa.nulls(len(batch), type=pa.string())
        return _concat_ws(sep, values)

    elif name == "SUBSTRING":
        return _substring(transform["inputs"], batch)

    elif name == "TRIM":
        flag = transform.get("trimFlag")
        if flag is None:
            raise ValueError("TRIM rule is missing trimFlag")
        return _trim(transform["inputs"], flag, batch)

    elif name == "NULL":
        return pa.nulls(len(batch), type=null_type)

    raise ValueError(f"Unknown transform type: {name}")


def _substring(inputs, batch: pa.RecordBatch) -> pa.Array:
    if not isinstance(inputs, list):
        raise ValueError(f"SUBSTRING inputs must be a list, got {inputs!r}")
    if len(inputs) not in (2, 3):
        raise ValueError(f"SUBSTRING takes 2 or 3 inputs, got {len(inputs)}")
    source = _resolve_transform_input(inputs[0], batch)
    begin = inputs[1]
    length = inputs[2] if len(inputs) == 3 else _ABSENT

    _check_string_input("SUBSTRING source", inputs[0], batch)

    # Jackson refuses a non-integral number, a boolean or an array when the rule is
    # read; a null is not malformed and propagates to a null result, as in SQL
    for position in (begin,) + ((length,) if length is not _ABSENT else ()):
        if position is None or isinstance(position, dict):
            continue
        if isinstance(position, bool) or not isinstance(position, (int, str)):
            raise ValueError(f"SUBSTRING position must be an integer: {position!r}")

    # a malformed literal is left to the per-row path, which raises where Java does
    begin_literal = _literal_position(begin)
    length_literal = _literal_position(length) if length is not _ABSENT else None

    # the kernel only matches the SQL semantics for a positive begin and length
    if begin_literal is not None and begin_literal >= 1:
        if length is _ABSENT:
            return pc.utf8_slice_codeunits(source, start=begin_literal - 1, stop=_MAX_STOP)
        if (
            length_literal is not None
            and length_literal > 0
            and begin_literal + length_literal - 1 <= _INT_MAX
        ):
            start = begin_literal - 1
            return pc.utf8_slice_codeunits(source, start=start, stop=start + length_literal)

    return _substring_per_row(source, begin, length, batch)


def _int_position(value):
    """A SUBSTRING begin/length, with Java's tolerance and no more: Integer.parseInt
    takes "+2" and "007" but not "1_0" or " 2 ", which int() accepts."""
    if isinstance(value, bool) or isinstance(value, float):
        raise ValueError(f"SUBSTRING position must be an integer: {value!r}")
    if isinstance(value, str):
        if not _JAVA_INT.match(value) or any(ord(c) > 0xFFFF for c in value):
            raise ValueError(f"SUBSTRING position must be an integer: {value!r}")
        position = int(value)
    elif isinstance(value, int):
        position = value
    else:
        raise ValueError(f"SUBSTRING position must be an integer: {value!r}")
    if not _INT_MIN <= position <= _INT_MAX:
        raise ValueError(f"SUBSTRING position is out of the integer range: {value!r}")
    return position


def _literal_position(value):
    """The value of a literal position, or None when it is a field or unusable here."""
    if value is None or isinstance(value, dict):
        return None
    try:
        return _int_position(value)
    except ValueError:
        return None


def _field_column(inp, batch: pa.RecordBatch):
    """The column a field reference names. Java rebuilds the reference from the schema
    before a rule runs, so the index it carries and, for SUBSTRING, the type it carries
    never decide anything; only the name is used to find the column."""
    name = inp.get("name")
    if not isinstance(name, str) or name not in batch.schema.names:
        raise ValueError(f"Column masking refers to a field that is not present: {inp!r}")
    return batch.column(name)


# StringTransform validates a stored FieldRef type when the rule is read, before
# TableQueryAuthResult replaces it from the schema. The whole spelling has to match:
# Java parses "STRING ARRAY" as an array type, accepts an explicit NULL as well as
# NOT NULL, needs no space after a ")" but does after a bare keyword, and rejects a
# length outside CharType and VarCharType's [1, MAX_VALUE].
_CHARACTER_TYPE = re.compile(
    r"\s*(?:STRING(?=\s|\Z)|(?:CHAR|VARCHAR)\s*(?:\(\s*(\d+)\s*\)|(?=\s|\Z)))"
    r"(?:\s*(?:NOT\s+)?NULL)?\s*\Z",
    re.IGNORECASE,
)


def _check_stored_character_type(slot: str, inp) -> None:
    stored = inp.get("type")
    matched = _CHARACTER_TYPE.match(stored) if isinstance(stored, str) else None
    length = matched.group(1) if matched else None
    if matched is None or (length is not None and not 1 <= int(length) <= _INT_MAX):
        raise ValueError(f"{slot} field must be a string: {stored!r}")


def _check_string_input(slot: str, value, batch: pa.RecordBatch, stored: bool = False) -> None:
    """A string input is a literal or a field reference to a string column."""
    if value is None or isinstance(value, str):
        return
    if not isinstance(value, dict):
        raise ValueError(f"{slot} must be a string or a field: {value!r}")
    if stored:
        _check_stored_character_type(slot, value)
    column = _field_column(value, batch)
    if not pa.types.is_string(column.type) and not pa.types.is_large_string(column.type):
        raise ValueError(f"{slot} field must be a string: {column.type}")


class _Positions:
    """A position slot resolved lazily, as Java reads one only when a row reaches it."""

    def __init__(self, inp, batch: pa.RecordBatch):
        self._inp = inp
        self._batch = batch
        self._values = None

    def value(self, index: int):
        if self._values is None:
            self._values = self._resolve()
        return self._values[index]

    def _resolve(self) -> list:
        inp = self._inp
        if not isinstance(inp, dict):
            return [inp] * len(self._batch)
        column = _field_column(inp, self._batch)
        if not pa.types.is_integer(column.type):
            raise ValueError(
                f"SUBSTRING position field must be an integer type: {column.type}")
        return column.to_pylist()


def _substring_sql(value: str, pos: int, length: int) -> str:
    """BinaryString.substringSQL: one-based, zero means one, negative counts from the end."""
    chars = len(value)
    start = pos - 1 if pos > 0 else (chars + pos if pos < 0 else 0)
    end = start + length
    # Java computes the end in long arithmetic and saturates it into an int
    end = min(max(end, _INT_MIN), _INT_MAX)
    if end <= start or start >= chars:
        return ""
    return value[max(start, 0):min(max(end, 0), chars)]


def _substring_per_row(source: pa.Array, begin, length, batch: pa.RecordBatch) -> pa.Array:
    # mirrors SubstringTransform.transform, including the order of its checks
    begins = _Positions(begin, batch)
    has_length = length is not _ABSENT
    lengths = _Positions(length, batch) if has_length else None
    result = []
    for i, value in enumerate(source.to_pylist()):
        if value is None:
            result.append(None)
            continue
        raw_begin = begins.value(i)
        # SQL null propagation: every position is checked before any is parsed
        if raw_begin is None or (has_length and lengths.value(i) is None):
            result.append(None)
            continue
        pos = _int_position(raw_begin)
        length_value = _int_position(lengths.value(i)) if has_length else _INT_MAX
        result.append(_substring_sql(value, pos, length_value))
    return pa.array(result, type=source.type)


def _trim(inputs, flag: str, batch: pa.RecordBatch) -> pa.Array:
    if not isinstance(inputs, list):
        raise ValueError(f"TRIM inputs must be a list, got {inputs!r}")
    if len(inputs) not in (1, 2):
        raise ValueError(f"TRIM takes 1 or 2 inputs, got {len(inputs)}")
    _check_string_input("TRIM source", inputs[0], batch, stored=True)
    if len(inputs) == 2:
        _check_string_input("TRIM characters", inputs[1], batch, stored=True)
    source = _resolve_transform_input(inputs[0], batch)
    # Java's one-input TRIM trims spaces only, not every whitespace character.
    chars = " " if len(inputs) == 1 else inputs[1]

    # validated first: Jackson rejects an unknown flag when the rule is read
    kernel = _trim_ops(flag)[0]

    if isinstance(chars, dict):
        return _trim_per_row(source, flag, batch.column(chars["name"]).to_pylist())

    if chars is None:
        # Java masks the whole column to null for a null charsToTrim
        return pa.nulls(len(batch), type=source.type)

    return kernel(source, characters=chars)


def _trim_ops(flag: str):
    ops = _TRIM_OPS.get(flag)
    if ops is None:
        raise ValueError(f"Unknown trimFlag: {flag}")
    return ops


def _trim_per_row(source: pa.Array, flag: str, chars_per_row: list) -> pa.Array:
    strip = _trim_ops(flag)[1]
    result = []
    for value, chars in zip(source.to_pylist(), chars_per_row):
        result.append(None if value is None or chars is None else strip(value, chars))
    return pa.array(result, type=source.type)


def _resolve_transform_input(inp, batch: pa.RecordBatch) -> pa.Array:
    if isinstance(inp, dict):
        return batch.column(inp["name"])
    elif isinstance(inp, str):
        return pa.array([inp] * len(batch), type=pa.string())
    elif inp is None:
        return pa.nulls(len(batch), type=pa.string())
    return pa.array([str(inp)] * len(batch), type=pa.string())


def _concat_ws(sep: pa.Array, value_arrays: list) -> pa.Array:
    sep_list = sep.to_pylist()
    val_lists = [v.to_pylist() for v in value_arrays]
    results = []
    for i in range(len(sep)):
        s = sep_list[i]
        if s is None:
            results.append(None)
            continue
        parts = [vl[i] for vl in val_lists if vl[i] is not None]
        results.append(s.join(parts))
    return pa.array(results, type=pa.string())


def _null_as_false(arr: pa.Array) -> pa.Array:
    """Replace nulls with False to match Java two-valued predicate semantics."""
    if arr.null_count == 0:
        return arr
    return pc.if_else(pc.is_valid(arr), arr, False)


def _apply_leaf_function(function: str, value_array: pa.Array, literals: list, batch_len: int) -> pa.Array:
    """Null literal yields False to match Java LeafBinaryFunction/LeafTernaryFunction semantics.

    All comparison results are coerced from three-valued (PyArrow null) to
    two-valued logic (null → False) so that compound AND/OR behaves
    identically to Java.
    """
    converted = [_convert_literal(lit, value_array.type) for lit in literals]

    if function == "EQUAL":
        if converted[0] is None:
            return pa.array([False] * batch_len, type=pa.bool_())
        return _null_as_false(pc.equal(value_array, converted[0]))
    elif function == "NOT_EQUAL":
        if converted[0] is None:
            return pa.array([False] * batch_len, type=pa.bool_())
        return _null_as_false(pc.not_equal(value_array, converted[0]))
    elif function == "LESS_THAN":
        if converted[0] is None:
            return pa.array([False] * batch_len, type=pa.bool_())
        return _null_as_false(pc.less(value_array, converted[0]))
    elif function == "LESS_OR_EQUAL":
        if converted[0] is None:
            return pa.array([False] * batch_len, type=pa.bool_())
        return _null_as_false(pc.less_equal(value_array, converted[0]))
    elif function == "GREATER_THAN":
        if converted[0] is None:
            return pa.array([False] * batch_len, type=pa.bool_())
        return _null_as_false(pc.greater(value_array, converted[0]))
    elif function == "GREATER_OR_EQUAL":
        if converted[0] is None:
            return pa.array([False] * batch_len, type=pa.bool_())
        return _null_as_false(pc.greater_equal(value_array, converted[0]))
    elif function == "IS_NULL":
        return pc.is_null(value_array)
    elif function == "IS_NOT_NULL":
        return pc.is_valid(value_array)
    elif function == "IN":
        non_null = [v for v in converted if v is not None]
        if not non_null:
            return pa.array([False] * batch_len, type=pa.bool_())
        in_mask = pc.is_in(value_array, pa.array(non_null, type=value_array.type))
        return pc.if_else(pc.is_valid(value_array), in_mask, False)
    elif function == "NOT_IN":
        if any(lit is None for lit in literals):
            return pa.array([False] * batch_len, type=pa.bool_())
        not_in_mask = pc.invert(
            pc.is_in(value_array, pa.array(converted, type=value_array.type)))
        return pc.if_else(pc.is_valid(value_array), not_in_mask, False)
    elif function == "BETWEEN":
        if converted[0] is None or converted[1] is None:
            return pa.array([False] * batch_len, type=pa.bool_())
        return _null_as_false(pc.and_(
            pc.greater_equal(value_array, converted[0]),
            pc.less_equal(value_array, converted[1])))
    elif function == "NOT_BETWEEN":
        if converted[0] is None or converted[1] is None:
            return pa.array([False] * batch_len, type=pa.bool_())
        return _null_as_false(pc.or_(
            pc.less(value_array, converted[0]),
            pc.greater(value_array, converted[1])))
    elif function == "STARTS_WITH":
        if converted[0] is None:
            return pa.array([False] * batch_len, type=pa.bool_())
        return _null_as_false(pc.starts_with(value_array, converted[0]))
    elif function == "ENDS_WITH":
        if converted[0] is None:
            return pa.array([False] * batch_len, type=pa.bool_())
        return _null_as_false(pc.ends_with(value_array, converted[0]))
    elif function == "CONTAINS":
        if converted[0] is None:
            return pa.array([False] * batch_len, type=pa.bool_())
        return _null_as_false(pc.match_substring(value_array, converted[0]))
    elif function == "LIKE":
        raw = literals[0]
        if raw is None:
            return pa.array([False] * batch_len, type=pa.bool_())
        from pypaimon.common.predicate import Like
        pattern = Like._sql_like_to_regex(raw)
        return _null_as_false(
            pc.match_substring_regex(value_array, f"^{pattern}$"))
    elif function == "TRUE":
        return pa.array([True] * batch_len, type=pa.bool_())
    elif function == "FALSE":
        return pa.array([False] * batch_len, type=pa.bool_())
    elif function == "IS_NAN":
        return _null_as_false(pc.is_nan(value_array))
    raise ValueError(f"Unknown leaf function: {function}")


def _convert_literal(literal, target_type: pa.DataType):
    if literal is None:
        return None
    if pa.types.is_timestamp(target_type):
        import datetime
        if isinstance(literal, str):
            dt = datetime.datetime.fromisoformat(literal.replace("Z", "+00:00"))
            return pa.scalar(dt, type=target_type)
        elif isinstance(literal, list):
            dt = datetime.datetime(*literal[:6])
            if len(literal) > 6:
                dt = dt.replace(microsecond=literal[6] // 1000)
            return pa.scalar(dt, type=target_type)
        elif isinstance(literal, (int, float)):
            dt = datetime.datetime.fromtimestamp(literal, tz=datetime.timezone.utc)
            return pa.scalar(dt, type=target_type)
    elif pa.types.is_date(target_type):
        import datetime
        if isinstance(literal, str):
            return pa.scalar(datetime.date.fromisoformat(literal), type=target_type)
        elif isinstance(literal, list):
            return pa.scalar(datetime.date(*literal[:3]), type=target_type)
    elif pa.types.is_time(target_type):
        import datetime
        if isinstance(literal, str):
            t = datetime.time.fromisoformat(literal)
            return pa.scalar(t, type=target_type)
        elif isinstance(literal, list):
            t = datetime.time(*literal[:3])
            if len(literal) > 3:
                t = t.replace(microsecond=literal[3] // 1000)
            return pa.scalar(t, type=target_type)
    elif pa.types.is_decimal(target_type):
        import decimal
        return pa.scalar(decimal.Decimal(str(literal)), type=target_type)
    return literal


def _paimon_type_to_arrow(paimon_type: str) -> pa.DataType:
    type_str = paimon_type.strip().upper()

    ltz_match = re.match(
        r"^TIMESTAMP\s*\((\d+)\)\s+WITH\s+LOCAL\s+TIME\s+ZONE", type_str)
    if ltz_match:
        precision = int(ltz_match.group(1))
        return pa.timestamp(_timestamp_precision_to_unit(precision), tz="UTC")

    m = re.match(r"^([A-Z_ ]+?)(?:\((.+)\))?(?:\s+NOT\s+NULL)?$", type_str)
    if not m:
        raise ValueError(f"Cannot parse Paimon type: '{paimon_type}'")
    base_type = m.group(1).strip()
    params = m.group(2)

    simple_mapping = {
        "INT": pa.int32(),
        "BIGINT": pa.int64(),
        "SMALLINT": pa.int16(),
        "TINYINT": pa.int8(),
        "FLOAT": pa.float32(),
        "DOUBLE": pa.float64(),
        "STRING": pa.string(),
        "BOOLEAN": pa.bool_(),
        "BYTES": pa.binary(),
        "DATE": pa.date32(),
    }
    if base_type in simple_mapping:
        return simple_mapping[base_type]

    if base_type in ("VARCHAR", "CHAR"):
        return pa.string()

    if base_type in ("VARBINARY", "BINARY"):
        return pa.binary()

    if base_type == "TIMESTAMP":
        precision = int(params) if params else 6
        unit = _timestamp_precision_to_unit(precision)
        return pa.timestamp(unit)

    if base_type in ("TIMESTAMP WITH LOCAL TIME ZONE", "TIMESTAMP_WITH_LOCAL_TIME_ZONE", "TIMESTAMP_LTZ"):
        precision = int(params) if params else 6
        unit = _timestamp_precision_to_unit(precision)
        return pa.timestamp(unit, tz="UTC")

    if base_type == "TIME":
        precision = int(params) if params else 6
        unit = _timestamp_precision_to_unit(precision)
        return pa.time64(unit) if unit in ("us", "ns") else pa.time32(unit)

    if base_type == "DECIMAL":
        if params:
            parts = [x.strip() for x in params.split(",")]
            if len(parts) == 2:
                return pa.decimal128(int(parts[0]), int(parts[1]))
        raise ValueError(f"DECIMAL type requires (precision, scale): '{paimon_type}'")

    raise ValueError(
        f"Unsupported Paimon type for PyArrow conversion: '{paimon_type}'. "
        f"Supported: INT, BIGINT, SMALLINT, TINYINT, FLOAT, DOUBLE, STRING, VARCHAR, CHAR, "
        f"BOOLEAN, BYTES, VARBINARY, DATE, TIME(p), TIMESTAMP(p), "
        f"TIMESTAMP WITH LOCAL TIME ZONE(p), DECIMAL(p,s)."
    )


def _timestamp_precision_to_unit(precision: int) -> str:
    if precision == 0:
        return "s"
    elif precision <= 3:
        return "ms"
    elif precision <= 6:
        return "us"
    else:
        return "ns"


def extract_referenced_fields(json_str: str) -> set:
    data = json.loads(json_str)
    fields = set()
    _collect_fields(data, fields)
    return fields


def _collect_fields(data: dict, fields: set):
    kind = data.get("kind")
    if kind == "LEAF":
        _collect_all_field_refs_from_transform(data["transform"], fields)
    elif kind == "COMPOUND":
        for child in data["children"]:
            _collect_fields(child, fields)


def _collect_all_field_refs_from_transform(transform: dict, fields: set = None) -> set:
    if fields is None:
        fields = set()
    name = transform.get("name")
    if name == "FIELD_REF" and "fieldRef" in transform:
        fields.add(transform["fieldRef"]["name"])
    elif name == "CAST" and "fieldRef" in transform:
        fields.add(transform["fieldRef"]["name"])
    else:
        for inp in transform.get("inputs", []):
            if isinstance(inp, dict):
                if "name" in inp and "index" in inp:
                    fields.add(inp["name"])
                elif "name" in inp:
                    _collect_all_field_refs_from_transform(inp, fields)
    return fields
