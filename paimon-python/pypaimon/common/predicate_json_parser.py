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


def _wrap_int32(value: int) -> int:
    """Wrap ``value`` into signed 32-bit, mirroring Java ``int`` overflow.

    OVERLAY's position arithmetic (``pos - 1`` and ``pos + replaced``) is
    evaluated by ``OverlayTransform.transform`` as 32-bit ``int``, so a large
    position wraps rather than growing unboundedly. Query-auth rules must admit
    exactly the rows Java admits, so compute these the same way.
    """
    return ((value + 2 ** 31) % 2 ** 32) - 2 ** 31

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

# Java DateExtractTransform names. LTZ is excluded because a calendar field
# would depend on a session time zone, which authorization rules do not carry.
_DATE_EXTRACT = {
    "YEAR": pc.year,
    "MONTH": pc.month,
    "DAY": pc.day,
    "HOUR": pc.hour,
    "MINUTE": pc.minute,
    "SECOND": pc.second,
    "QUARTER": pc.quarter,
    "DAY_OF_YEAR": pc.day_of_year,
    "WEEKDAY": lambda c: pc.day_of_week(c, count_from_zero=True, week_start=1),
    "ISO_DAY_OF_WEEK": lambda c: pc.day_of_week(c, count_from_zero=False, week_start=1),
    "DAY_OF_WEEK": lambda c: pc.day_of_week(c, count_from_zero=False, week_start=7),
    "WEEK": pc.iso_week,
    "YEAR_OF_WEEK": pc.iso_year,
}


def parse_predicate_to_batch_filter(json_str: str) -> Callable[[pa.RecordBatch], pa.Array]:
    data = json.loads(json_str)
    execute = _build_filter(data)
    validate = _build_filter(data, short_circuit=False)
    validated_schema = None

    def filter_fn(batch):
        nonlocal validated_schema
        # Java deserializes every child before row evaluation. Validate all
        # definitions on typed empty arrays even if short-circuiting skips them.
        if validated_schema != batch.schema:
            validate(batch.slice(0, 0))
            validated_schema = batch.schema
        return execute(batch)

    return filter_fn


def _build_filter(data: dict, short_circuit=True) -> Callable[[pa.RecordBatch], pa.Array]:
    kind = data["kind"]
    if kind == "LEAF":
        return _build_leaf_filter(data)
    elif kind == "COMPOUND":
        return _build_compound_filter(data, short_circuit)
    raise ValueError(f"Unknown predicate kind: {kind}")


def _build_leaf_filter(data: dict) -> Callable:
    transform = data["transform"]
    function = data["function"]
    literals = data.get("literals")
    if literals is None and function in ('TRUE', 'FALSE', 'IS_NULL', 'IS_NOT_NULL', 'IS_NAN'):
        literals = []
    if not isinstance(literals, list):
        raise ValueError(f"{function} needs a literal list")

    def filter_fn(batch: pa.RecordBatch) -> pa.Array:
        value_array = _apply_predicate_transform(transform, batch)
        return _apply_leaf_function(function, value_array, literals, len(batch))

    return filter_fn


def _build_compound_filter(data: dict, short_circuit=True) -> Callable:
    function = data["function"]
    if function not in ('AND', 'OR'):
        raise ValueError(f"Unknown compound function: {function}")
    children = data['children']
    if not children:
        raise ValueError('Compound predicate needs children')
    child_filters = [_build_filter(child, short_circuit) for child in children]

    def filter_fn(batch: pa.RecordBatch) -> pa.Array:
        result = child_filters[0](batch)
        for cf in child_filters[1:]:
            if not short_circuit:
                result = pc.and_(result, cf(batch)) if function == 'AND' else pc.or_(result, cf(batch))
                continue
            # Java evaluates each later child only for undecided rows. Retain
            # their original positions so nested compounds keep row order.
            pending = result if function == 'AND' else pc.invert(result)
            if not pc.any(pending).as_py():
                break
            if pc.all(pending).as_py():
                result = cf(batch)
                continue
            result = pc.replace_with_mask(result, pending, cf(batch.filter(pending)))
        return result

    return filter_fn


def _apply_predicate_transform(transform: dict, batch: pa.RecordBatch,
                               null_type: pa.DataType = pa.bool_()) -> pa.Array:
    name = transform["name"]

    if name == "FIELD_REF":
        return batch.column(transform["fieldRef"]["name"])

    elif name == "CAST":
        col = batch.column(transform["fieldRef"]["name"])
        target = transform['type']
        width = re.fullmatch(r"\s*(CHAR|VARCHAR|BINARY|VARBINARY)(?=\s|\(|\Z)"
                             r"(?:\s*\(\s*([0-9]+)\s*\))?"
                             r"(?:\s*(?:NOT\s+)?NULL)?\s*", target, flags=re.IGNORECASE)
        if width is not None:
            kind, length = width.group(1).upper(), int(width.group(2) or 1)
            if length < 1 or length > _INT_MAX:
                raise ValueError('Cast width must be between 1 and 2147483647')
            target_type = pa.string() if kind in ('CHAR', 'VARCHAR') else pa.binary()
        else:
            target_type = _paimon_type_to_arrow(target)
        stored = _paimon_type_to_arrow(transform['fieldRef']['type'])
        col = _cast_input_for_java_executor(col, stored, target_type)
        result = pc.cast(col, target_type, safe=False)
        # Java CastTransform enforces widths; Arrow uses unbounded arrays.
        if width is not None:
            pad = kind in ('CHAR', 'BINARY')
            padding = ' ' if kind == 'CHAR' else b'\x00'
            result = pa.array([
                None if value is None else
                (value[:length] + padding * max(0, length - len(value)) if pad else value[:length])
                for value in result.to_pylist()
            ], type=target_type)
        return result

    elif name == "UPPER":
        input_col = _resolve_transform_input(transform["inputs"][0], batch)
        return pc.utf8_upper(input_col)

    elif name == "LOWER":
        input_col = _resolve_transform_input(transform["inputs"][0], batch)
        return pc.utf8_lower(input_col)

    elif name == "CONCAT":
        resolved = [_resolve_transform_input(inp, batch) for inp in transform["inputs"]]
        if not resolved:
            return pa.array([''] * len(batch), type=pa.string())
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

    elif name in _DATE_EXTRACT:
        return _date_extract(name, transform["fieldRef"], batch)
    elif name == "LENGTH":
        _check_string_input("LENGTH input", transform["inputs"][0], batch, stored=True)
        source = _resolve_transform_input(transform["inputs"][0], batch)
        return pa.array(
            [None if v is None else len(v) for v in source.to_pylist()],
            type=pa.int32())

    elif name == "BIT_LENGTH":
        _check_string_input("BIT_LENGTH input", transform["inputs"][0], batch, stored=True)
        source = _resolve_transform_input(transform["inputs"][0], batch)
        return pa.array(
            [None if v is None else len(v.encode("utf-8")) * 8
             for v in source.to_pylist()],
            type=pa.int32())

    elif name == "TRANSLATE":
        return _translate(transform["inputs"], batch)

    elif name == "OVERLAY":
        return _overlay(transform["inputs"], batch)

    elif name == "PAD":
        return _pad(transform["inputs"], transform.get("direction"), batch)

    raise ValueError(f"Unknown transform type: {name}")


def _cast_input_for_java_executor(col, stored, target):
    # copyWithNewInputs retains Java's original executor. Character, Boolean
    # and byte executors require their original runtime value class.
    needs_class = (((pa.types.is_string(stored) or pa.types.is_large_string(stored)) and
                    not (pa.types.is_string(col.type) or pa.types.is_large_string(col.type))) or
                   (pa.types.is_boolean(stored) and not pa.types.is_boolean(col.type)) or
                   ((pa.types.is_binary(stored) or pa.types.is_large_binary(stored)) and
                    not (pa.types.is_binary(col.type) or pa.types.is_large_binary(col.type))))
    if needs_class:
        if col.null_count != len(col):
            raise ValueError('CAST source no longer has the runtime type its Java executor expects')
        return pa.nulls(len(col), type=target)

    numeric = pa.types.is_integer(stored) or pa.types.is_floating(stored) or pa.types.is_decimal(stored)
    if numeric and pa.types.is_string(target) and col.type != stored:
        # NumericToString accepts Object. DATE and TIME getters return Integer,
        # so this executor prints epoch days / milliseconds, not calendar text.
        if pa.types.is_date(col.type):
            return col.cast(pa.date32(), safe=False).cast(pa.int32())
        if pa.types.is_time(col.type):
            return col.cast(pa.time32('ms'), safe=False).cast(pa.int32())
        if pa.types.is_decimal(col.type):
            # Paimon Decimal.toString uses BigDecimal.toPlainString. Arrow may
            # emit an exponent for a small scaled decimal, including zero.
            return pa.array([None if value is None else format(value, 'f') for value in col.to_pylist()],
                            type=pa.string())
        if not (pa.types.is_integer(col.type) or pa.types.is_decimal(col.type) or
                pa.types.is_boolean(col.type) or pa.types.is_string(col.type) or pa.types.is_large_string(col.type)):
            # Timestamp.toString uses LocalDateTime text, and other objects
            # have their own formatting. An Arrow cast would authorize other
            # rows; refuse the unsupported rebound instead.
            if col.null_count != len(col):
                raise ValueError('CAST rebound source requires its original Java executor formatting')
            return pa.nulls(len(col), type=target)
    return col


def _date_extract(name: str, field_ref: dict, batch: pa.RecordBatch) -> pa.Array:
    """Java calendar extraction on DATE days or Timestamp milliseconds.

    Timestamp.toLocalDateTime narrows the epoch day to a signed int. Work on
    that representation directly, including DATE32 values which cannot be
    cast to an Arrow microsecond timestamp or a Python datetime.
    """
    from pypaimon.casting.temporal_utils import _signed
    stored = field_ref.get('type')
    if not isinstance(stored, str):
        raise ValueError("Calendar transform field needs a DATE or TIMESTAMP type")
    # Validate the complete stored SQL spelling before schema-name rebinding.
    match = re.fullmatch(r"\s*(DATE(?=\s|\Z)|TIMESTAMP(?=\s|\(|\Z)(?:\s*\(\s*([0-9]+)\s*\))?"
                         r"(?:\s*WITHOUT\s+TIME\s+ZONE(?=\s|\Z))?)"
                         r"(?:\s*(?:NOT\s+)?NULL)?\s*", stored, flags=re.IGNORECASE)
    if match is None or (match.group(2) is not None and int(match.group(2)) > 9):
        raise ValueError("Calendar transforms require DATE or TIMESTAMP without time zone")
    column = _field_column(field_ref, batch)
    date = pa.types.is_date32(column.type)
    timestamp = pa.types.is_timestamp(column.type) and column.type.tz is None
    if not date and not timestamp:
        raise ValueError("Calendar transforms require DATE or TIMESTAMP without time zone")
    raw = column.cast(pa.int32() if date else pa.int64())
    unit = column.type.unit if timestamp else None
    # Arrow calendar kernels agree with Java within years 1..9999. Keep their
    # vectorized execution for ordinary batches; outside this range use Java's
    # epoch-day arithmetic, avoiding Arrow's bounded calendar representation.
    bounds = pc.min_max(raw).as_py()
    ticks_per_day = 1 if date else 86400 * {'s': 1, 'ms': 1000, 'us': 1000000, 'ns': 1000000000}[unit]
    if (bounds['min'] is None or
            (-719162 * ticks_per_day <= bounds['min'] and
             bounds['max'] < 2932897 * ticks_per_day)):
        source = column.cast(pa.timestamp('us')) if date else column
        return _DATE_EXTRACT[name](source).cast(pa.int32())
    values = raw.to_pylist()
    results = []
    for value in values:
        if value is None:
            results.append(None)
            continue
        if date:
            day, millis = value, 0
        else:
            if unit == 's':
                millis = _signed(value * 1000, 64)
            elif unit == 'ms':
                millis = value
            else:
                millis = value // (1000 if unit == 'us' else 1000000)
            day, millis = _signed(millis // 86400000, 32), millis % 86400000
        results.append(_calendar_field(name, day, millis))
    return pa.array(results, type=pa.int32())


def _calendar_field(name, day, millis):
    from pypaimon.casting.temporal_utils import _epoch_day_to_date
    year, month, date = _epoch_day_to_date(day)
    weekday = (day + 3) % 7
    if name == 'YEAR':
        return year
    if name == 'MONTH':
        return month
    if name == 'DAY':
        return date
    if name == 'HOUR':
        return millis // 3600000
    if name == 'MINUTE':
        return millis // 60000 % 60
    if name == 'SECOND':
        return millis // 1000 % 60
    if name == 'QUARTER':
        return (month - 1) // 3 + 1
    if name == 'DAY_OF_YEAR':
        return day - _january_first(year) + 1
    if name == 'WEEKDAY':
        return weekday
    if name == 'ISO_DAY_OF_WEEK':
        return weekday + 1
    if name == 'DAY_OF_WEEK':
        return (weekday + 1) % 7 + 1
    thursday = day + 3 - weekday
    week_year = _epoch_day_to_date(thursday)[0]
    return week_year if name == 'YEAR_OF_WEEK' else (thursday - _january_first(week_year)) // 7 + 1


def _january_first(year):
    era, yoe = divmod(year - 1, 400)
    return era * 146097 + yoe * 365 + yoe // 4 - yoe // 100 + 306 - 719468


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


_INTEGER_TYPE = re.compile(
    r"\s*(?:TINYINT|SMALLINT|INT|INTEGER|BIGINT)(?=\s|\Z)"
    r"(?:\s*(?:NOT\s+)?NULL)?\s*\Z", re.IGNORECASE)


def _check_integer_input(slot, value, batch):
    if value is None or (isinstance(value, int) and not isinstance(value, bool)):
        return
    if not isinstance(value, dict):
        raise ValueError(f"{slot} must be an integer or a field: {value!r}")
    stored = value.get("type")
    if not isinstance(stored, str) or _INTEGER_TYPE.match(stored) is None:
        raise ValueError(f"{slot} field must be an integer: {stored!r}")
    column = _field_column(value, batch)
    if not pa.types.is_integer(column.type):
        raise ValueError(f"{slot} field must be an integer: {column.type}")


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


def _pad(inputs, direction, batch: pa.RecordBatch) -> pa.Array:
    if not isinstance(inputs, list) or len(inputs) != 3:
        raise ValueError(f"PAD takes 3 inputs, got {inputs!r}")
    if direction not in ("LEFT", "RIGHT"):
        raise ValueError(f"PAD direction must be LEFT or RIGHT: {direction!r}")
    _check_string_input("PAD input", inputs[0], batch, stored=True)
    _check_string_input("PAD string", inputs[2], batch, stored=True)
    _check_integer_input("PAD length", inputs[1], batch)
    source = _resolve_transform_input(inputs[0], batch)
    pad = _resolve_transform_input(inputs[2], batch)
    lengths = _Positions(inputs[1], batch)
    src_list = source.to_pylist()
    pad_list = pad.to_pylist()
    result = []
    for i in range(len(source)):
        value = src_list[i]
        pad_str = pad_list[i]
        raw_len = lengths.value(i)
        length = _int_position(raw_len) if raw_len is not None else None
        if value is None or length is None or pad_str is None:
            result.append(None)
            continue
        result.append(_pad_one(value, length, pad_str, direction))
    return pa.array(result, type=pa.string())


def _pad_one(source, length, pad, direction):
    needed = length - len(source)
    if needed <= 0 or pad == "":
        # Truncate to the first `length` characters (empty for a non-positive length).
        return source[:length] if length > 0 else ""
    pad_chars = len(pad)
    padding = pad * (needed // pad_chars) + pad[:needed % pad_chars]
    return padding + source if direction == "LEFT" else source + padding


def _translate(inputs, batch: pa.RecordBatch) -> pa.Array:
    if not isinstance(inputs, list) or len(inputs) != 3:
        raise ValueError(f"TRANSLATE takes 3 inputs, got {inputs!r}")
    _check_string_input("TRANSLATE source", inputs[0], batch, stored=True)
    _check_string_input("TRANSLATE matching", inputs[1], batch, stored=True)
    _check_string_input("TRANSLATE replacement", inputs[2], batch, stored=True)
    source = _resolve_transform_input(inputs[0], batch)
    matching = _resolve_transform_input(inputs[1], batch)
    replacement = _resolve_transform_input(inputs[2], batch)
    result = []
    for src, mat, rep in zip(source.to_pylist(), matching.to_pylist(),
                             replacement.to_pylist()):
        result.append(_translate_one(src, mat, rep))
    return pa.array(result, type=pa.string())


def _translate_one(source, matching, replacement):
    if source is None or matching is None or replacement is None:
        return None
    # First mapping wins per source character; a replacement code point of 0
    # (or a missing one) deletes the character, mirroring Java's TranslateTransform.
    dictionary = {}
    for i, ch in enumerate(matching):
        if ch not in dictionary:
            mapped = replacement[i] if i < len(replacement) else None
            dictionary[ch] = mapped if (mapped is not None and ord(mapped) != 0) else None
    out = []
    for ch in source:
        if ch not in dictionary:
            out.append(ch)
        elif dictionary[ch] is not None:
            out.append(dictionary[ch])
    return "".join(out)


def _overlay(inputs, batch: pa.RecordBatch) -> pa.Array:
    if not isinstance(inputs, list) or len(inputs) not in (3, 4):
        raise ValueError(f"OVERLAY takes 3 or 4 inputs, got {inputs!r}")
    _check_string_input("OVERLAY input", inputs[0], batch, stored=True)
    _check_string_input("OVERLAY replacement", inputs[1], batch, stored=True)
    _check_integer_input("OVERLAY position", inputs[2], batch)
    if len(inputs) == 4:
        _check_integer_input("OVERLAY length", inputs[3], batch)
    source = _resolve_transform_input(inputs[0], batch)
    replacement = _resolve_transform_input(inputs[1], batch)
    positions = _Positions(inputs[2], batch)
    has_length = len(inputs) == 4
    lengths = _Positions(inputs[3], batch) if has_length else None
    src_list = source.to_pylist()
    repl_list = replacement.to_pylist()
    result = []
    for i in range(len(source)):
        value = src_list[i]
        repl = repl_list[i]
        raw_pos = positions.value(i)
        # Java evaluates both integer arguments before propagating string NULLs.
        pos = _int_position(raw_pos) if raw_pos is not None else None
        raw_length = lengths.value(i) if has_length else None
        length = _int_position(raw_length) if raw_length is not None else None
        if value is None or repl is None or pos is None or (has_length and length is None):
            result.append(None)
            continue
        replaced = len(repl) if (length is None or length < 0) else length
        # pos - 1 and pos + replaced are 32-bit int expressions in Java's
        # OverlayTransform; wrap them so a boundary position overflows exactly
        # as Java's does (otherwise an auth rule admits different rows here).
        head = _substring_sql(value, 1, _wrap_int32(pos - 1))
        tail = _substring_sql(value, _wrap_int32(pos + replaced), _INT_MAX)
        result.append(head + repl + tail)
    return pa.array(result, type=pa.string())


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
    if (pa.types.is_binary(value_array.type) or pa.types.is_large_binary(value_array.type)) and any(
            literal is not None for literal in literals):
        raise ValueError('Java predicates do not support non-null binary literals')
    converted = []
    for literal in literals:
        value = _convert_literal(literal, value_array.type)
        if value is not None:
            # Java deserializes every literal using the expression's output
            # type before evaluation, including unused literals and NULL tests.
            checked = pa.scalar(value, type=value_array.type)
            value = checked if isinstance(value, pa.Scalar) else checked.as_py()
        converted.append(value)

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
    if (name in ("FIELD_REF", "CAST") or name in _DATE_EXTRACT) and "fieldRef" in transform:
        fields.add(transform["fieldRef"]["name"])
    else:
        for inp in transform.get("inputs", []):
            if isinstance(inp, dict) and "name" in inp:
                # _resolve_transform_input reads every dict input as a column by name
                fields.add(inp["name"])
                _collect_all_field_refs_from_transform(inp, fields)
    return fields
