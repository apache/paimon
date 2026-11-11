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

"""Infer authorization output schemas from rule metadata without reading rows."""

import re

import pyarrow as pa

from pypaimon.common.predicate_json_parser import _DATE_EXTRACT_NAMES


_INT_MAX = (1 << 31) - 1
_CHARACTER_TYPE = re.compile(
    r"\s*(?:STRING(?=\s|\Z)|(?:CHAR|VARCHAR)\s*(?:\(\s*(\d+)\s*\)|(?=\s|\Z)))"
    r"(?:\s*(?:NOT\s+)?NULL)?\s*\Z", re.IGNORECASE)
_INTEGER_TYPE = re.compile(
    r"\s*(?:TINYINT|SMALLINT|INT|INTEGER|BIGINT)(?=\s|\Z)"
    r"(?:\s*(?:NOT\s+)?NULL)?\s*\Z", re.IGNORECASE)


def effective_masked_schema(schema, masking):
    if not masking:
        return schema
    fields = []
    for field in schema:
        transform = masking.get(field.name)
        if transform is None:
            fields.append(field)
        else:
            data_type = _transform_type(transform, schema, field.type)
            fields.append(pa.field(field.name, data_type, nullable=True, metadata=field.metadata))
    return pa.schema(fields, metadata=schema.metadata)


def _field_type(reference, schema):
    name = reference.get('name') if isinstance(reference, dict) else None
    if not isinstance(name, str) or name not in schema.names:
        raise ValueError('Column masking refers to a field that is not present: {!r}'.format(reference))
    return schema.field(name).type


def _string_type(value, schema, stored=False, coerce_literals=False):
    if value is None or isinstance(value, str):
        return pa.string()
    if not isinstance(value, dict):
        if coerce_literals:
            return pa.string()
        raise ValueError('Transform input must be a string or a field: {!r}'.format(value))
    if stored:
        spelling = value.get('type')
        match = _CHARACTER_TYPE.fullmatch(spelling) if isinstance(spelling, str) else None
        if match is None or (match.group(1) is not None and not 1 <= int(match.group(1)) <= _INT_MAX):
            raise ValueError('Transform field must be a string: {!r}'.format(spelling))
    data_type = _field_type(value, schema)
    if not (pa.types.is_string(data_type) or pa.types.is_large_string(data_type)):
        raise ValueError('Transform field must be a string: {}'.format(data_type))
    return data_type


def _integer_input(value, schema, stored=True):
    if value is None or (isinstance(value, int) and not isinstance(value, bool)):
        return
    if not isinstance(value, dict):
        raise ValueError('Transform position must be an integer or a field: {!r}'.format(value))
    spelling = value.get('type')
    if stored and (not isinstance(spelling, str) or _INTEGER_TYPE.fullmatch(spelling) is None):
        raise ValueError('Transform position field must be an integer: {!r}'.format(spelling))
    if not pa.types.is_integer(_field_type(value, schema)):
        raise ValueError('Transform position field must be an integer')


def _inputs(transform, minimum, maximum):
    values = transform.get('inputs')
    if not isinstance(values, list) or not minimum <= len(values) <= maximum:
        raise ValueError('{} takes {} to {} inputs'.format(transform['name'], minimum, maximum))
    return values


def _transform_type(transform, schema, null_type):
    name = transform['name']
    if name == 'NULL':
        return null_type
    if name == 'FIELD_REF':
        return _field_type(transform['fieldRef'], schema)
    if name == 'CAST':
        _field_type(transform['fieldRef'], schema)
        _paimon_type_to_arrow(transform['fieldRef']['type'])
        target = transform['type']
        width = re.fullmatch(r'\s*(CHAR|VARCHAR|BINARY|VARBINARY)(?=\s|\(|\Z)'
                             r'(?:\s*\(\s*([0-9]+)\s*\))?'
                             r'(?:\s*(?:NOT\s+)?NULL)?\s*', target, flags=re.IGNORECASE)
        if width is not None:
            if not 1 <= int(width.group(2) or 1) <= _INT_MAX:
                raise ValueError('Cast width must be between 1 and 2147483647')
            return pa.string() if width.group(1).upper() in ('CHAR', 'VARCHAR') else pa.binary()
        return _paimon_type_to_arrow(target)
    if name in _DATE_EXTRACT_NAMES:
        reference = transform['fieldRef']
        stored = reference.get('type')
        match = re.fullmatch(r'\s*(DATE(?=\s|\Z)|TIMESTAMP(?=\s|\(|\Z)(?:\s*\(\s*([0-9]+)\s*\))?'
                             r'(?:\s*WITHOUT\s+TIME\s+ZONE(?=\s|\Z))?)'
                             r'(?:\s*(?:NOT\s+)?NULL)?\s*', stored, flags=re.IGNORECASE) \
            if isinstance(stored, str) else None
        actual = _field_type(reference, schema)
        if (match is None or (match.group(2) is not None and int(match.group(2)) > 9)
                or not (pa.types.is_date32(actual) or (pa.types.is_timestamp(actual) and actual.tz is None))):
            raise ValueError('Calendar transforms require DATE or TIMESTAMP without time zone')
        return pa.int32()
    if name in ('UPPER', 'LOWER', 'LENGTH', 'BIT_LENGTH'):
        values = _inputs(transform, 1, 1)
        result = _string_type(values[0], schema, stored=name in ('LENGTH', 'BIT_LENGTH'),
                              coerce_literals=name in ('UPPER', 'LOWER'))
        return pa.int32() if name in ('LENGTH', 'BIT_LENGTH') else result
    if name in ('CONCAT', 'CONCAT_WS'):
        values = _inputs(transform, 0 if name == 'CONCAT' else 1, _INT_MAX)
        types = [_string_type(value, schema, coerce_literals=True) for value in values]
        # Python Arrow's CONCAT also supplies a string separator. A large-string
        # argument cannot bind that mixed-width kernel, even for an empty batch.
        if name == 'CONCAT' and any(pa.types.is_large_string(data_type) for data_type in types):
            raise TypeError('CONCAT inputs must use the same string width as its separator')
        return pa.string()
    if name == 'SUBSTRING':
        values = _inputs(transform, 2, 3)
        result = _string_type(values[0], schema)
        for value in values[1:]:
            if isinstance(value, str):
                continue
            _integer_input(value, schema, stored=False)
        return result
    if name == 'TRIM':
        values = _inputs(transform, 1, 2)
        if transform.get('trimFlag') not in ('BOTH', 'LEADING', 'TRAILING'):
            raise ValueError('Unknown trimFlag: {}'.format(transform.get('trimFlag')))
        types = [_string_type(value, schema, stored=True) for value in values]
        return types[0]
    if name in ('PAD', 'TRANSLATE', 'OVERLAY'):
        values = _inputs(transform, 3, 4 if name == 'OVERLAY' else 3)
        _string_type(values[0], schema, stored=True)
        if name == 'PAD':
            if transform.get('direction') not in ('LEFT', 'RIGHT'):
                raise ValueError('PAD direction must be LEFT or RIGHT')
            _integer_input(values[1], schema)
            _string_type(values[2], schema, stored=True)
        elif name == 'TRANSLATE':
            for value in values[1:]:
                _string_type(value, schema, stored=True)
        else:
            _string_type(values[1], schema, stored=True)
            for value in values[2:]:
                _integer_input(value, schema)
        return pa.string()
    raise ValueError('Unknown transform type: {}'.format(name))


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
