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


def _apply_predicate_transform(transform: dict, batch: pa.RecordBatch) -> pa.Array:
    name = transform["name"]

    if name == "FIELD_REF":
        return batch.column(transform["fieldRef"]["name"])

    elif name == "CAST":
        col = batch.column(transform["fieldRef"]["name"])
        target_type = _paimon_type_to_arrow(transform["type"])
        return pc.cast(col, target_type)

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
        return pc.binary_join_element_wise(*values, sep, null_handling='skip')

    elif name == "NULL":
        return pa.nulls(len(batch), type=pa.bool_())

    raise ValueError(f"Unknown transform type in predicate: {name}")


def _resolve_transform_input(inp, batch: pa.RecordBatch) -> pa.Array:
    if isinstance(inp, dict):
        return batch.column(inp["name"])
    elif isinstance(inp, str):
        return pa.array([inp] * len(batch), type=pa.string())
    elif inp is None:
        return pa.nulls(len(batch), type=pa.string())
    return pa.array([str(inp)] * len(batch), type=pa.string())


def _apply_leaf_function(function: str, value_array: pa.Array, literals: list, batch_len: int) -> pa.Array:
    converted = [_convert_literal(lit, value_array.type) for lit in literals]

    if function == "EQUAL":
        return pc.equal(value_array, converted[0])
    elif function == "NOT_EQUAL":
        return pc.not_equal(value_array, converted[0])
    elif function == "LESS_THAN":
        return pc.less(value_array, converted[0])
    elif function == "LESS_OR_EQUAL":
        return pc.less_equal(value_array, converted[0])
    elif function == "GREATER_THAN":
        return pc.greater(value_array, converted[0])
    elif function == "GREATER_OR_EQUAL":
        return pc.greater_equal(value_array, converted[0])
    elif function == "IS_NULL":
        return pc.is_null(value_array)
    elif function == "IS_NOT_NULL":
        return pc.is_valid(value_array)
    elif function == "IN":
        return pc.is_in(value_array, pa.array(converted, type=value_array.type))
    elif function == "NOT_IN":
        return pc.invert(pc.is_in(value_array, pa.array(converted, type=value_array.type)))
    elif function == "BETWEEN":
        return pc.and_(pc.greater_equal(value_array, converted[0]),
                       pc.less_equal(value_array, converted[1]))
    elif function == "NOT_BETWEEN":
        return pc.or_(pc.less(value_array, converted[0]),
                      pc.greater(value_array, converted[1]))
    elif function == "STARTS_WITH":
        return pc.starts_with(value_array, converted[0])
    elif function == "ENDS_WITH":
        return pc.ends_with(value_array, converted[0])
    elif function == "CONTAINS":
        return pc.match_substring(value_array, converted[0])
    elif function == "LIKE":
        raw = literals[0]
        escaped = re.escape(raw)
        pattern = escaped.replace("%", ".*").replace("_", ".")
        return pc.match_substring_regex(value_array, f"^{pattern}$")
    elif function == "TRUE":
        return pa.array([True] * batch_len, type=pa.bool_())
    elif function == "FALSE":
        return pa.array([False] * batch_len, type=pa.bool_())
    raise ValueError(f"Unknown leaf function: {function}")


def _convert_literal(literal, target_type: pa.DataType):
    if literal is None:
        return None
    if pa.types.is_timestamp(target_type):
        import datetime
        if isinstance(literal, str):
            dt = datetime.datetime.fromisoformat(literal.replace("Z", "+00:00"))
            return pa.scalar(dt, type=target_type)
    elif pa.types.is_date(target_type):
        import datetime
        if isinstance(literal, str):
            return pa.scalar(datetime.date.fromisoformat(literal), type=target_type)
    elif pa.types.is_time(target_type):
        import datetime
        if isinstance(literal, str):
            t = datetime.time.fromisoformat(literal)
            return pa.scalar(t, type=target_type)
    elif pa.types.is_decimal(target_type):
        import decimal
        return pa.scalar(decimal.Decimal(str(literal)), type=target_type)
    return literal


def _paimon_type_to_arrow(paimon_type: str) -> pa.DataType:
    type_str = paimon_type.strip().upper()

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
    elif name in ("UPPER", "LOWER", "CONCAT", "CONCAT_WS"):
        for inp in transform.get("inputs", []):
            if isinstance(inp, dict) and "name" in inp:
                fields.add(inp["name"])
    return fields
