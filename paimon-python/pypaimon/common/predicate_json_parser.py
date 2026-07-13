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

    elif name == "NULL":
        return pa.nulls(len(batch), type=null_type)

    raise ValueError(f"Unknown transform type: {name}")


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
