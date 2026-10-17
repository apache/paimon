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

import base64
import decimal
import logging
import math
import re
import struct
import threading
import time
import uuid
from collections import namedtuple
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import json
from typing import Any, Dict, List, Optional, Tuple, Union, TYPE_CHECKING
from urllib.parse import urlparse

if TYPE_CHECKING:
    from pypaimon.catalog.rest.rest_token import RESTToken

from pypaimon.api.api_request import (AlterDatabaseRequest, AlterTableRequest,
                                      CreateBranchRequest,
                                      CreateDatabaseRequest,
                                      CreatePartitionsRequest,
                                      CreateTableRequest, CreateTagRequest,
                                      DropPolicyRequest,
                                      GrantPermissionRequest, PolicyRequest,
                                      RenameTableRequest,
                                      RevokePermissionRequest)
from pypaimon.api.api_response import (ConfigResponse, CreatePartitionsResponse,
                                       GetDatabaseResponse,
                                       GetFunctionResponse,
                                       GetTableResponse, GetTagResponse,
                                       ListBranchesResponse,
                                       ListDatabasesResponse,
                                       ListFunctionDetailsResponse,
                                       ListFunctionsGloballyResponse,
                                       ListFunctionsResponse,
                                       ListPartitionsResponse,
                                       ListPermissionsResponse,
                                       ListPoliciesResponse, ListTablesResponse,
                                       ListTagsResponse,
                                       PagedList, Partition,
                                       RESTResponse, ErrorResponse)
from pypaimon.api.resource_paths import ResourcePaths
from pypaimon.api.rest_util import RESTUtil
from pypaimon.catalog.catalog_exception import (BranchAlreadyExistException,
                                                BranchNotExistException,
                                                DatabaseNoPermissionException,
                                                DatabaseNotExistException,
                                                TableNoPermissionException,
                                                TableNotExistException, DatabaseAlreadyExistException,
                                                TableAlreadyExistException,
                                                FunctionNotExistException,
                                                FunctionAlreadyExistException,
                                                DefinitionAlreadyExistException,
                                                DefinitionNotExistException,
                                                TagNotExistException,
                                                TagAlreadyExistException)
from pypaimon.casting import data_type_casts as casts
from pypaimon.catalog.rest.table_metadata import TableMetadata
from pypaimon.common.identifier import Identifier
from pypaimon.api.typedef import RESTAuthParameter
from pypaimon.common.json_util import JSON
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.management.column_mask import ColumnMask
from pypaimon.management.data_policy import DataPolicy
from pypaimon.management.list_permissions_request import ListPermissionsRequest
from pypaimon.management.permission_access import PermissionAccess
from pypaimon.management.permission_assignment import (PermissionAssignment, _INSTANT,
                                                       _days_from_civil, _days_in_month)
from pypaimon.management.permission_columns import PermissionColumns
from pypaimon.management.permission_resource import PermissionResource
from pypaimon.management.resource_type import ResourceType
from pypaimon.management.row_filter import RowFilter
from pypaimon import Schema
from pypaimon.schema.data_types import (ArrayType, AtomicType, DataField, DataType, DataTypeParser, MapType,
                                        MultisetType, RowType, VectorType)
from pypaimon.schema.schema_change import Actions, SchemaChange
from pypaimon.schema.schema_manager import SchemaManager
from pypaimon.schema.table_schema import TableSchema

# Constants
DEFAULT_MAX_RESULTS = 100
AUTHORIZATION_HEADER_KEY = "Authorization"

# REST API parameter constants
DATABASE_NAME_PATTERN = "databaseNamePattern"
TABLE_NAME_PATTERN = "tableNamePattern"
TABLE_TYPE = "tableType"
VIEW_NAME_PATTERN = "viewNamePattern"
FUNCTION_NAME_PATTERN = "functionNamePattern"
PARTITION_NAME_PATTERN = "partitionNamePattern"
TAG_NAME_PREFIX = "tagNamePrefix"
MAX_RESULTS = "maxResults"
PAGE_TOKEN = "pageToken"

# Core options
PATH = "path"
TYPE = "type"
WAREHOUSE = "warehouse"
SNAPSHOT_CLEAN_EMPTY_DIRECTORIES = "snapshot.clean-empty-directories"

# Table types
FORMAT_TABLE = "FORMAT_TABLE"
OBJECT_TABLE = "OBJECT_TABLE"


def _dict_to_schema_change(change_dict: dict) -> SchemaChange:
    from pypaimon.schema.schema_change import (
        SetOption, RemoveOption, UpdateComment, AddColumn, RenameColumn,
        DropColumn, UpdateColumnType, UpdateColumnNullability,
        UpdateColumnComment, UpdateColumnDefaultValue, UpdateColumnPosition, Move, MoveType
    )

    action = change_dict.get(Actions.FIELD_ACTION)
    if action == Actions.SET_OPTION_ACTION:
        return SetOption(key=change_dict["key"], value=change_dict["value"])
    elif action == Actions.REMOVE_OPTION_ACTION:
        return RemoveOption(key=change_dict["key"])
    elif action == Actions.UPDATE_COMMENT_ACTION:
        return UpdateComment(comment=change_dict.get("comment"))
    elif action == Actions.ADD_COLUMN_ACTION:
        from pypaimon.schema.data_types import DataTypeParser
        data_type_value = change_dict.get("dataType") or change_dict.get(AddColumn.FIELD_DATA_TYPE)
        if data_type_value is None:
            raise ValueError(f"Missing dataType field in AddColumn change: {change_dict}")
        data_type = DataTypeParser.parse_data_type(data_type_value)
        move = None
        if "move" in change_dict and change_dict["move"] is not None:
            move_dict = change_dict["move"]
            if isinstance(move_dict, dict):
                move_type_str = move_dict.get("type") or move_dict.get(Move.FIELD_TYPE)
                if move_type_str is None:
                    raise ValueError(f"Missing type field in Move: {move_dict}")
                move_type = MoveType(move_type_str)
                field_name = move_dict.get("fieldName") or move_dict.get(Move.FIELD_FIELD_NAME)
                if field_name is None:
                    raise ValueError(f"Missing fieldName field in Move: {move_dict}")
                reference_field = (
                    move_dict.get("referenceFieldName") or
                    move_dict.get(Move.FIELD_REFERENCE_FIELD_NAME)
                )
                move = Move(
                    field_name=field_name,
                    reference_field_name=reference_field,
                    type=move_type
                )
        field_names = change_dict.get("fieldNames") or change_dict.get(AddColumn.FIELD_FIELD_NAMES)
        if field_names is None:
            raise ValueError(f"Missing fieldNames field in AddColumn change: {change_dict}")
        return AddColumn(
            field_names=field_names,
            data_type=data_type,
            comment=change_dict.get("comment") or change_dict.get(AddColumn.FIELD_COMMENT),
            move=move
        )
    elif action == Actions.RENAME_COLUMN_ACTION:
        return RenameColumn(field_names=change_dict["fieldNames"], new_name=change_dict["newName"])
    elif action == Actions.DROP_COLUMN_ACTION:
        return DropColumn(field_names=change_dict["fieldNames"])
    elif action == Actions.UPDATE_COLUMN_TYPE_ACTION:
        from pypaimon.schema.data_types import DataTypeParser
        new_type = DataTypeParser.parse_data_type(change_dict["newDataType"])
        return UpdateColumnType(
            field_names=change_dict["fieldNames"],
            new_data_type=new_type,
            keep_nullability=change_dict.get("keepNullability", False)
        )
    elif action == Actions.UPDATE_COLUMN_NULLABILITY_ACTION:
        return UpdateColumnNullability(
            field_names=change_dict["fieldNames"],
            new_nullability=change_dict["newNullability"]
        )
    elif action == Actions.UPDATE_COLUMN_COMMENT_ACTION:
        return UpdateColumnComment(
            field_names=change_dict["fieldNames"],
            new_comment=change_dict.get("newComment")
        )
    elif action == Actions.UPDATE_COLUMN_DEFAULT_VALUE_ACTION:
        return UpdateColumnDefaultValue(
            field_names=change_dict["fieldNames"],
            new_default_value=change_dict["newDefaultValue"]
        )
    elif action == Actions.UPDATE_COLUMN_POSITION_ACTION:
        move_dict = change_dict.get("move") or change_dict.get(UpdateColumnPosition.FIELD_MOVE)
        if move_dict is None:
            raise ValueError(f"Missing move field in UpdateColumnPosition change: {change_dict}")
        if not isinstance(move_dict, dict):
            raise ValueError(f"move field must be a dict in UpdateColumnPosition change: {change_dict}")
        move_type_str = move_dict.get("type") or move_dict.get(Move.FIELD_TYPE)
        if move_type_str is None:
            raise ValueError(f"Missing type field in Move: {move_dict}")
        move_type = MoveType(move_type_str)
        field_name = move_dict.get("fieldName") or move_dict.get(Move.FIELD_FIELD_NAME)
        if field_name is None:
            raise ValueError(f"Missing fieldName field in Move: {move_dict}")
        move = Move(
            field_name=field_name,
            reference_field_name=move_dict.get("referenceFieldName") or move_dict.get(Move.FIELD_REFERENCE_FIELD_NAME),
            type=move_type
        )
        return UpdateColumnPosition(move=move)
    else:
        raise ValueError(f"Unknown schema change action: {action}")


def _cursor_key(*parts: Optional[str]) -> str:
    return "".join("-1:" if part is None else "{}:{}".format(len(part), part) for part in parts)


def _permission_key(resource: PermissionResource, access: str, principal: str) -> str:
    return _cursor_key(resource.get_type().name, resource.get_database(), resource.get_table(),
                       resource.get_function(), resource.get_view(),
                       PermissionAccess.canonicalize_for(resource, access), principal)


def _assignment_key(assignment: PermissionAssignment) -> str:
    return _permission_key(
        assignment.get_resource(), assignment.get_access(), assignment.get_principal())


def _resource_name(resource: PermissionResource) -> str:
    name = resource.get_table() or resource.get_function() or resource.get_view()
    return ".".join(part for part in (resource.get_database(), name) if part) or "catalog"


def _policy_identity(policy: DataPolicy) -> Tuple[str, str, Optional[str]]:
    mask = policy.get_column_mask()
    return policy.type().name, policy.get_principal(), None if mask is None else mask.get_on_column()


def _policy_key(policy: DataPolicy) -> str:
    return _cursor_key(*_policy_identity(policy))


def _policy_name(policy_type: str, principal: str, column: Optional[str]) -> str:
    return ":".join(part for part in (policy_type, principal, column) if part is not None)


def _field_refs(transform: Dict[str, Any]) -> List[Dict[str, Any]]:
    refs = list(transform.get("inputs") or []) + [transform.get("fieldRef")]
    return [ref for ref in refs if isinstance(ref, dict)]


def _leaves(predicate: Dict[str, Any]) -> List[Dict[str, Any]]:
    if predicate["kind"] == "COMPOUND":
        children = predicate.get("children")
        if predicate.get("function") not in ("AND", "OR") or not children or None in children:
            raise ValueError("Compound predicate needs AND or OR and at least one predicate.")
        return [leaf for child in children for leaf in _leaves(child)]
    if predicate["kind"] != "LEAF" or not isinstance(predicate.get("transform"), dict):
        raise ValueError("Unknown predicate {}.".format(json.dumps(predicate)))
    return [predicate]


def _reject_constant(token: str) -> None:
    raise ValueError("Non-standard JSON token {}.".format(token))


def _policy_json(text: str) -> Any:
    return json.loads(text, parse_constant=_reject_constant)


def _policy_transforms(policy: DataPolicy) -> List[Tuple[Dict[str, Any], Optional[Dict[str, Any]]]]:
    mask = policy.get_column_mask()
    if mask is not None:
        return [(_policy_json(mask.get_transform()), None)]
    return [(leaf["transform"], leaf) for leaf in _leaves(_policy_json(policy.get_row_filter().get_predicate()))]


def _policy_columns(policy: DataPolicy) -> set:
    mask = policy.get_column_mask()
    columns = {ref["name"] for transform, _ in _policy_transforms(policy) for ref in _field_refs(transform)}
    return columns | ({mask.get_on_column()} if mask is not None else set())


_DATE_PARTS = ("YEAR", "QUARTER", "MONTH", "WEEK", "YEAR_OF_WEEK", "DAY", "DAY_OF_WEEK", "ISO_DAY_OF_WEEK",
               "WEEKDAY", "DAY_OF_YEAR", "HOUR", "MINUTE", "SECOND")
_TRANSFORM_INPUTS = {
    "CONCAT": ("s", 0, None, "STRING"), "CONCAT_WS": ("s", 2, None, "STRING"),
    "UPPER": ("s", 1, 1, "STRING"), "LOWER": ("s", 1, 1, "STRING"), "TRIM": ("s", 1, 2, "STRING"),
    "TRANSLATE": ("s", 3, 3, "STRING"), "SUBSTRING": ("ra", 2, 3, "STRING"),
    "OVERLAY": ("ssii", 3, 4, "STRING"), "PAD": ("sis", 3, 3, "STRING"),
    "LENGTH": ("s", 1, 1, "INT"), "BIT_LENGTH": ("s", 1, 1, "INT"),
    "DATE_ADD": ("di", 2, 2, "DATE"), "DATE_DIFF": ("dd", 2, 2, "INT"), "DATE_TRUNC": ("ds", 2, 2, "DATE"),
}
_TRANSFORM_FLAGS = {"TRIM": ("trimFlag", ("LEADING", "TRAILING", "BOTH")), "PAD": ("direction", ("LEFT", "RIGHT"))}
_INPUT_ROOTS = {"s": casts.CHARACTER_STRING, "i": casts.INTEGER_NUMERIC, "d": {casts.DATE}}
_TYPE_DEFAULTS = {casts.CHAR: (1,), casts.VARCHAR: (1,), casts.BINARY: (1,), casts.VARBINARY: (1,),
                  casts.DECIMAL: (10, 0), casts.TIME: (0,), casts.TIMESTAMP: (6,), casts.TIMESTAMP_LTZ: (6,)}
_LEAF_FUNCTIONS = {
    "EQUAL", "NOT_EQUAL", "LESS_THAN", "LESS_OR_EQUAL", "GREATER_THAN", "GREATER_OR_EQUAL", "IS_NULL",
    "IS_NOT_NULL", "STARTS_WITH", "NOT_STARTS_WITH", "ENDS_WITH", "NOT_ENDS_WITH", "CONTAINS", "NOT_CONTAINS",
    "ARRAY_CONTAINS", "ARRAYS_OVERLAP", "ARRAY_CONTAINS_ALL", "LIKE", "NOT_LIKE", "IN", "NOT_IN", "BETWEEN",
    "NOT_BETWEEN", "TRUE", "FALSE"}
_ARRAY_FUNCTIONS = ("ARRAY_CONTAINS", "ARRAYS_OVERLAP", "ARRAY_CONTAINS_ALL")
_PRIMITIVES = casts.INTEGER_NUMERIC | {casts.FLOAT, casts.DOUBLE}
_CAST_RULES = (
    (casts.CHARACTER_STRING, casts.CHARACTER_STRING | casts.BINARY_STRING | _PRIMITIVES | casts.TIMESTAMP_FAMILY
     | {casts.BOOLEAN, casts.DATE, casts.DECIMAL, casts.TIME, casts.ARRAY, casts.MAP, casts.ROW}),
    (casts.BINARY_STRING, casts.BINARY_STRING | casts.CHARACTER_STRING),
    ({casts.BOOLEAN}, casts.NUMERIC | casts.CHARACTER_STRING),
    ({casts.DATE}, casts.CHARACTER_STRING | casts.TIMESTAMP_FAMILY),
    ({casts.DECIMAL}, _PRIMITIVES | {casts.DECIMAL}),
    (_PRIMITIVES, _PRIMITIVES | {casts.DECIMAL}),
    (casts.INTEGER_NUMERIC, {casts.BOOLEAN}),
    (casts.NUMERIC, casts.CHARACTER_STRING),
    ({casts.INTEGER, casts.BIGINT}, casts.TIMESTAMP_FAMILY),
    ({casts.ARRAY, casts.MAP, casts.ROW}, casts.CHARACTER_STRING),
    (casts.TIMESTAMP_FAMILY, casts.CHARACTER_STRING | casts.TIMESTAMP_FAMILY
     | {casts.DATE, casts.TIME, casts.BIGINT, casts.INTEGER}),
    ({casts.TIME}, casts.CHARACTER_STRING | casts.TIMESTAMP_FAMILY),
)
_DECIMAL_LITERAL = re.compile(r"[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?")
_LOCAL_DATE = re.compile(r"([0-9]{4}|\+[0-9]{5,9}|-[0-9]{4,9})-([0-9]{2})-([0-9]{2})")
_LOCAL_TIME = re.compile(r"([0-9]{2}):([0-9]{2})(?::([0-9]{2})(?:\.([0-9]{0,9}))?)?")
_INTEGER_BITS = {casts.TINYINT: 8, casts.SMALLINT: 16, casts.INTEGER: 32, casts.BIGINT: 64}


def _type_root(data_type: Optional[DataType]) -> Optional[str]:
    if isinstance(data_type, AtomicType) and "LOCAL TIME ZONE" in data_type.type.upper():
        return casts.TIMESTAMP_LTZ
    return casts._root(data_type)


def _logical_type(data_type: Optional[DataType]) -> Any:
    if isinstance(data_type, RowType):
        return "ROW", tuple((field.id, field.name, _logical_type(field.type), field.description, field.default_value)
                            for field in data_type.fields), data_type.nullable
    if isinstance(data_type, MapType):
        return "MAP", _logical_type(data_type.key), _logical_type(data_type.value), data_type.nullable
    if isinstance(data_type, (ArrayType, MultisetType, VectorType)):
        return type(data_type).__name__, _logical_type(data_type.element), getattr(data_type, "length", None), \
            data_type.nullable
    if not isinstance(data_type, AtomicType):
        return data_type
    spelling = data_type.type.upper()
    root = _type_root(data_type)
    found = (2147483647,) if spelling in ("STRING", "BYTES") else tuple(map(int, re.findall(r"\d+", spelling)))
    return root, found + _TYPE_DEFAULTS.get(root, ())[len(found):], data_type.nullable


def _castable(source: DataType, target: DataType) -> bool:
    if not any(_type_root(source) in inputs and _type_root(target) in targets for inputs, targets in _CAST_RULES):
        return False
    if _type_root(source) not in casts.CHARACTER_STRING or not isinstance(target, (ArrayType, MapType, RowType)):
        return True
    children = [field.type for field in target.fields] if isinstance(target, RowType) else \
        [target.key, target.value] if isinstance(target, MapType) else [target.element]
    return all(_castable(source, child) for child in children)


def _java_integer(value: Any, bits: int) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("Not a number: {!r}".format(value))
    if isinstance(value, float):
        limit = 2 ** (63 if bits == 64 else 31)
        value = 0 if math.isnan(value) else int(max(-limit, min(limit - 1, value)))
    value &= (1 << bits) - 1
    return value - (1 << bits) if value >> (bits - 1) else value


class _JavaFloat(float):
    pass


def _float32(value: float) -> float:
    return struct.unpack("f", struct.pack("f", value))[0]


def _java_float(value: Any, single: bool) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError("Not a number: {!r}".format(value))
    try:
        value = float(value)
        return _JavaFloat(_float32(value)) if single else value
    except OverflowError:
        return (_JavaFloat if single else float)(math.inf if value > 0 else -math.inf)


def _java_number_string(value: float, single: bool = False) -> str:
    if math.isnan(value) or math.isinf(value):
        return "NaN" if math.isnan(value) else "Infinity" if value > 0 else "-Infinity"
    if value == 0:
        return "-0.0" if math.copysign(1, value) < 0 else "0.0"
    fits = (lambda text: _float32(float(text)) == value) if single else (lambda text: float(text) == value)
    digits = next(digits for digits in range(1, 18) if fits("{:.{}e}".format(value, digits - 1)))
    number = decimal.Decimal("{:.{}e}".format(value, max(digits, 2) - 1)).normalize()
    if 1e-3 <= abs(value) < 1e7:
        text = "{:f}".format(number)
        return text if "." in text else text + ".0"
    sign, digits, exponent = number.as_tuple()
    mantissa = "".join(map(str, digits))
    return "{}{}.{}E{}".format("-" if sign else "", mantissa[0], mantissa[1:] or "0", exponent + len(digits) - 1)


class _UnsupportedConversion(ValueError):
    pass


def _java_string(value: Any) -> str:
    if isinstance(value, (float, list, dict)):
        raise _UnsupportedConversion("Policy literal {} cannot be compared as a string.".format(json.dumps(value)))
    if isinstance(value, _Binary):
        return value.text
    if isinstance(value, _Decimal):
        return "{:f}".format(value.value)
    if isinstance(value, _Timestamp):
        return _java_local_date_time(value)
    if isinstance(value, str):
        return value
    if isinstance(value, bool) or value is None:
        return {True: "true", False: "false", None: "null"}[value]
    if isinstance(value, float):
        return _java_number_string(value, isinstance(value, _JavaFloat))
    if isinstance(value, list):
        return "[" + ", ".join(map(_java_string, value)) + "]"
    if isinstance(value, dict):
        return "{" + ", ".join("{}={}".format(key, _java_string(item)) for key, item in value.items()) + "}"
    return str(value)


_Binary = namedtuple("_Binary", "text")
_Decimal = namedtuple("_Decimal", "value precision")
_Timestamp = namedtuple("_Timestamp", "millisecond nano_of_millisecond")


def _java_decimal(value: Any, literal_type: DataType) -> Optional[_Decimal]:
    if not isinstance(value, str) or _DECIMAL_LITERAL.fullmatch(value) is None:
        raise ValueError("Not a decimal: {!r}".format(value))
    precision, scale = _logical_type(literal_type)[1]
    with decimal.localcontext() as context:
        context.prec, context.Emax, context.Emin = 10 ** 6, decimal.MAX_EMAX, decimal.MIN_EMIN
        try:
            value = decimal.Decimal(value).quantize(decimal.Decimal(1).scaleb(-scale), rounding=decimal.ROUND_HALF_UP)
        except decimal.InvalidOperation:
            return None
    if len(value.as_tuple().digits) > precision:
        return None
    return _Decimal(abs(value) if value.is_zero() else value, precision)


def _java_date(year: int, month: int, day: int) -> str:
    return "{}{:04d}-{:02d}-{:02d}".format("-" if year < 0 else "+" if year > 9999 else "", abs(year), month, day)


def _java_time(hour: int, minute: int, second: int, nanos: int, seconds: bool = False) -> str:
    text = "{:02d}:{:02d}".format(hour, minute)
    if second or nanos or seconds:
        text += ":{:02d}".format(second)
    if nanos:
        fraction = "{:09d}".format(nanos)
        text += "." + (fraction[:3] if nanos % 10 ** 6 == 0 else fraction[:6] if nanos % 1000 == 0 else fraction)
    return text


def _parse_date(text: str) -> Tuple[int, int, int]:
    match = _LOCAL_DATE.fullmatch(text)
    year, month, day = map(int, match.groups()) if match else (0, 0, 0)
    if not 1 <= month <= 12 or not 1 <= day <= _days_in_month(year, month):
        raise ValueError("Not a date: {!r}".format(text))
    return year, month, day


def _parse_time(text: str) -> Tuple[int, int, int, int]:
    match = _LOCAL_TIME.fullmatch(text)
    if match is None or int(match.group(1)) > 23 or int(match.group(2)) > 59 or int(match.group(3) or 0) > 59:
        raise ValueError("Not a time: {!r}".format(text))
    return int(match.group(1)), int(match.group(2)), int(match.group(3) or 0), int((match.group(4) or "").ljust(9, "0"))


def _civil_from_days(days: int) -> Tuple[int, int, int]:
    days += 719468
    era = days // 146097
    day_of_era = days - era * 146097
    year_of_era = (day_of_era - day_of_era // 1460 + day_of_era // 36524 - day_of_era // 146096) // 365
    day_of_year = day_of_era - (365 * year_of_era + year_of_era // 4 - year_of_era // 100)
    shifted_month = (5 * day_of_year + 2) // 153
    month = shifted_month + 3 if shifted_month < 10 else shifted_month - 9
    return year_of_era + era * 400 + (month <= 2), month, day_of_year - (153 * shifted_month + 2) // 5 + 1


def _timestamp(days: int, hour: int, minute: int, second: int, nanos: int) -> _Timestamp:
    nano_of_day = ((hour * 60 + minute) * 60 + second) * 10 ** 9 + nanos
    return _Timestamp(days * 86400000 + nano_of_day // 10 ** 6, nano_of_day % 10 ** 6)


def _parse_instant(text: str) -> _Timestamp:
    match = _INSTANT.fullmatch(text)
    if match is None:
        raise ValueError("Not an instant: {!r}".format(text))
    sign, digits, month, day, hour, minute, second, fraction = match.groups()
    year, month, day = int(sign + digits), int(month), int(day)
    hour, minute, second = int(hour), int(minute), int(second)
    nanos, next_day = int((fraction or "").ljust(9, "0")), hour == 24 and minute == second == 0
    hour, second = 0 if next_day else hour, 59 if (hour, minute, second) == (23, 59, 60) else second
    if not 1 <= month <= 12 or not 1 <= day <= _days_in_month(year, month) or hour > 23 or minute > 59 \
            or second > 59 or (next_day and nanos):
        raise ValueError("Not an instant: {!r}".format(text))
    return _timestamp(_days_from_civil(year, month, day) + next_day, hour, minute, second, nanos)


def _date_time_parts(timestamp: _Timestamp) -> Tuple[str, int, int, int, int]:
    days, millis = divmod(timestamp.millisecond, 86400000)
    seconds, nanos = divmod(millis * 10 ** 6 + timestamp.nano_of_millisecond, 10 ** 9)
    return _java_date(*_civil_from_days(days)), seconds // 3600, seconds // 60 % 60, seconds % 60, nanos


def _java_local_date_time(timestamp: _Timestamp) -> str:
    date, hour, minute, second, nanos = _date_time_parts(timestamp)
    return date + "T" + _java_time(hour, minute, second, nanos)


def _java_instant(timestamp: _Timestamp) -> str:
    date, hour, minute, second, nanos = _date_time_parts(timestamp)
    return date + "T" + _java_time(hour, minute, second, nanos, seconds=True) + "Z"


def _internal_literal(literal_type: DataType, literal: Any) -> Any:
    root = _type_root(literal_type)
    if literal is None or root == casts.BOOLEAN:
        return literal
    if root in _INTEGER_BITS:
        return _java_integer(literal, _INTEGER_BITS[root])
    if root in (casts.FLOAT, casts.DOUBLE):
        return _java_float(literal, root == casts.FLOAT)
    if root in casts.CHARACTER_STRING:
        return _Binary(_java_string(literal))
    if not isinstance(literal, str):
        raise ValueError("{} literal must be a string: {!r}".format(root, literal))
    if root == casts.DECIMAL:
        return _java_decimal(literal, literal_type)
    if root == casts.DATE:
        return _days_from_civil(*_parse_date(literal))
    if root == casts.TIME:
        hour, minute, second, nanos = _parse_time(literal)
        return ((hour * 60 + minute) * 60 + second) * 1000 + nanos // 10 ** 6
    if root == casts.TIMESTAMP:
        parts = re.split("[Tt]", literal, maxsplit=1)
        if len(parts) != 2:
            raise ValueError("Not a timestamp: {!r}".format(literal))
        return _timestamp(_days_from_civil(*_parse_date(parts[0])), *_parse_time(parts[1]))
    if root == casts.TIMESTAMP_LTZ:
        return _parse_instant(literal)
    raise ValueError("Unsupported literal type {}.".format(literal_type))


def _json_literal(literal_type: DataType, value: Any) -> Any:
    root = _type_root(literal_type)
    single = isinstance(value, _JavaFloat)
    if isinstance(value, float) and root != (casts.FLOAT if single else casts.DOUBLE) \
            and (root in _INTEGER_BITS or root in (casts.FLOAT, casts.DOUBLE)) \
            and decimal.Decimal(_java_number_string(value, single)) != decimal.Decimal(value):
        raise _UnsupportedConversion("Policy literal {} cannot be written as {} because JDK versions write it "
                                     "differently.".format(_java_number_string(value, single), literal_type))
    if value is None or root == casts.BOOLEAN or root in _INTEGER_BITS or root in (casts.FLOAT, casts.DOUBLE):
        return value
    if root in casts.CHARACTER_STRING:
        return _java_string(value)
    if root in (casts.DATE, casts.TIME):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise RuntimeError("{!r} cannot be cast to a Number.".format(value))
        number = _java_integer(value, 32)
        if root == casts.DATE:
            return _java_date(*_civil_from_days(number))
        if not 0 <= number < 86400000:
            raise RuntimeError("Invalid time of day {}.".format(number))
        return _java_time(number // 3600000, number // 60000 % 60, number // 1000 % 60, number % 1000 * 10 ** 6)
    if root == casts.DECIMAL and isinstance(value, _Decimal):
        return _java_string(value)
    if root in (casts.TIMESTAMP, casts.TIMESTAMP_LTZ) and isinstance(value, _Timestamp):
        return _java_local_date_time(value) if root == casts.TIMESTAMP else _java_instant(value)
    raise RuntimeError("{!r} cannot be written as {}.".format(value, literal_type))


def _literal_type(function: str, output: DataType, error: type = ValueError) -> DataType:
    if function not in _ARRAY_FUNCTIONS:
        return output
    if not isinstance(output, ArrayType):
        raise error("{} needs an ARRAY field.".format(function))
    return output.element


def _leaf_literals(leaf: Dict[str, Any], output: DataType) -> Optional[List[Any]]:
    function, literals = leaf.get("function"), leaf.get("literals")
    if function not in _LEAF_FUNCTIONS or not isinstance(literals, (list, type(None))):
        raise ValueError("Unknown leaf function {} or literals {}.".format(function, literals))
    literal_type = _literal_type(function, output)
    return None if literals is None else [_internal_literal(literal_type, literal) for literal in literals]


def _ref_type(ref: Dict[str, Any], columns: Optional[Dict[str, DataType]]) -> DataType:
    return _declared_type(ref["type"]) if columns is None else columns[ref["name"]]


def _input_fits(kind: str, value: Any, columns: Optional[Dict[str, DataType]]) -> bool:
    if value is None:
        return True
    if isinstance(value, dict):
        return kind in "ra" or _type_root(_ref_type(value, columns)) in _INPUT_ROOTS[kind]
    if isinstance(value, str):
        return kind in "sra"
    if isinstance(value, bool) or not isinstance(value, int):
        return False
    return kind in "ia" or (kind == "d" and -2 ** 31 <= value < 2 ** 31)


def _transform_type(transform: Dict[str, Any],
                    columns: Optional[Dict[str, DataType]] = None) -> Optional[DataType]:
    name = transform["name"]
    if "fieldRef" in transform:
        source = _ref_type(transform["fieldRef"], columns)
        if name == "CAST":
            target = _declared_type(transform["type"])
            return target if columns is not None or _castable(source, target) else None
        if name in _DATE_PARTS:
            return AtomicType("INT") if _type_root(source) in (casts.DATE, casts.TIMESTAMP) else None
        for part in transform.get("path") or []:
            source = next((field.type for field in getattr(source, "fields", ()) if field.name == part), None)
        nested = name == "NESTED_FIELD_REF" and transform.get("path")
        return source if name == "FIELD_REF" or nested else None
    if name == "NULL":
        return AtomicType("BOOLEAN")
    if name in _TRANSFORM_FLAGS:
        flag, values = _TRANSFORM_FLAGS[name]
        if transform.get(flag) not in values:
            return None
    kinds, low, high, output = _TRANSFORM_INPUTS.get(name, ("", 1, 0, None))
    inputs = transform.get("inputs")
    if not isinstance(inputs, list) or not low <= len(inputs) <= (len(inputs) if high is None else high):
        return None
    fits = all(_input_fits(kinds[min(position, len(kinds) - 1)], value, columns)
               for position, value in enumerate(inputs))
    return AtomicType(output) if fits else None


def _policy_type_error(policy: DataPolicy, schema: TableSchema) -> Optional[str]:
    columns = {field.name: field.type for field in schema.fields}
    output = None
    for transform, leaf in _policy_transforms(policy):
        declared_types = _declared_types(transform)
        declared = _transform_type(transform)
        output = _transform_type(transform, columns)
        if declared is None or output is None:
            return "Policy transform {} does not fit the table schema.".format(transform["name"])
        if transform["name"] == "NESTED_FIELD_REF":
            name, path = transform["fieldRef"]["name"], transform["path"]
            if _path_ids(declared_types[name], path) != _path_ids(columns[name], path):
                return "Nested field {}.{} changed identity.".format(name, ".".join(path))
        try:
            if leaf is not None:
                _leaf_literals(leaf, declared)
        except ValueError as e:
            return "Row filter function {} does not accept {}: {}".format(
                leaf.get("function"), leaf.get("literals"), e)
    mask = policy.get_column_mask()
    if mask is not None and _logical_type(output) != _logical_type(columns[mask.get_on_column()]):
        return "Column mask output type {} does not match target column {} type {}.".format(
            output, mask.get_on_column(), columns[mask.get_on_column()])
    return None


def _java_spelling(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _java_spelling(item) for key, item in value.items() if key != "nullable" and item is not None}
    if isinstance(value, list):
        return [_java_spelling(item) for item in value]
    if isinstance(value, str):
        return re.sub(r"^TIMESTAMP_LTZ\(([0-9]+)\)", r"TIMESTAMP(\1) WITH LOCAL TIME ZONE", value)
    return value


_TYPE_RANGES = {casts.CHAR: ((1, 2147483647),), casts.VARCHAR: ((1, 2147483647),),
                casts.BINARY: ((1, 2147483647),), casts.VARBINARY: ((1, 2147483647),), casts.DECIMAL: ((1, 38),),
                casts.TIME: ((0, 9),), casts.TIMESTAMP: ((0, 9),), casts.TIMESTAMP_LTZ: ((0, 9),)}


def _valid_type(data_type: DataType) -> bool:
    if not isinstance(data_type, AtomicType):
        children = [field.type for field in getattr(data_type, "fields", ())] + [
            getattr(data_type, name) for name in ("element", "key", "value") if hasattr(data_type, name)]
        return all(map(_valid_type, children))
    root, parameters, _ = _logical_type(data_type)
    ranges = _TYPE_RANGES.get(root, ())
    if root == casts.DECIMAL and len(parameters) == 2 and not 0 <= parameters[1] <= parameters[0]:
        return False
    return all(low <= value <= high for value, (low, high) in zip(parameters, ranges))


def _declared_type(value: Any) -> DataType:
    data_type = DataTypeParser.parse_data_type(value)
    if _java_spelling(value) != _java_type(data_type) or not _valid_type(data_type):
        raise ValueError("Policy type {} is not written as Java writes it.".format(json.dumps(value)))
    return data_type


def _declared_types(transform: Dict[str, Any]) -> Dict[str, DataType]:
    return {ref["name"]: _declared_type(ref["type"]) for ref in _field_refs(transform)}


def _canonical_transform(transform: Dict[str, Any], schema: TableSchema) -> Dict[str, Any]:
    names = [field.name for field in schema.fields]

    def remap(value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        index = names.index(value["name"])
        return {"index": index, "name": value["name"], "type": _java_type(schema.fields[index].type)}

    canonical = {"name": transform["name"]}
    if "fieldRef" in transform:
        canonical["fieldRef"] = remap(transform["fieldRef"])
        if transform["name"] == "NESTED_FIELD_REF":
            canonical["path"] = transform["path"]
        if transform["name"] == "CAST":
            canonical["type"] = _java_type(_declared_type(transform["type"]))
    elif transform["name"] != "NULL":
        canonical["inputs"] = [remap(value) for value in transform.get("inputs") or []]
    if transform["name"] in _TRANSFORM_FLAGS:
        flag = _TRANSFORM_FLAGS[transform["name"]][0]
        canonical[flag] = transform[flag]
    return canonical


def _canonical_predicate(predicate: Dict[str, Any], schema: TableSchema) -> Dict[str, Any]:
    if predicate["kind"] == "COMPOUND":
        return {"kind": "COMPOUND", "function": predicate["function"],
                "children": [_canonical_predicate(child, schema) for child in predicate["children"]]}
    transform = predicate["transform"]
    literals = _leaf_literals(predicate, _transform_type(transform))
    output = _transform_type(transform, {field.name: field.type for field in schema.fields})
    literal_type = _literal_type(predicate["function"], output, RuntimeError)
    return {"kind": "LEAF", "transform": _canonical_transform(transform, schema), "function": predicate["function"],
            "literals": None if literals is None else [_json_literal(literal_type, value) for value in literals]}


def _canonical_policy(policy: DataPolicy, schema: TableSchema) -> DataPolicy:
    mask = policy.get_column_mask()
    if mask is None:
        predicate = _canonical_predicate(_policy_json(policy.get_row_filter().get_predicate()), schema)
        return DataPolicy.row_filter(policy.get_resource(), RowFilter(_flat_json(predicate)), policy.get_principal())
    transform = _canonical_transform(_policy_json(mask.get_transform()), schema)
    return DataPolicy.column_mask(policy.get_resource(), ColumnMask(mask.get_on_column(), _flat_json(transform)),
                                  policy.get_principal())


def _flat_json(value: Any) -> str:
    if isinstance(value, _Binary):
        data = value.text.encode("utf-8")
        encoded = base64.b64encode(data).decode("ascii")
        value = {"segments": [{"heapMemory": encoded, "array": encoded, "offHeap": False}],
                 "offset": 0, "sizeInBytes": len(data)}
    elif isinstance(value, _Decimal):
        value = {"compact": value.precision <= 18}
    elif isinstance(value, _Timestamp):
        value = {"millisecond": value.millisecond, "nanoOfMillisecond": value.nano_of_millisecond}
    if isinstance(value, dict):
        return "{" + ",".join(_flat_json(key) + ":" + _flat_json(item) for key, item in value.items()) + "}"
    if isinstance(value, list):
        return "[" + ",".join(map(_flat_json, value)) + "]"
    if isinstance(value, float):
        text = _java_number_string(value, isinstance(value, _JavaFloat))
        return json.dumps(text) if math.isnan(value) or math.isinf(value) else text
    return json.dumps(value, ensure_ascii=False)


_JAVA_TYPE_FORMATS = {
    casts.CHAR: "CHAR({})", casts.VARCHAR: "VARCHAR({})", casts.BINARY: "BINARY({})",
    casts.VARBINARY: "VARBINARY({})", casts.INTEGER: "INT", casts.DECIMAL: "DECIMAL({}, {})",
    casts.TIME: "TIME({})", casts.TIMESTAMP: "TIMESTAMP({})", casts.TIMESTAMP_LTZ: "TIMESTAMP({}) WITH LOCAL TIME ZONE"}


def _java_type(data_type: DataType) -> Any:
    suffix = "" if data_type.nullable else " NOT NULL"
    if isinstance(data_type, AtomicType):
        root, parameters, _ = _logical_type(data_type)
        if parameters == (2147483647,) and root in (casts.VARCHAR, casts.VARBINARY):
            return ("STRING" if root == casts.VARCHAR else "BYTES") + suffix
        return _JAVA_TYPE_FORMATS.get(root, root).format(*parameters) + suffix
    if isinstance(data_type, RowType):
        return {"type": "ROW" + suffix, "fields": [_java_field(field) for field in data_type.fields]}
    if isinstance(data_type, MapType):
        return {"type": "MAP" + suffix, "key": _java_type(data_type.key), "value": _java_type(data_type.value)}
    name = {ArrayType: "ARRAY", MultisetType: "MULTISET", VectorType: "VECTOR"}[type(data_type)]
    java = {"type": name + suffix, "element": _java_type(data_type.element)}
    if isinstance(data_type, VectorType):
        java["length"] = data_type.length
    return java


def _java_field(field: DataField) -> Dict[str, Any]:
    java = {"id": field.id, "name": field.name, "type": _java_type(field.type)}
    if field.description is not None:
        java["description"] = field.description
    if field.default_value is not None:
        java["defaultValue"] = field.default_value
    return java


def _path_ids(data_type: Optional[DataType], path: List[str]) -> Optional[List[int]]:
    ids = []
    for part in path:
        field = next((field for field in getattr(data_type, "fields", ()) if field.name == part), None)
        if field is None:
            return None
        ids.append(field.id)
        data_type = field.type
    return ids


def _moved_columns(policy: DataPolicy, previous: TableSchema, current: TableSchema) -> List[str]:
    previous_ids = {field.name: field.id for field in previous.fields}
    current_ids = {field.name: field.id for field in current.fields}
    return sorted(column for column in _policy_columns(policy) if current_ids.get(column) != previous_ids.get(column))


def _granted_columns(assignment: PermissionAssignment) -> List[str]:
    columns = assignment.get_columns()
    names = columns.get_column_names()
    return list(names if names is not None else columns.get_excluded_column_names())


def _expired(assignment: PermissionAssignment) -> bool:
    expire_time = assignment.get_expire_time()
    if expire_time is None:
        return False
    sign, digits, month, day, hour, minute, second, fraction = _INSTANT.fullmatch(expire_time).groups()
    hour, minute, second = int(hour), int(minute), int(second)
    days = _days_from_civil(-int(digits) if sign == "-" else int(digits), int(month), int(day))
    if hour == 24:
        days, hour = days + 1, 0
    second = min(second, 59)
    epoch_millis = ((days * 86400 + hour * 3600 + minute * 60 + second) * 1000
                    + int((fraction or "").ljust(9, "0")) // 1000000)
    return epoch_millis <= int(time.time() * 1000)


def _renamed_columns(previous: TableSchema, current: TableSchema) -> Dict[str, Optional[str]]:
    current_names = {field.id: field.name for field in current.fields}
    return {field.name: current_names.get(field.id) for field in previous.fields}


def _query_auth_enabled(schema: TableSchema) -> bool:
    return CoreOptions.from_dict(schema.options).query_auth_enabled


def _on_table(resource: PermissionResource, identifier: Identifier) -> bool:
    return resource.get_type() in (ResourceType.TABLE, ResourceType.COLUMN) \
        and resource.get_database() == identifier.get_database_name() \
        and resource.get_table() == identifier.get_table_name()


def _matches(parameters: Dict[str, str], name: str, value: Optional[str]) -> bool:
    return name not in parameters or parameters[name] == value


def _matches_policy(policy: DataPolicy, parameters: Dict[str, str]) -> bool:
    mask = policy.get_column_mask()
    if "column" in parameters and (mask is None or mask.get_on_column() != parameters["column"]):
        return False
    return _matches(parameters, "type", policy.type().name) \
        and _matches(parameters, "principal", policy.get_principal())


class RESTCatalogServer:
    """Mock REST server for testing"""

    def __init__(self, data_path: str, auth_provider, config: ConfigResponse, warehouse: str,
                 role_name: str = None, token_json: str = None):
        self.logger = logging.getLogger(__name__)
        self.warehouse = warehouse
        self.config_response = config

        # Initialize resource paths
        prefix = config.defaults.get("prefix")
        self.resource_paths = ResourcePaths(prefix=prefix)
        self.database_uri = self.resource_paths.databases()

        # Initialize storage
        self.database_store: Dict[str, GetDatabaseResponse] = {}
        self.table_metadata_store: Dict[str, TableMetadata] = {}
        self.table_latest_snapshot_store: Dict[str, str] = {}
        self.table_partitions_store: Dict[str, List] = {}
        self.function_store: Dict[str, Dict] = {}  # key: "db.func_name", value: GetFunctionResponse-like dict
        # Tag store: key = full table name, value = {tag_name: GetTagResponse}.
        self.tag_store: Dict[str, Dict[str, GetTagResponse]] = {}
        # Branch store: key = full table name, value = set of branch names.
        self.branch_store: Dict[str, set] = {}
        self.no_permission_databases: List[str] = []
        self.no_permission_tables: List[str] = []
        self.table_token_store: Dict[str, "RESTToken"] = {}
        self.table_query_auth_store = {}
        self.permission_store: Dict[str, PermissionAssignment] = {}
        self.policy_store: Dict[str, Dict[str, DataPolicy]] = {}
        self.table_identifiers: Dict[str, Identifier] = {}
        self.query_principals: set = set()

        # Initialize mock catalog (simplified)
        self.data_path = data_path
        self.auth_provider = auth_provider
        self.role_name = role_name
        self.token_json = token_json

        # HTTP server setup
        self.server = None
        self.server_thread = None
        self.port = 0

    def start(self) -> None:
        """Start the mock server"""
        handler = self._create_request_handler()
        self.server = HTTPServer(('localhost', 0), handler)
        self.port = self.server.server_port

        self.server_thread = threading.Thread(target=self.server.serve_forever)
        self.server_thread.daemon = True
        self.server_thread.start()

        self.logger.info(f"Mock REST server started on port {self.port}")

    def get_url(self) -> str:
        """Get server URL"""
        return f"http://localhost:{self.port}"

    def shutdown(self) -> None:
        """Shutdown the server"""
        if self.server:
            self.server.shutdown()
            self.server.server_close()
        if self.server_thread:
            self.server_thread.join()

    def _create_request_handler(self):
        """Create HTTP request handler"""
        server_instance = self

        class RequestHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                self._handle_request('GET')

            def do_POST(self):
                self._handle_request('POST')

            def do_DELETE(self):
                self._handle_request('DELETE')

            def _handle_request(self, method: str):
                try:
                    # Parse request
                    parsed_url = urlparse(self.path)
                    resource_path = parsed_url.path
                    parameters = self._parse_query_params(parsed_url.query)

                    # Get request body
                    content_length = int(self.headers.get('Content-Length', 0))
                    data = self.rfile.read(content_length).decode('utf-8') if content_length > 0 else ""

                    # Get headers (case-insensitive from HTTPMessage)
                    auth_token = self.headers.get(AUTHORIZATION_HEADER_KEY)
                    headers = dict(self.headers)

                    # Handle authentication
                    if not self._authenticate(auth_token, resource_path, parameters, method, data):
                        self._send_response(401, "Unauthorized")
                        return

                    # Route request
                    response, status_code = server_instance._route_request(
                        method, resource_path, parameters, data, headers
                    )

                    self._send_response(status_code, response)

                except Exception as e:
                    server_instance.logger.error(f"Request handling error: {e}")
                    self._send_response(500, str(e))

            def _parse_query_params(self, query: str) -> Dict[str, str]:
                """Parse query parameters"""
                if not query:
                    return {}

                params = {}
                for pair in query.split('&'):
                    if '=' in pair:
                        key, value = pair.split('=', 1)
                        params[key.strip()] = RESTUtil.decode_string(value.strip())
                return params

            def _authenticate(self, token: str, path: str, params: Dict[str, str],
                              method: str, data: str) -> bool:
                """Authenticate request by verifying Authorization header."""
                if server_instance.auth_provider is None:
                    return True
                if path.startswith("/ram/security-credential"):
                    return True
                if not token:
                    return False
                rest_auth_parameter = RESTAuthParameter(
                    method=method,
                    path=path,
                    data=data or "",
                    parameters=params or {},
                )
                from pypaimon.api.auth.base import RESTAuthFunction
                auth_fn = RESTAuthFunction({}, server_instance.auth_provider)
                expected_headers = auth_fn(rest_auth_parameter)
                expected_token = expected_headers.get(
                    AUTHORIZATION_HEADER_KEY, "")
                return token == expected_token

            def _send_response(self, status_code: int, body: str):
                """Send HTTP response"""
                self.send_response(status_code)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                self.wfile.write(body.encode('utf-8'))

            def log_message(self, format, *args):
                """Override to use our logger"""
                server_instance.logger.debug(format % args)

        return RequestHandler

    def _route_request(self, method: str, resource_path: str, parameters: Dict[str, str],
                       data: str, headers: Dict[str, str]) -> Tuple[str, int]:
        """Route HTTP request to appropriate handler"""
        try:
            # Config endpoint
            # Config endpoint
            if resource_path.startswith(self.resource_paths.config()):
                warehouse_param = parameters.get(WAREHOUSE)
                if warehouse_param == self.warehouse:
                    return self._mock_response(self.config_response, 200)

            # ecs role
            if resource_path == '/ram/security-credential/':
                return self._mock_response(self.role_name, 200)

            if resource_path == f'/ram/security-credential/{self.role_name}':
                return self._mock_response(self.token_json, 200)

            # Databases endpoint
            if resource_path == self.resource_paths.databases() or resource_path.startswith(self.database_uri + "?"):
                return self._databases_api_handler(method, data, parameters)

            if resource_path == self.resource_paths.rename_table():
                rename_request = JSON.from_json(data, RenameTableRequest)
                source_table = rename_request.source
                destination_table = rename_request.destination
                source_name = source_table.get_full_name()
                destination_name = destination_table.get_full_name()
                if source_name not in self.table_metadata_store:
                    raise TableNotExistException(source_table)
                if destination_name in self.table_metadata_store:
                    raise TableAlreadyExistException(destination_table)
                source_table_dir = (Path(self.data_path) / self.warehouse
                                    / source_table.get_database_name() / source_table.get_object_name())
                destination_table_dir = (Path(self.data_path) / self.warehouse
                                         / destination_table.get_database_name() / destination_table.get_object_name())
                if not source_table_dir.exists():
                    destination_table_dir.mkdir(parents=True)
                else:
                    destination_table_dir.parent.mkdir(parents=True, exist_ok=True)
                    source_table_dir.rename(destination_table_dir)
                for store in (self.table_metadata_store, self.branch_store, self.tag_store,
                              self.table_latest_snapshot_store, self.table_partitions_store):
                    if source_name in store:
                        store[destination_name] = store.pop(source_name)
                self._rename_table_access(source_table, destination_table)
                return self._mock_response("", 200)

            # Global functions endpoint (catalog-scoped)
            if resource_path == self.resource_paths.functions() and method == "GET":
                return self._functions_globally_handle(parameters)

            if resource_path in (self.resource_paths.permissions(),
                                 self.resource_paths.grant_permission(),
                                 self.resource_paths.revoke_permission()):
                return self._permissions_handle(method, resource_path, data, parameters)

            database = resource_path.split("/")[4]
            # Database-specific endpoints
            if resource_path.startswith(self.resource_paths.database(database)):
                """Handle database-specific resource requests"""
                # Extract database name and resource path
                path_parts = resource_path[len(self.database_uri) + 1:].split('/')
                database_name = RESTUtil.decode_string(path_parts[0])

                if len(path_parts) in (4, 5) and path_parts[1] == ResourcePaths.TABLES \
                        and path_parts[3] == ResourcePaths.POLICIES and path_parts[4:] in ([], ['drop']):
                    return self._policies_handle(
                        method, data, Identifier.create(database_name, RESTUtil.decode_string(path_parts[2])),
                        parameters, len(path_parts) == 5)

                # Check database permissions
                if database_name in self.no_permission_databases:
                    raise DatabaseNoPermissionException(database_name)

                if database_name not in self.database_store:
                    raise DatabaseNotExistException(database_name)

                # Handle different resource types
                if len(path_parts) == 1:
                    # Database operations
                    return self._database_handle(method, data, database_name)

                elif len(path_parts) == 2:
                    # Collection operations (tables, views, functions)
                    resource_type = path_parts[1]

                    if resource_type.startswith(ResourcePaths.TABLES):
                        return self._tables_handle(method, data, database_name, parameters)
                    elif resource_type == ResourcePaths.FUNCTIONS:
                        return self._functions_handle(method, data, database_name, parameters)
                    elif resource_type == ResourcePaths.FUNCTION_DETAILS:
                        return self._function_details_handle(database_name, parameters)

                elif len(path_parts) >= 3:
                    # Individual resource operations
                    resource_type = path_parts[1]
                    resource_name = RESTUtil.decode_string(path_parts[2])
                    identifier = Identifier.create(database_name, resource_name)

                    if resource_type == ResourcePaths.TABLES:
                        return self._handle_table_resource(method, path_parts, identifier, data, parameters)
                    elif resource_type == ResourcePaths.PARTITIONS:
                        return self._table_partitions_handle(method, data, identifier, parameters)
                    elif resource_type == ResourcePaths.FUNCTIONS:
                        return self._function_handle(method, data, identifier)

                return self._mock_response(ErrorResponse(None, None, "Not Found", 404), 404)

            return self._mock_response(ErrorResponse(None, None, "Not Found", 404), 404)

        except DatabaseNotExistException as e:
            response = ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_DATABASE, e.database, str(e), 404
            )
            return self._mock_response(response, 404)
        except TableNotExistException as e:
            response = ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_TABLE, e.identifier.get_table_name(), str(e), 404
            )
            return self._mock_response(response, 404)
        except DatabaseNoPermissionException as e:
            response = ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_DATABASE, e.database, str(e), 403
            )
            return self._mock_response(response, 403)
        except TableNoPermissionException as e:
            response = ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_TABLE, e.identifier.get_table_name(), str(e), 403
            )
            return self._mock_response(response, 403)
        except DatabaseAlreadyExistException as e:
            response = ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_DATABASE, e.database, str(e), 409
            )
            return self._mock_response(response, 409)
        except TableAlreadyExistException as e:
            response = ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_TABLE, e.identifier.get_full_name(), str(e), 409
            )
            return self._mock_response(response, 409)
        except FunctionNotExistException as e:
            response = ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_FUNCTION, e.identifier.get_object_name(), str(e), 404
            )
            return self._mock_response(response, 404)
        except FunctionAlreadyExistException as e:
            response = ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_FUNCTION, e.identifier.get_full_name(), str(e), 409
            )
            return self._mock_response(response, 409)
        except TagNotExistException as e:
            response = ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_TAG, e.tag, str(e), 404
            )
            return self._mock_response(response, 404)
        except TagAlreadyExistException as e:
            response = ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_TAG, e.tag, str(e), 409
            )
            return self._mock_response(response, 409)
        except BranchNotExistException as e:
            response = ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_BRANCH, e.branch, str(e), 404
            )
            return self._mock_response(response, 404)
        except BranchAlreadyExistException as e:
            response = ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_BRANCH, e.branch, str(e), 409
            )
            return self._mock_response(response, 409)
        except DefinitionAlreadyExistException as e:
            response = ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_DEFINITION, e.name, str(e), 409
            )
            return self._mock_response(response, 409)
        except DefinitionNotExistException as e:
            response = ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_DEFINITION, e.name, str(e), 404
            )
            return self._mock_response(response, 404)
        except ValueError as e:
            response = ErrorResponse(None, None, str(e), 400)
            return self._mock_response(response, 400)
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
            response = ErrorResponse(None, None, str(e), 500)
            return self._mock_response(response, 500)

    def _handle_table_resource(self, method: str, path_parts: List[str],
                               identifier: Identifier, data: str,
                               parameters: Dict[str, str]) -> Tuple[str, int]:
        """Handle table-specific resource requests."""
        # The branch (if any) is encoded into the object name as
        # "$branch_<name>" — see Identifier. Strip it for table lookup, and
        # surface the branch separately for routes that care (e.g. commit).
        branch_part = identifier.get_branch_name()
        if branch_part is not None:
            lookup_identifier = Identifier.create(
                identifier.get_database_name(), identifier.get_table_name())
        else:
            lookup_identifier = identifier

        # Check table permissions using the base identifier
        if lookup_identifier.get_full_name() in self.no_permission_tables:
            raise TableNoPermissionException(lookup_identifier)

        if len(path_parts) == 3:
            # Basic table operations (GET, DELETE, etc.)
            return self._table_handle(
                method, data, lookup_identifier, response_identifier=identifier)
        elif len(path_parts) == 4:
            # Extended operations (e.g., commit, token, snapshot)
            operation = path_parts[3]
            if operation == "commit":
                return self._table_commit_handle(method, data, lookup_identifier, branch_part)
            elif operation == "token":
                return self._table_token_handle(method, lookup_identifier)
            elif operation == "auth":
                if branch_part is not None:
                    self._branch_table_metadata(lookup_identifier, branch_part)
                return self._table_query_auth_handle(method, data, lookup_identifier)
            elif operation == "rollback":
                return self._table_rollback_handle(method, data, lookup_identifier)
            elif operation == "snapshot":
                return self._table_snapshot_handle(
                    method, lookup_identifier, branch_part)
            elif operation == ResourcePaths.PARTITIONS:
                return self._table_partitions_handle(method, data, lookup_identifier, parameters)
            elif operation == ResourcePaths.TAGS:
                return self._tags_handle(method, data, lookup_identifier, parameters)
            elif operation == ResourcePaths.BRANCHES:
                return self._branches_handle(method, data, lookup_identifier)
            else:
                return self._mock_response(ErrorResponse(None, None, "Not Found", 404), 404)
        elif len(path_parts) == 5 and path_parts[3] == ResourcePaths.TAGS:
            tag_name = RESTUtil.decode_string(path_parts[4])
            return self._tag_handle(method, lookup_identifier, tag_name)
        elif len(path_parts) == 5 and path_parts[3] == ResourcePaths.BRANCHES:
            branch_name = RESTUtil.decode_string(path_parts[4])
            return self._branch_handle(method, lookup_identifier, branch_name)
        elif len(path_parts) == 6 and path_parts[3] == ResourcePaths.BRANCHES:
            branch_name = RESTUtil.decode_string(path_parts[4])
            sub = path_parts[5]
            if sub == ResourcePaths.FORWARD:
                return self._branch_forward_handle(method, lookup_identifier, branch_name)
            return self._mock_response(ErrorResponse(None, None, "Not Found", 404), 404)
        return self._mock_response(ErrorResponse(None, None, "Not Found", 404), 404)

    def set_query_principals(self, *principals: str) -> None:
        self.query_principals = set(principals)

    def _permissions_handle(self, method: str, resource_path: str, data: str,
                            parameters: Dict[str, str]) -> Tuple[str, int]:
        if method == "GET" and resource_path == self.resource_paths.permissions():
            target = PermissionResource(
                parameters.get("resourceType"), parameters.get("database"),
                parameters.get("table"), parameters.get("function"), parameters.get("view"))
            missing = self._missing_permission_resource(target)
            if missing is not None:
                return missing
            matched = [assignment for assignment in self.permission_store.values()
                       if assignment.get_resource() == target
                       and _matches(parameters, "principal", assignment.get_principal())
                       and _matches(parameters, "access", assignment.get_access())]
            return self._mock_response(ListPermissionsResponse(
                *self._management_page({_assignment_key(grant): grant for grant in matched}, parameters)), 200)
        if method == "POST" and resource_path == self.resource_paths.grant_permission():
            assignment = JSON.from_json(data, GrantPermissionRequest).assignment()
            missing = self._missing_permission_resource(assignment.get_resource())
            if missing is not None:
                return missing
            invalid = self._invalid_column_grant(assignment)
            if invalid is not None:
                return invalid
            self.permission_store[_assignment_key(assignment)] = assignment
            return self._mock_response("", 200)
        if method == "POST" and resource_path == self.resource_paths.revoke_permission():
            request = JSON.from_json(data, RevokePermissionRequest)
            missing = self._missing_permission_resource(request.get_resource())
            if missing is not None:
                return missing
            self.permission_store.pop(_permission_key(
                request.get_resource(), request.get_access(), request.get_principal()), None)
            return self._mock_response("", 200)
        return self._mock_response(ErrorResponse(None, None, "Not Found", 404), 404)

    def _missing_permission_resource(
            self, resource: PermissionResource) -> Optional[Tuple[str, int]]:
        resource_type = resource.get_type()
        database = resource.get_database()
        if resource_type in (ResourceType.CATALOG, ResourceType.CATALOG_ALL):
            return None
        exists = database in self.database_store
        if resource_type in (ResourceType.TABLE, ResourceType.COLUMN):
            exists = exists and Identifier.create(
                database, resource.get_table()).get_full_name() in self.table_metadata_store
        elif resource_type == ResourceType.FUNCTION:
            exists = exists and "{}.{}".format(
                database, resource.get_function()) in self.function_store
        elif resource_type == ResourceType.VIEW:
            exists = False
        if exists:
            return None
        return self._mock_response(ErrorResponse(
            resource_type.name, _resource_name(resource),
            "Permission resource does not exist.", 404), 404)

    def _invalid_column_grant(self, assignment: PermissionAssignment) -> Optional[Tuple[str, int]]:
        resource = assignment.get_resource()
        if resource.get_type() != ResourceType.COLUMN:
            return None
        identifier = Identifier.create(resource.get_database(), resource.get_table())
        schema = self.table_metadata_store[identifier.get_full_name()].schema
        if self.branch_store.get(identifier.get_full_name()):
            return self._mock_response(ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_TABLE, _resource_name(resource),
                "Column permissions are not supported on tables with branches.", 400), 400)
        if not _query_auth_enabled(schema):
            return self._mock_response(ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_TABLE, _resource_name(resource),
                "Column permissions require query-auth.enabled=true.", 409), 409)
        names = {field.name for field in schema.fields}
        for column in _granted_columns(assignment):
            if column not in names:
                return self._mock_response(ErrorResponse(
                    ErrorResponse.RESOURCE_TYPE_COLUMN, column,
                    "Permission column does not exist.", 404), 404)
        return None

    def _restricted(self, identifier: Identifier, metadata: TableMetadata) -> bool:
        return bool(self.policy_store.get(metadata.uuid or identifier.get_full_name())
                    or self._column_grants(identifier))

    def _column_grants(self, identifier: Identifier) -> List[PermissionAssignment]:
        return [assignment for assignment in self.permission_store.values()
                if assignment.get_resource().get_type() == ResourceType.COLUMN
                and _on_table(assignment.get_resource(), identifier)]

    def _policies_handle(self, method: str, data: str, identifier: Identifier,
                         parameters: Dict[str, str], drop: bool) -> Tuple[str, int]:
        resource = PermissionResource(
            ResourceType.TABLE, identifier.get_database_name(), identifier.get_object_name())
        metadata = self.table_metadata_store.get(identifier.get_full_name())
        if metadata is None:
            return self._missing_permission_resource(resource)
        policies = self._table_policies(identifier, metadata)
        if method == "GET" and not drop:
            matched = [DataPolicy(resource, policy.get_row_filter(), policy.get_column_mask(),
                                  policy.get_principal())
                       for policy in policies.values() if _matches_policy(policy, parameters)]
            return self._mock_response(ListPoliciesResponse(
                *self._management_page({_policy_key(policy): policy for policy in matched}, parameters)), 200)
        if method == "POST" and not drop:
            policy = JSON.from_json(data, PolicyRequest).policy(resource)
            if self.branch_store.get(identifier.get_full_name()):
                return self._mock_response(ErrorResponse(
                    ErrorResponse.RESOURCE_TYPE_TABLE, identifier.get_full_name(),
                    "Data policies are not supported on tables with branches.", 400), 400)
            if not _query_auth_enabled(metadata.schema):
                return self._mock_response(ErrorResponse(
                    ErrorResponse.RESOURCE_TYPE_TABLE, identifier.get_full_name(),
                    "Data policies require the target table option query-auth.enabled=true.",
                    409), 409)
            identity = _policy_identity(policy)
            try:
                missing = sorted(_policy_columns(policy) - {field.name for field in metadata.schema.fields})
                error = "Policy column {} does not exist in table {}.".format(
                    missing[0], identifier.get_full_name()) if missing else _policy_type_error(policy, metadata.schema)
            except Exception as e:
                error = "Invalid policy: {}".format(e)
            if error:
                return self._mock_response(ErrorResponse(
                    ErrorResponse.RESOURCE_TYPE_POLICY, _policy_name(*identity), error, 400), 400)
            try:
                canonical = _canonical_policy(policy, metadata.schema)
            except _UnsupportedConversion as e:
                return self._mock_response(ErrorResponse(
                    ErrorResponse.RESOURCE_TYPE_POLICY, _policy_name(*identity), str(e), 400), 400)
            if _cursor_key(*identity) in policies:
                return self._mock_response(ErrorResponse(
                    ErrorResponse.RESOURCE_TYPE_POLICY, _policy_name(*identity),
                    "Policy already exists.", 409), 409)
            policies[_cursor_key(*identity)] = canonical
            return self._mock_response("", 200)
        if method == "POST" and drop:
            request = JSON.from_json(data, DropPolicyRequest)
            identity = (request.get_type().name, request.get_principal(), request.get_column())
            if policies.pop(_cursor_key(*identity), None) is None:
                return self._mock_response(ErrorResponse(
                    ErrorResponse.RESOURCE_TYPE_POLICY, _policy_name(*identity),
                    "Policy does not exist.", 404), 404)
            return self._mock_response("", 200)
        return self._mock_response(ErrorResponse(None, None, "Not Found", 404), 404)

    def _remove_permissions(self, matches) -> None:
        for key in [key for key, assignment in self.permission_store.items()
                    if matches(assignment.get_resource())]:
            del self.permission_store[key]

    def _drop_table_access(self, identifier: Identifier, metadata: TableMetadata) -> None:
        for branch in self.branch_store.pop(identifier.get_full_name(), ()):
            manager = self._schema_manager(identifier, branch)
            manager.file_io.delete(manager.branch_path, recursive=True)
        if not metadata.is_external:
            manager = self._schema_manager(identifier)
            manager.file_io.delete(manager.table_path, recursive=True)
        self.policy_store.pop(metadata.uuid or identifier.get_full_name(), None)
        self.table_identifiers.pop(identifier.get_full_name(), None)
        self._remove_permissions(lambda resource: _on_table(resource, identifier))

    def _table_identifier(self, name: str) -> Identifier:
        return self.table_identifiers.get(name) or Identifier.from_string(name)

    def _rename_table_access(self, source: Identifier, destination: Identifier) -> None:
        self.table_identifiers.pop(source.get_full_name(), None)
        self.table_identifiers[destination.get_full_name()] = destination
        moved = [assignment for assignment in self.permission_store.values()
                 if _on_table(assignment.get_resource(), source)]
        self._remove_permissions(lambda resource: _on_table(resource, source))
        for assignment in moved:
            renamed = PermissionAssignment(
                PermissionResource(assignment.get_resource().get_type(),
                                   destination.get_database_name(), destination.get_table_name()),
                assignment.get_access(), assignment.get_principal(),
                assignment.get_columns(), assignment.get_expire_time())
            self.permission_store[_assignment_key(renamed)] = renamed
        metadata = self.table_metadata_store[destination.get_full_name()]
        source_key = metadata.uuid or source.get_full_name()
        if source_key in self.policy_store:
            policies = self.policy_store.pop(source_key)
            self._table_policies(destination, metadata).update(policies)

    def _table_policies(self, identifier: Identifier,
                        metadata: TableMetadata) -> Dict[str, DataPolicy]:
        return self.policy_store.setdefault(metadata.uuid or identifier.get_full_name(), {})

    def _check_column_grants(self, identifier: Identifier, select: Optional[List[str]]) -> None:
        fields = [field.name for field in self.table_metadata_store[identifier.get_full_name()].schema.fields]
        grants = [grant for grant in self._column_grants(identifier)
                  if grant.get_principal() in self.query_principals and not _expired(grant)]
        if grants:
            included = set(fields)
            for grant in grants:
                columns = set(_granted_columns(grant))
                if grant.get_columns().get_column_names() is not None:
                    included &= columns
                else:
                    included -= columns
            if any(column not in included for column in (fields if select is None else select)):
                raise TableNoPermissionException(identifier)

    def _policy_rules(self, identifier: Identifier) -> Tuple[Optional[List[str]], Optional[Dict[str, str]]]:
        metadata = self.table_metadata_store[identifier.get_full_name()]
        policies = self._table_policies(identifier, metadata)
        names = {field.name for field in metadata.schema.fields}
        applied, masked = [], set()
        for key in sorted(policies):
            policy = policies[key]
            if policy.get_principal() not in self.query_principals:
                continue
            try:
                invalid = _policy_columns(policy) - names or _policy_type_error(policy, metadata.schema)
            except Exception:
                invalid = True
            mask = policy.get_column_mask()
            if invalid or (mask is not None and mask.get_on_column() in masked):
                raise TableNoPermissionException(identifier)
            masked |= {mask.get_on_column()} if mask is not None else set()
            applied.append(_canonical_policy(policy, metadata.schema))
        filters = [policy.get_row_filter().get_predicate() for policy in applied if policy.get_row_filter()]
        masks = {policy.get_column_mask().get_on_column(): policy.get_column_mask().get_transform()
                 for policy in applied if policy.get_column_mask()}
        return filters or None, masks or None

    # ======================= Function Handlers ===============================

    def _functions_handle(self, method: str, data: str, database_name: str,
                          parameters: Dict[str, str]) -> Tuple[str, int]:
        """Handle database-scoped function list / create."""
        if method == "GET":
            function_name_pattern = parameters.get(FUNCTION_NAME_PATTERN)
            functions = [
                key.split(".", 1)[1]
                for key in self.function_store.keys()
                if key.startswith(database_name + ".")
                and (not function_name_pattern or self._match_name_pattern(key.split(".", 1)[1], function_name_pattern))
            ]
            return self._generate_final_list_functions_response(parameters, functions)
        elif method == "POST":
            import json as json_module
            request_dict = json_module.loads(data)
            func_name = request_dict.get("name")
            key = f"{database_name}.{func_name}"
            if key in self.function_store:
                identifier = Identifier.create(database_name, func_name)
                raise FunctionAlreadyExistException(identifier)
            self.function_store[key] = GetFunctionResponse(
                uuid=str(uuid.uuid4()),
                name=func_name,
                input_params=request_dict.get("inputParams"),
                return_params=request_dict.get("returnParams"),
                deterministic=request_dict.get("deterministic", False),
                definitions=request_dict.get("definitions"),
                comment=request_dict.get("comment"),
                options=request_dict.get("options", {}),
                owner="owner",
                created_at=1,
                created_by="owner",
                updated_at=1,
                updated_by="owner",
            )
            return self._mock_response("", 200)
        return self._mock_response(ErrorResponse(None, None, "Method Not Allowed", 405), 405)

    def _function_handle(self, method: str, data: str, identifier: Identifier) -> Tuple[str, int]:
        """Handle individual function operations (GET, POST alter, DELETE)."""
        key = identifier.get_full_name()
        if method == "GET":
            if key not in self.function_store:
                raise FunctionNotExistException(identifier)
            return self._mock_response(self.function_store[key], 200)
        elif method == "POST":
            # Alter function
            if key not in self.function_store:
                raise FunctionNotExistException(identifier)
            import json as json_module
            request_dict = json_module.loads(data)
            changes = request_dict.get("changes", [])
            self._apply_function_changes(identifier, changes)
            return self._mock_response("", 200)
        elif method == "DELETE":
            if key not in self.function_store:
                raise FunctionNotExistException(identifier)
            del self.function_store[key]
            self._remove_permissions(
                lambda resource: resource.get_type() == ResourceType.FUNCTION
                and resource.get_database() == identifier.get_database_name()
                and resource.get_function() == identifier.get_object_name())
            return self._mock_response("", 200)
        return self._mock_response(ErrorResponse(None, None, "Method Not Allowed", 405), 405)

    def _function_details_handle(self, database_name: str,
                                 parameters: Dict[str, str]) -> Tuple[str, int]:
        """Handle function details listing."""
        function_name_pattern = parameters.get(FUNCTION_NAME_PATTERN)
        details = []
        for key, resp in self.function_store.items():
            if key.startswith(database_name + "."):
                func_name = key.split(".", 1)[1]
                if not function_name_pattern or self._match_name_pattern(func_name, function_name_pattern):
                    details.append(resp)
        return self._generate_final_list_function_details_response(parameters, details)

    def _functions_globally_handle(self, parameters: Dict[str, str]) -> Tuple[str, int]:
        """Handle catalog-scoped function listing."""
        database_name_pattern = parameters.get(DATABASE_NAME_PATTERN)
        function_name_pattern = parameters.get(FUNCTION_NAME_PATTERN)
        identifiers = []
        for key in self.function_store.keys():
            db_name, func_name = key.split(".", 1)
            if database_name_pattern and not self._match_name_pattern(db_name, database_name_pattern):
                continue
            if function_name_pattern and not self._match_name_pattern(func_name, function_name_pattern):
                continue
            identifiers.append(Identifier.create(db_name, func_name))
        return self._generate_final_list_functions_globally_response(parameters, identifiers)

    def _apply_function_changes(self, identifier: Identifier, changes: List[Dict]) -> None:
        """Apply function changes to the function store, mirroring Java mock server logic."""
        from pypaimon.function.function_change import Actions
        key = identifier.get_full_name()
        func_resp = self.function_store[key]

        # Work with mutable copies
        options = dict(func_resp.options) if func_resp.options else {}
        definitions = dict(func_resp.definitions) if func_resp.definitions else {}
        comment = func_resp.comment

        for change in changes:
            action = change.get("action")
            if action == Actions.SET_OPTION:
                options[change["key"]] = change["value"]
            elif action == Actions.REMOVE_OPTION:
                options.pop(change["key"], None)
            elif action == Actions.UPDATE_COMMENT:
                comment = change.get("comment")
            elif action == Actions.ADD_DEFINITION:
                name = change["name"]
                if name in definitions:
                    raise DefinitionAlreadyExistException(identifier, name)
                definitions[name] = change["definition"]
            elif action == Actions.UPDATE_DEFINITION:
                name = change["name"]
                if name not in definitions:
                    raise DefinitionNotExistException(identifier, name)
                definitions[name] = change["definition"]
            elif action == Actions.DROP_DEFINITION:
                name = change["name"]
                if name not in definitions:
                    raise DefinitionNotExistException(identifier, name)
                del definitions[name]

        self.function_store[key] = GetFunctionResponse(
            uuid=func_resp.uuid,
            name=func_resp.name,
            input_params=func_resp.input_params,
            return_params=func_resp.return_params,
            deterministic=func_resp.deterministic,
            definitions=definitions,
            comment=comment,
            options=options,
            owner=func_resp.owner,
            created_at=func_resp.created_at,
            created_by=func_resp.created_by,
            updated_at=func_resp.updated_at,
            updated_by=func_resp.updated_by,
        )

    def _generate_final_list_functions_response(self, parameters: Dict[str, str],
                                                functions: List[str]) -> Tuple[str, int]:
        if functions:
            max_results = self._get_max_results(parameters)
            page_token = parameters.get(PAGE_TOKEN)
            paged = self._build_paged_entities(functions, max_results, page_token)
            response = ListFunctionsResponse(
                functions=paged.elements,
                next_page_token=paged.next_page_token
            )
        else:
            response = ListFunctionsResponse(functions=[], next_page_token=None)
        return self._mock_response(response, 200)

    def _generate_final_list_function_details_response(self, parameters: Dict[str, str],
                                                       details: List) -> Tuple[str, int]:
        if details:
            max_results = self._get_max_results(parameters)
            page_token = parameters.get(PAGE_TOKEN)
            paged = self._build_paged_entities(details, max_results, page_token)
            response = ListFunctionDetailsResponse(
                function_details=paged.elements,
                next_page_token=paged.next_page_token,
            )
        else:
            response = ListFunctionDetailsResponse(function_details=[], next_page_token=None)
        return self._mock_response(response, 200)

    def _generate_final_list_functions_globally_response(self, parameters: Dict[str, str],
                                                         identifiers: List) -> Tuple[str, int]:
        if identifiers:
            max_results = self._get_max_results(parameters)
            page_token = parameters.get(PAGE_TOKEN)
            paged = self._build_paged_entities(identifiers, max_results, page_token)
            response = ListFunctionsGloballyResponse(
                functions=paged.elements,
                next_page_token=paged.next_page_token,
            )
        else:
            response = ListFunctionsGloballyResponse(functions=[], next_page_token=None)
        return self._mock_response(response, 200)

    def _table_partitions_handle(
            self, method: str, data: str, identifier: Identifier,
            parameters: Dict[str, str]) -> Tuple[str, int]:
        """Handle the table-scoped partitions collection (POST create / GET list-paged)."""
        if identifier.get_full_name() not in self.table_metadata_store:
            raise TableNotExistException(identifier)

        if method == "POST":
            request = JSON.from_json(data, CreatePartitionsRequest)
            store = self.table_partitions_store.setdefault(identifier.get_full_name(), [])
            existing = {self._partition_spec_key(p.spec) for p in store}
            seen, created, existed = set(existing), [], []
            for spec in request.partition_specs or []:
                key = self._partition_spec_key(spec)
                if key in seen:
                    existed.append(spec)
                    continue
                seen.add(key)
                created.append(spec)
            if existed and not request.ignore_if_exists:
                return self._mock_response(
                    ErrorResponse(None, None,
                                  "Partition already exists: {}".format(existed[0]), 409),
                    409)
            store.extend(
                Partition(spec=dict(spec), record_count=0, file_size_in_bytes=0,
                          file_count=0, last_file_creation_time=0, total_buckets=-1,
                          done=False)
                for spec in created)
            return self._mock_response(
                CreatePartitionsResponse(created=created, existed=existed), 200)

        if method != "GET":
            return self._mock_response(ErrorResponse(None, None, "Method Not Allowed", 405), 405)

        partitions = self._list_partitions(identifier, parameters)
        return self._generate_final_list_partitions_response(parameters, partitions)

    @staticmethod
    def _partition_spec_key(spec: Dict[str, str]) -> str:
        """Order-independent identity for a spec."""
        return str(sorted((spec or {}).items()))

    # ======================= Tag Handlers ====================================

    def _tags_handle(self, method: str, data: str, identifier: Identifier,
                     parameters: Dict[str, str]) -> Tuple[str, int]:
        """Handle the table-scoped tags collection (POST create / GET list-paged)."""
        if identifier.get_full_name() not in self.table_metadata_store:
            raise TableNotExistException(identifier)

        if method == "POST":
            request = JSON.from_json(data, CreateTagRequest)
            store = self.tag_store.setdefault(identifier.get_full_name(), {})
            if request.tag_name in store:
                raise TagAlreadyExistException(request.tag_name)
            snapshot = self._resolve_tag_snapshot(identifier, request.snapshot_id)
            store[request.tag_name] = GetTagResponse(
                tag_name=request.tag_name,
                snapshot=snapshot,
                tag_create_time=int(time.time() * 1000),
                tag_time_retained=request.time_retained,
            )
            return self._mock_response("", 200)

        if method == "GET":
            tags = list(self.tag_store.get(identifier.get_full_name(), {}).keys())
            tag_name_prefix = parameters.get(TAG_NAME_PREFIX)
            if tag_name_prefix:
                tags = [t for t in tags if t.startswith(tag_name_prefix)]
            if tags:
                max_results = self._get_max_results(parameters)
                page_token = parameters.get(PAGE_TOKEN)
                paged = self._build_paged_entities(tags, max_results, page_token)
                response = ListTagsResponse(
                    tags=paged.elements, next_page_token=paged.next_page_token)
            else:
                response = ListTagsResponse(tags=[], next_page_token=None)
            return self._mock_response(response, 200)

        return self._mock_response(ErrorResponse(None, None, "Method Not Allowed", 405), 405)

    def _tag_handle(self, method: str, identifier: Identifier,
                    tag_name: str) -> Tuple[str, int]:
        """Handle a single tag (GET / DELETE)."""
        if identifier.get_full_name() not in self.table_metadata_store:
            raise TableNotExistException(identifier)
        store = self.tag_store.get(identifier.get_full_name(), {})

        if method == "GET":
            if tag_name not in store:
                raise TagNotExistException(tag_name)
            return self._mock_response(store[tag_name], 200)
        if method == "DELETE":
            if tag_name not in store:
                raise TagNotExistException(tag_name)
            del store[tag_name]
            return self._mock_response("", 200)
        return self._mock_response(ErrorResponse(None, None, "Method Not Allowed", 405), 405)

    def _resolve_tag_snapshot(self, identifier: Identifier,
                              snapshot_id: Optional[int]):
        """Look up the snapshot to embed in GetTagResponse.

        When ``snapshot_id`` is None, fall back to the table's latest snapshot.
        Mirrors Java behavior of ``TagManager`` resolving the latest snapshot
        when no explicit id is supplied.
        """
        try:
            table = self._get_file_table(identifier)
            snapshot_manager = table.snapshot_manager()
            if snapshot_id is None:
                return snapshot_manager.get_latest_snapshot()
            return snapshot_manager.get_snapshot_by_id(snapshot_id)
        except Exception:
            return None

    # ======================= Branch Handlers ================================

    @staticmethod
    def _validate_branch_name(branch_name):
        from pypaimon.branch.branch_manager import BranchManager
        BranchManager.validate_branch(branch_name)

    def _branches_handle(self, method: str, data: str,
                         identifier: Identifier) -> Tuple[str, int]:
        """Handle the table-scoped branches collection (POST create / GET list)."""
        if identifier.get_full_name() not in self.table_metadata_store:
            raise TableNotExistException(identifier)

        if method == "POST":
            request = JSON.from_json(data, CreateBranchRequest)
            self._validate_branch_name(request.branch)
            store = self.branch_store.setdefault(identifier.get_full_name(), set())
            if request.branch in store:
                raise BranchAlreadyExistException(request.branch)

            metadata = self.table_metadata_store[identifier.get_full_name()]
            schema_id, snapshot = metadata.schema.id, None
            if request.from_tag is not None:
                tags = self.tag_store.get(identifier.get_full_name(), {})
                if request.from_tag not in tags:
                    raise TagNotExistException(request.from_tag)
                snapshot = tags[request.from_tag].snapshot
                schema_id = schema_id if snapshot is None else snapshot.schema_id
            if self._restricted(identifier, metadata):
                raise ValueError("Tables with data policies or column permissions cannot have branches.")
            manager = self._schema_manager(identifier)
            if snapshot is not None:
                self._write_snapshot_files(identifier, snapshot, None, request.branch)
            target = manager.copy_with_branch(request.branch)
            for schema in manager.list_all():
                if schema.id <= schema_id:
                    target.commit(schema)
            store.add(request.branch)
            return self._mock_response("", 200)

        if method == "GET":
            store = self.branch_store.get(identifier.get_full_name(), set())
            response = ListBranchesResponse(branches=sorted(store))
            return self._mock_response(response, 200)

        return self._mock_response(ErrorResponse(None, None, "Method Not Allowed", 405), 405)

    def _branch_handle(self, method: str, identifier: Identifier,
                       branch_name: str) -> Tuple[str, int]:
        """Handle a single branch DELETE."""
        if identifier.get_full_name() not in self.table_metadata_store:
            raise TableNotExistException(identifier)
        store = self.branch_store.get(identifier.get_full_name(), set())

        if method == "DELETE":
            self._validate_branch_name(branch_name)
            if branch_name not in store:
                raise BranchNotExistException(branch_name)
            manager = self._schema_manager(identifier, branch_name)
            manager.file_io.delete(manager.branch_path, recursive=True)
            store.discard(branch_name)
            return self._mock_response("", 200)
        return self._mock_response(ErrorResponse(None, None, "Method Not Allowed", 405), 405)

    def _branch_forward_handle(self, method: str, identifier: Identifier,
                               branch_name: str) -> Tuple[str, int]:
        if method != "POST":
            return self._mock_response(ErrorResponse(None, None, "Method Not Allowed", 405), 405)
        if identifier.get_full_name() not in self.table_metadata_store:
            raise TableNotExistException(identifier)

        store = self.branch_store.get(identifier.get_full_name(), set())
        if branch_name not in store:
            raise BranchNotExistException(branch_name)
        # Mock no-op: real Java fast-forward moves the main branch ref to the
        # target branch's snapshot. Mock just acknowledges the request.
        return self._mock_response("", 200)

    def _databases_api_handler(self, method: str, data: str,
                               parameters: Dict[str, str]) -> Tuple[str, int]:
        """Handle databases API requests"""
        if method == "GET":
            database_name_pattern = parameters.get(DATABASE_NAME_PATTERN)
            databases = [
                db_name for db_name in self.database_store.keys()
                if not database_name_pattern or self._match_name_pattern(db_name, database_name_pattern)
            ]
            return self._generate_final_list_databases_response(parameters, databases)
        if method == "POST":
            create_database = JSON.from_json(data, CreateDatabaseRequest)
            if create_database.name in self.database_store:
                raise DatabaseAlreadyExistException(create_database.name)
            self.database_store.update({
                create_database.name: self.mock_database(create_database.name, create_database.options)
            })
            return self._mock_response("", 200)
        return self._mock_response(ErrorResponse(None, None, "Method Not Allowed", 405), 405)

    def _database_handle(self, method: str, data: str, database_name: str) -> Tuple[str, int]:
        """Handle individual database operations"""
        if database_name not in self.database_store:
            raise DatabaseNotExistException(database_name)

        database = self.database_store[database_name]

        if method == "GET":
            response = database
            return self._mock_response(response, 200)

        elif method == "POST":
            request_body = JSON.from_json(data, AlterDatabaseRequest)
            removals = request_body.removals or []
            updates = request_body.updates or {}
            options = dict(database.options) if database.options else {}
            options.update(updates)
            for key in removals:
                options.pop(key, None)
            self.database_store[database_name] = self.mock_database(
                database_name, options)
            return self._mock_response("", 200)

        elif method == "DELETE":
            del self.database_store[database_name]
            for name in [name for name in self.table_metadata_store
                         if self._table_identifier(name).get_database_name() == database_name]:
                self._drop_table_access(self._table_identifier(name), self.table_metadata_store.pop(name))
                self.table_latest_snapshot_store.pop(name, None)
                self.table_partitions_store.pop(name, None)
            self._remove_permissions(lambda resource: resource.get_database() == database_name)
            return self._mock_response("", 200)
        return self._mock_response(ErrorResponse(None, None, "Method Not Allowed", 405), 405)

    def _tables_handle(self, method: str = None, data: str = None, database_name: str = None,
                       parameters: Dict[str, str] = None) -> Tuple[str, int]:
        """Handle tables operations"""
        if parameters is None:
            parameters = {}

        if database_name:
            # Database-specific tables
            if method == "GET":
                tables = self._list_tables(database_name, parameters)
                return self._generate_final_list_tables_response(parameters, tables)
            elif method == "POST":
                create_table = JSON.from_json(data, CreateTableRequest)
                if create_table.identifier.get_full_name() in self.table_metadata_store:
                    raise TableAlreadyExistException(create_table.identifier)
                table_metadata = self._create_table_metadata(
                    create_table.identifier, 0, create_table.schema, str(uuid.uuid4()), False
                )
                self.table_metadata_store.update({create_table.identifier.get_full_name(): table_metadata})
                self.table_identifiers[create_table.identifier.get_full_name()] = create_table.identifier
                table_dir = (
                    Path(self.data_path) / self.warehouse / database_name /
                    create_table.identifier.get_object_name() / 'schema'
                )
                if not table_dir.exists():
                    table_dir.mkdir(parents=True)
                with open(table_dir / "schema-0", "w") as f:
                    f.write(JSON.to_json(table_metadata.schema, indent=2))
                return self._mock_response("", 200)
        return self._mock_response(ErrorResponse(None, None, "Method Not Allowed", 405), 405)

    def _table_handle(self, method: str, data: str, identifier: Identifier,
                      response_identifier: Identifier = None) -> Tuple[str, int]:
        """Handle individual table operations"""
        if method == "GET":
            if identifier.get_full_name() not in self.table_metadata_store:
                raise TableNotExistException(identifier)
            table_metadata = self._branch_table_metadata(
                identifier, (response_identifier or identifier).get_branch_name())
            table_path = (f'file://{self.data_path}/{self.warehouse}/'
                          f'{identifier.get_database_name()}/{identifier.get_object_name()}')
            schema = table_metadata.schema.to_schema()
            response = self.mock_table(
                response_identifier or identifier, table_metadata, table_path, schema)
            return self._mock_response(response, 200)
        elif method == "POST":
            # Alter table
            request_body = JSON.from_json(data, AlterTableRequest)
            self._alter_table_impl(response_identifier or identifier, request_body.changes)
            return self._mock_response("", 200)
        elif method == "DELETE":
            # Drop table
            if identifier.get_full_name() not in self.table_metadata_store:
                raise TableNotExistException(identifier)
            else:
                self._drop_table_access(
                    identifier, self.table_metadata_store.pop(identifier.get_full_name()))
            if identifier.get_full_name() in self.table_latest_snapshot_store:
                del self.table_latest_snapshot_store[identifier.get_full_name()]
            if identifier.get_full_name() in self.table_partitions_store:
                del self.table_partitions_store[identifier.get_full_name()]

            return self._mock_response("", 200)

        return self._mock_response(ErrorResponse(None, None, "Method Not Allowed", 405), 405)

    def _table_query_auth_handle(self, method, data, identifier):
        if method != "POST":
            return self._mock_response(ErrorResponse(None, None, "Method Not Allowed", 405), 405)
        if identifier.get_full_name() not in self.table_metadata_store:
            raise TableNotExistException(identifier)
        select = json.loads(data).get('select')
        self._check_column_grants(identifier, select)
        from pypaimon.api.api_response import AuthTableQueryResponse
        response = self.table_query_auth_store.get(identifier.get_full_name())
        if response is None:
            response = AuthTableQueryResponse(*self._policy_rules(identifier))
        elif callable(response):
            response = response(select)
        return self._mock_response(response, 200)

    def _table_token_handle(self, method: str, identifier: Identifier) -> Tuple[str, int]:
        if method != "GET":
            return self._mock_response(ErrorResponse(None, None, "Method Not Allowed", 405), 405)

        if identifier.get_full_name() not in self.table_metadata_store:
            raise TableNotExistException(identifier)

        from pypaimon.api.api_response import GetTableTokenResponse

        token_key = identifier.get_full_name()
        if token_key in self.table_token_store:
            rest_token = self.table_token_store[token_key]
            response = GetTableTokenResponse(
                token=rest_token.token,
                expires_at_millis=rest_token.expire_at_millis
            )
        else:
            default_token = {
                "akId": "akId" + str(int(time.time() * 1000)),
                "akSecret": "akSecret" + str(int(time.time() * 1000))
            }
            response = GetTableTokenResponse(
                token=default_token,
                expires_at_millis=int(time.time() * 1000) + 3600_000  # 1 hour from now
            )

        return self._mock_response(response, 200)

    def set_table_token(self, identifier: Identifier, token: "RESTToken") -> None:
        self.table_token_store[identifier.get_full_name()] = token

    def get_table_token(self, identifier: Identifier) -> Optional["RESTToken"]:
        return self.table_token_store.get(identifier.get_full_name())

    def reset_table_token(self, identifier: Identifier) -> None:
        if identifier.get_full_name() in self.table_token_store:
            del self.table_token_store[identifier.get_full_name()]

    def _table_commit_handle(self, method: str, data: str, identifier: Identifier,
                             branch: str = None) -> Tuple[str, int]:
        """Handle table commit operations"""
        if method != "POST":
            return self._mock_response(ErrorResponse(None, None, "Method Not Allowed", 405), 405)

        # Check if table exists
        if identifier.get_full_name() not in self.table_metadata_store:
            raise TableNotExistException(identifier)

        try:
            # Parse the commit request
            from pypaimon.api.api_request import CommitTableRequest
            from pypaimon.api.api_response import CommitTableResponse

            commit_request = JSON.from_json(data, CommitTableRequest)

            # Basic validation
            if not commit_request.snapshot:
                return self._mock_response(
                    ErrorResponse("SNAPSHOT", None, "Snapshot is required for commit operation", 400), 400
                )

            table = self._get_file_table(identifier, branch)
            current_snapshot = table.snapshot_manager().get_latest_snapshot()
            current_snapshot_uuid = (
                current_snapshot.uuid if current_snapshot else None
            )
            if current_snapshot_uuid != commit_request.base_snapshot_uuid:
                return self._mock_response(
                    CommitTableResponse(success=False), 200
                )

            # Write snapshot to file system
            self._write_snapshot_files(
                identifier, commit_request.snapshot,
                commit_request.statistics, branch)

            self.logger.info(f"Successfully committed snapshot for table {identifier.get_full_name()}, "
                             f"branch: {branch or 'main'}")
            self.logger.info(f"Snapshot ID: {commit_request.snapshot.id}")
            self.logger.info(f"Statistics count: {len(commit_request.statistics) if commit_request.statistics else 0}")

            # Create success response
            response = CommitTableResponse(success=True)
            return self._mock_response(response, 200)

        except Exception as e:
            self.logger.error(f"Error in commit operation: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return self._mock_response(
                ErrorResponse(None, None, f"Commit failed: {str(e)}", 500), 500
            )

    def _table_rollback_handle(self, method: str, data: str,
                               identifier: Identifier) -> Tuple[str, int]:
        """Handle table rollback operations"""
        if method != "POST":
            return self._mock_response(ErrorResponse(None, None, "Method Not Allowed", 405), 405)

        if identifier.get_full_name() not in self.table_metadata_store:
            raise TableNotExistException(identifier)

        try:
            import json as json_module
            from pypaimon.table.instant import Instant, SnapshotInstant, TagInstant

            request_dict = json_module.loads(data)
            instant_dict = request_dict.get("instant")
            from_snapshot = request_dict.get("fromSnapshot")

            instant = Instant.from_dict(instant_dict)

            if isinstance(instant, SnapshotInstant):
                return self._rollback_table_by_snapshot(
                    identifier, instant.snapshot_id, from_snapshot)
            elif isinstance(instant, TagInstant):
                return self._rollback_table_by_tag(identifier, instant.tag_name)
            else:
                return self._mock_response(
                    ErrorResponse(None, None, "Unknown instant type", 400), 400)

        except Exception as e:
            self.logger.error(f"Error in rollback operation: {e}")
            import traceback
            self.logger.error(f"Traceback: {traceback.format_exc()}")
            return self._mock_response(
                ErrorResponse(None, None, f"Rollback failed: {str(e)}", 500), 500)

    def _rollback_table_by_snapshot(self, identifier: Identifier, snapshot_id: int,
                                    from_snapshot: Optional[int]) -> Tuple[str, int]:
        """Rollback table to a specific snapshot ID by delegating to table.rollback_to()."""
        table = self._get_file_table(identifier)

        snapshot_mgr = table.snapshot_manager()
        snapshot = snapshot_mgr.get_snapshot_by_id(snapshot_id)
        if snapshot is None:
            return self._mock_response(
                ErrorResponse(ErrorResponse.RESOURCE_TYPE_SNAPSHOT,
                              str(snapshot_id), "", 404), 404)

        latest = snapshot_mgr.get_latest_snapshot()
        if latest is None:
            return self._mock_response(
                ErrorResponse(None, None, "No latest snapshot found", 500), 500)

        if from_snapshot is not None and from_snapshot != latest.id:
            return self._mock_response(
                ErrorResponse(None, None,
                              f"Latest snapshot {latest.id} is not {from_snapshot}",
                              500), 500)

        table.rollback_to(snapshot_id)
        return self._mock_response("", 200)

    def _rollback_table_by_tag(self, identifier: Identifier,
                               tag_name: str) -> Tuple[str, int]:
        """Rollback table to a specific tag by delegating to table.rollback_to()."""
        table = self._get_file_table(identifier)

        tag_mgr = table.tag_manager()
        if not tag_mgr.tag_exists(tag_name):
            return self._mock_response(
                ErrorResponse(ErrorResponse.RESOURCE_TYPE_TAG,
                              tag_name, "", 404), 404)

        table.rollback_to(tag_name)
        return self._mock_response("", 200)

    def _table_snapshot_handle(self, method: str, identifier: Identifier,
                               branch: str = None) -> Tuple[str, int]:
        """Handle table snapshot operations.

        Args:
            method: HTTP method
            identifier: Table identifier

        Returns:
            Tuple of (response JSON, HTTP status code)
        """
        if method != "GET":
            return self._mock_response(ErrorResponse(None, None, "Method Not Allowed", 405), 405)

        if identifier.get_full_name() not in self.table_metadata_store:
            raise TableNotExistException(identifier)

        table_metadata = self.table_metadata_store[identifier.get_full_name()]
        if table_metadata.is_external:
            response = ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_TABLE,
                identifier.get_full_name(),
                "external paimon table does not support get table snapshot in rest server",
                501)
            return self._mock_response(response, 404)

        # Get the table and snapshot manager to retrieve snapshot
        table = self._get_file_table(identifier, branch)
        snapshot_manager = table.snapshot_manager()

        # Get latest snapshot
        snapshot = snapshot_manager.get_latest_snapshot()

        if snapshot is None:
            response = ErrorResponse(
                ErrorResponse.RESOURCE_TYPE_SNAPSHOT,
                identifier.get_database_name(),
                "No Snapshot",
                404)
            return self._mock_response(response, 404)

        from pypaimon.api.api_response import GetTableSnapshotResponse
        from pypaimon.snapshot.table_snapshot import TableSnapshot

        table_snapshot = TableSnapshot(
            snapshot=snapshot,
            record_count=snapshot.total_record_count,
            file_size_in_bytes=0,
            file_count=0,
            last_file_creation_time=snapshot.time_millis
        )
        response = GetTableSnapshotResponse(table_snapshot)
        return self._mock_response(response, 200)

    def _get_file_table(self, identifier: Identifier, branch: str = None):
        """Construct a FileStoreTable from the metadata store.

        loads the schema from the metadata store, builds a CatalogEnvironment
        (without catalog loader so rollback goes through local file cleanup),
        and returns a FileStoreTable.
        """
        from pypaimon.catalog.catalog_environment import CatalogEnvironment
        from pypaimon.common.file_io import FileIO
        from pypaimon.common.options.options import Options
        from pypaimon.table.file_store_table import FileStoreTable

        table_metadata = self._branch_table_metadata(identifier, branch)
        table_schema = table_metadata.schema
        table_path = (
            f'file://{self.data_path}/{self.warehouse}/'
            f'{identifier.get_database_name()}/{identifier.get_object_name()}')

        table_identifier = Identifier.create(
            identifier.get_database_name(), identifier.get_table_name(), branch=branch)
        catalog_env = CatalogEnvironment(
            identifier=table_identifier,
            uuid=table_metadata.uuid,
            catalog_loader=None,
            supports_version_management=False
        )

        file_io = FileIO.get(table_path, Options({}))
        return FileStoreTable(
            file_io, table_identifier, table_path, table_schema, catalog_env)

    def _write_snapshot_files(self, identifier: Identifier, snapshot, statistics,
                              branch: str = None):
        """Write snapshot and related files to the file system"""
        import os

        # Construct table path: {warehouse}/{database}/{table}
        from pypaimon.branch.branch_manager import BranchManager

        table_path = os.path.join(
            self.data_path, self.warehouse, identifier.get_database_name(),
            identifier.get_object_name())
        table_path = BranchManager.branch_path(
            table_path, BranchManager.normalize_branch(branch))

        # Create directory structure
        snapshot_dir = os.path.join(table_path, "snapshot")

        os.makedirs(snapshot_dir, exist_ok=True)

        # Write snapshot file (snapshot-{id})
        snapshot_file = os.path.join(snapshot_dir, f"snapshot-{snapshot.id}")
        with open(snapshot_file, 'w') as f:
            f.write(JSON.to_json(snapshot))

        # Write LATEST file
        latest_file = os.path.join(snapshot_dir, "LATEST")
        with open(latest_file, 'w') as f:
            f.write(str(snapshot.id))

        # Create partition directories based on statistics
        if statistics:
            for stat in statistics:
                if hasattr(stat, 'spec') and stat.spec:
                    # Extract partition information from spec
                    partition_parts = []
                    for key, value in stat.spec.items():
                        partition_parts.append(f"{key}={value}")

                    if partition_parts:
                        partition_dir = os.path.join(table_path, *partition_parts)
                        os.makedirs(partition_dir, exist_ok=True)

        # If no statistics provided, create default partition directories for test
        if not statistics:
            # Create default partitions that the test expects
            default_partitions = ["dt=p1", "dt=p2"]
            for partition in default_partitions:
                partition_dir = os.path.join(table_path, partition)
                os.makedirs(partition_dir, exist_ok=True)

        self.logger.info(f"Created snapshot files at: {snapshot_dir}")

    # Utility methods
    def _mock_response(self, response: Union[RESTResponse, str], http_code: int) -> Tuple[str, int]:
        """Create mock response"""
        if isinstance(response, str):
            return response, http_code

        try:
            return JSON.to_json(response), http_code
        except Exception as e:
            self.logger.error(f"Failed to serialize response: {e}")
            return str(e), 500

    def _get_max_results(self, parameters: Dict[str, str]) -> int:
        """Get max results from parameters"""
        max_results_str = parameters.get(MAX_RESULTS)
        if max_results_str:
            try:
                max_results = int(max_results_str)
                return min(max_results, DEFAULT_MAX_RESULTS) if max_results > 0 else DEFAULT_MAX_RESULTS
            except ValueError:
                raise ValueError(f"Invalid maxResults value: {max_results_str}")
        return DEFAULT_MAX_RESULTS

    def _management_page(self, entities: Dict[str, Any],
                         parameters: Dict[str, str]) -> Tuple[List[Any], Optional[str]]:
        token = parameters.get(PAGE_TOKEN)
        keys = sorted(key for key in entities if not token or key > token)
        size = int(parameters[MAX_RESULTS]) if parameters.get(MAX_RESULTS) else DEFAULT_MAX_RESULTS
        page = keys[:max(1, min(size, ListPermissionsRequest.MAX_PAGE_SIZE))]
        return [entities[key] for key in page], page[-1] if len(keys) > len(page) else None

    def _build_paged_entities(self, entities: List[Any], max_results: int,
                              page_token: Optional[str], desc: bool = False) -> PagedList:
        """Build paged entities"""
        # Sort entities
        sorted_entities = sorted(entities, key=self._get_paged_key, reverse=desc)

        # Apply pagination
        paged_entities = []
        for entity in sorted_entities:
            if len(paged_entities) < max_results:
                if not page_token or self._get_paged_key(entity) > page_token:
                    paged_entities.append(entity)
            else:
                break

        # Determine next page token
        next_page_token = None
        if len(paged_entities) == max_results and len(sorted_entities) > max_results:
            next_page_token = self._get_paged_key(paged_entities[-1])

        return PagedList(elements=paged_entities, next_page_token=next_page_token)

    def _get_paged_key(self, entity: Any) -> str:
        """Get paging key for entity"""
        if isinstance(entity, str):
            return entity
        elif isinstance(entity, Partition):
            return "/".join(f"{k}={v}" for k, v in sorted(entity.spec.items()))
        elif hasattr(entity, 'get_name'):
            return entity.get_name()
        elif hasattr(entity, 'get_full_name'):
            return entity.get_full_name()
        elif hasattr(entity, 'name'):
            return entity.name
        else:
            return str(entity)

    def _match_name_pattern(self, name: str, pattern: str) -> bool:
        """Match name against SQL pattern"""
        if not pattern:
            raise ValueError("Pattern cannot be empty")
        regex_pattern = self._sql_pattern_to_regex(pattern)
        return re.match(regex_pattern, name) is not None

    def _sql_pattern_to_regex(self, pattern: str) -> str:
        """Convert SQL pattern to regex"""
        regex = []
        escaped = False

        for char in pattern:
            if escaped:
                regex.append(re.escape(char))
                escaped = False
            elif char == '\\':
                escaped = True
            elif char == '%':
                regex.append('.*')
            elif char == '_':
                regex.append('.')
            else:
                regex.append(re.escape(char))

        return '^' + ''.join(regex) + '$'

    def _schema_manager(self, identifier: Identifier, branch: str = None):
        table_path = (Path(self.data_path) / self.warehouse /
                      identifier.get_database_name() / identifier.get_table_name())
        return SchemaManager(self._get_file_io(), str(table_path), branch or 'main')

    def _branch_table_metadata(self, identifier: Identifier, branch: str = None):
        base = Identifier.create(identifier.get_database_name(), identifier.get_table_name())
        metadata = self.table_metadata_store.get(base.get_full_name())
        if metadata is None:
            raise TableNotExistException(base)
        if branch is None or branch == 'main':
            return metadata
        if branch not in self.branch_store.get(base.get_full_name(), set()):
            raise BranchNotExistException(branch)
        schema = self._schema_manager(base, branch).latest()
        if schema is None:
            raise BranchNotExistException(branch)
        return TableMetadata(schema=schema, uuid=metadata.uuid, is_external=metadata.is_external)

    def _alter_table_impl(self, identifier: Identifier, changes: List) -> None:
        branch = identifier.get_branch_name()
        table_metadata = self._branch_table_metadata(identifier, branch)

        schema_changes = []
        for change in changes:
            if isinstance(change, dict):
                try:
                    schema_changes.append(_dict_to_schema_change(change))
                except (KeyError, TypeError) as e:
                    raise ValueError(f"Failed to convert change dict to SchemaChange: {change}, error: {e}") from e
            else:
                schema_changes.append(change)

        schema_manager = self._schema_manager(identifier, branch)
        main = branch is None or branch == 'main'
        base = Identifier.create(identifier.get_database_name(), identifier.get_table_name())
        if main:
            self._validate_access_for_schema(
                base, table_metadata, schema_manager._generate_table_schema(table_metadata.schema, schema_changes))
        new_schema = schema_manager.commit_changes(schema_changes)

        updated_metadata = TableMetadata(
            schema=new_schema,
            is_external=table_metadata.is_external,
            uuid=table_metadata.uuid
        )
        if main:
            self._evolve_column_grants(base, table_metadata.schema, new_schema)
            self.table_metadata_store[base.get_full_name()] = updated_metadata

    def _validate_access_for_schema(self, identifier: Identifier, metadata: TableMetadata,
                                    schema: TableSchema) -> None:
        name = identifier.get_full_name()
        policies = self.policy_store.get(metadata.uuid or name, {})
        if policies and not _query_auth_enabled(schema):
            raise ValueError(
                "Cannot disable query-auth.enabled while table {} has data policies.".format(name))
        for policy in policies.values():
            missing = _moved_columns(policy, metadata.schema, schema)
            if missing:
                raise ValueError("Cannot remove or rename policy column {} from table {}.".format(missing[0], name))
            mismatch = _policy_type_error(policy, schema)
            if mismatch:
                raise ValueError(mismatch)
            try:
                _canonical_policy(policy, schema)
            except _UnsupportedConversion:
                raise
            except Exception:
                pass
        grants = self._column_grants(identifier)
        if grants and not _query_auth_enabled(schema):
            raise ValueError(
                "Cannot disable query-auth.enabled while table {} has column permissions.".format(name))
        renamed = _renamed_columns(metadata.schema, schema)
        for grant in grants:
            if grant.get_columns().get_column_names() is not None \
                    and not any(renamed.get(column) for column in _granted_columns(grant)):
                raise ValueError(
                    "Cannot drop every allowed column while table {} has column permissions."
                    .format(name))

    def _evolve_column_grants(self, identifier: Identifier, previous: TableSchema,
                              current: TableSchema) -> None:
        renamed = _renamed_columns(previous, current)
        for grant in self._column_grants(identifier):
            del self.permission_store[_assignment_key(grant)]
            evolved = [renamed[column] for column in _granted_columns(grant) if renamed.get(column)]
            if not evolved:
                continue
            columns = (PermissionColumns(column_names=evolved)
                       if grant.get_columns().get_column_names() is not None
                       else PermissionColumns(excluded_column_names=evolved))
            evolved_grant = PermissionAssignment(
                grant.get_resource(), grant.get_access(), grant.get_principal(),
                columns, grant.get_expire_time())
            self.permission_store[_assignment_key(evolved_grant)] = evolved_grant

    def _get_file_io(self):
        """Get FileIO instance for SchemaManager"""
        from pypaimon.common.file_io import FileIO
        from pypaimon.common.options import Options
        warehouse_path = str(Path(self.data_path) / self.warehouse)
        options = Options({"warehouse": warehouse_path})
        return FileIO.get(warehouse_path, options)

    def _create_table_metadata(self, identifier: Identifier, schema_id: int,
                               schema: Schema, uuid_str: str, is_external: bool) -> TableMetadata:
        """Create table metadata"""
        options = schema.options.copy()

        fields = schema.fields
        if schema.primary_keys:
            pk_set = set(schema.primary_keys)
            for field in fields:
                if field.name in pk_set:
                    field.type.nullable = False

        table_schema = TableSchema(
            version=TableSchema.CURRENT_VERSION,
            id=schema_id,
            fields=schema.fields,
            highest_field_id=len(schema.fields) - 1,
            partition_keys=schema.partition_keys,
            primary_keys=schema.primary_keys,
            options=options,
            comment=schema.comment
        )

        return TableMetadata(
            schema=table_schema,
            is_external=is_external,
            uuid=uuid_str or str(uuid.uuid4())
        )

    # List methods
    def _list_tables(self, database_name: str, parameters: Dict[str, str]) -> List[str]:
        """List tables in database"""
        table_name_pattern = parameters.get(TABLE_NAME_PATTERN)
        table_type = parameters.get(TABLE_TYPE)
        tables = []

        for full_name, metadata in self.table_metadata_store.items():
            identifier = Identifier.from_string(full_name)
            metadata_table_type = (
                metadata.schema.options.get(TYPE, "table")
                if metadata and metadata.schema and metadata.schema.options
                else "table"
            )
            table_type_matches = (
                not table_type
                or metadata_table_type == table_type
            )
            if (identifier.get_database_name() == database_name and
                table_type_matches and
                (not table_name_pattern or self._match_name_pattern(identifier.get_table_name(),
                                                                    table_name_pattern))):
                tables.append(identifier.get_table_name())

        return tables

    def _list_partitions(self, identifier: Identifier, parameters: Dict[str, str]) -> List[Partition]:
        """List partitions for a table from the partitions store."""
        partition_name_pattern = parameters.get(PARTITION_NAME_PATTERN)
        partitions = self.table_partitions_store.get(identifier.get_full_name(), [])
        if partition_name_pattern:
            partitions = [
                p for p in partitions
                if self._match_partition_name_pattern(p, partition_name_pattern)
            ]
        return partitions

    def _match_partition_name_pattern(self, partition: Partition, pattern: str) -> bool:
        """Match partition spec against a name pattern."""
        partition_name = "/".join(f"{k}={v}" for k, v in sorted(partition.spec.items()))
        return self._match_name_pattern(partition_name, pattern)

    # Response generation methods
    def _generate_final_list_databases_response(self, parameters: Dict[str, str],
                                                databases: List[str]) -> Tuple[str, int]:
        """Generate final list databases response"""
        if databases:
            max_results = self._get_max_results(parameters)
            page_token = parameters.get(PAGE_TOKEN)
            paged_dbs = self._build_paged_entities(databases, max_results, page_token)
            response = ListDatabasesResponse(
                databases=paged_dbs.elements,
                next_page_token=paged_dbs.next_page_token
            )
        else:
            response = ListDatabasesResponse(databases=[], next_page_token=None)

        return self._mock_response(response, 200)

    def _generate_final_list_tables_response(self, parameters: Dict[str, str],
                                             tables: List[str]) -> Tuple[str, int]:
        """Generate final list tables response"""
        if tables:
            max_results = self._get_max_results(parameters)
            page_token = parameters.get(PAGE_TOKEN)
            paged_tables = self._build_paged_entities(tables, max_results, page_token)
            response = ListTablesResponse(
                tables=paged_tables.elements,
                next_page_token=paged_tables.next_page_token
            )
        else:
            response = ListTablesResponse(tables=[], next_page_token=None)

        return self._mock_response(response, 200)

    def _generate_final_list_partitions_response(
            self, parameters: Dict[str, str], partitions: List[Partition]) -> Tuple[str, int]:
        """Generate final list partitions response"""
        if partitions:
            max_results = self._get_max_results(parameters)
            page_token = parameters.get(PAGE_TOKEN)
            paged_partitions = self._build_paged_entities(partitions, max_results, page_token)
            response = ListPartitionsResponse(
                partitions=paged_partitions.elements,
                next_page_token=paged_partitions.next_page_token
            )
        else:
            response = ListPartitionsResponse(partitions=[], next_page_token=None)

        return self._mock_response(response, 200)

    def add_no_permission_database(self, database: str) -> None:
        """Add no permission database"""
        self.no_permission_databases.append(database)

    def add_no_permission_table(self, identifier: Identifier) -> None:
        """Add no permission table"""
        self.no_permission_tables.append(identifier.get_full_name())

    def mock_database(self, name: str, options: Dict[str, str]) -> GetDatabaseResponse:
        return GetDatabaseResponse(
            id=str(uuid.uuid4()),
            name=name,
            location=f"{self.data_path}/{name}",
            options=options,
            owner="owner",
            created_at=int(time.time()) * 1000,
            created_by="created",
            updated_at=int(time.time()) * 1000,
            updated_by="updated"
        )

    def mock_table(self, identifier: Identifier, table_metadata: TableMetadata, path: str,
                   schema: Schema) -> GetTableResponse:
        return GetTableResponse(
            id=str(table_metadata.uuid),
            name=identifier.get_object_name(),
            path=path,
            is_external=table_metadata.is_external,
            schema_id=table_metadata.schema.id,
            schema=schema,
            owner="owner",
            created_at=1,
            created_by="created",
            updated_at=1,
            updated_by="updated"
        )
