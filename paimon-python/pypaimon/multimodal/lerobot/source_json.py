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
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""JSON representation of source Arrow metadata without opaque IPC payloads."""

import base64
import json
import math
from decimal import Decimal

import pyarrow as pa


def _metadata_json(metadata):
    return None if metadata is None else [
        [base64.b64encode(key).decode("ascii"),
         base64.b64encode(value).decode("ascii")]
        for key, value in metadata.items()]


def _metadata_from_json(value):
    return None if value is None else {
        base64.b64decode(key): base64.b64decode(item)
        for key, item in value}


def _type_json(dtype):
    if pa.types.is_struct(dtype):
        return {"kind": "struct", "fields": [_field_json(field) for field in dtype]}
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype) \
            or pa.types.is_fixed_size_list(dtype):
        result = {"kind": "list", "value": _field_json(dtype.value_field)}
        if pa.types.is_fixed_size_list(dtype):
            result["size"] = dtype.list_size
        if pa.types.is_large_list(dtype):
            result["large"] = True
        return result
    if pa.types.is_map(dtype):
        return {"kind": "map", "key": _type_json(dtype.key_type),
                "item": _type_json(dtype.item_type),
                "keys_sorted": dtype.keys_sorted}
    if pa.types.is_dictionary(dtype):
        return {"kind": "dictionary", "index": _type_json(dtype.index_type),
                "value": _type_json(dtype.value_type), "ordered": dtype.ordered}
    if pa.types.is_decimal(dtype):
        return {"kind": "decimal", "bits": dtype.bit_width,
                "precision": dtype.precision, "scale": dtype.scale}
    if pa.types.is_timestamp(dtype):
        return {"kind": "timestamp", "unit": dtype.unit, "tz": dtype.tz}
    if pa.types.is_time32(dtype) or pa.types.is_time64(dtype) \
            or pa.types.is_duration(dtype):
        return {"kind": "temporal", "name": dtype.id, "unit": dtype.unit}
    if pa.types.is_fixed_size_binary(dtype):
        return {"kind": "fixed_binary", "width": dtype.byte_width}
    return {"kind": "alias", "name": str(dtype)}


def _type_from_json(value):
    kind = value["kind"]
    if kind == "struct":
        return pa.struct([_field_from_json(field) for field in value["fields"]])
    if kind == "list":
        field = _field_from_json(value["value"])
        if "size" in value:
            return pa.list_(field, value["size"])
        return pa.large_list(field) if value.get("large") else pa.list_(field)
    if kind == "map":
        return pa.map_(_type_from_json(value["key"]),
                       _type_from_json(value["item"]), value["keys_sorted"])
    if kind == "dictionary":
        return pa.dictionary(_type_from_json(value["index"]),
                             _type_from_json(value["value"]), value["ordered"])
    if kind == "decimal":
        factory = pa.decimal256 if value["bits"] == 256 else pa.decimal128
        return factory(value["precision"], value["scale"])
    if kind == "timestamp":
        return pa.timestamp(value["unit"], tz=value["tz"])
    if kind == "temporal":
        factory = {pa.lib.Type_TIME32: pa.time32,
                   pa.lib.Type_TIME64: pa.time64,
                   pa.lib.Type_DURATION: pa.duration}[value["name"]]
        return factory(value["unit"])
    if kind == "fixed_binary":
        return pa.binary(value["width"])
    return pa.type_for_alias(value["name"])


def _field_json(field):
    return {"name": field.name, "type": _type_json(field.type),
            "nullable": field.nullable, "metadata": _metadata_json(field.metadata)}


def _field_from_json(value):
    return pa.field(value["name"], _type_from_json(value["type"]),
                    nullable=value["nullable"],
                    metadata=_metadata_from_json(value["metadata"]))


def _schema_json(schema):
    return json.dumps({
        "fields": [_field_json(field) for field in schema],
        "metadata": _metadata_json(schema.metadata),
    }, ensure_ascii=False, separators=(",", ":"))


def _schema_from_json(value):
    value = json.loads(value)
    return pa.schema([_field_from_json(field) for field in value["fields"]],
                     metadata=_metadata_from_json(value["metadata"]))


def _value_json(value, dtype):
    if value is None:
        return None
    if pa.types.is_dictionary(dtype):
        return _value_json(value, dtype.value_type)
    if pa.types.is_struct(dtype):
        return {field.name: _value_json(value[field.name], field.type)
                for field in dtype}
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype) \
            or pa.types.is_fixed_size_list(dtype):
        return [_value_json(item, dtype.value_type) for item in value]
    if pa.types.is_map(dtype):
        return [[_value_json(key, dtype.key_type),
                 _value_json(item, dtype.item_type)] for key, item in value]
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype) \
            or pa.types.is_fixed_size_binary(dtype):
        return base64.b64encode(value).decode("ascii")
    if pa.types.is_decimal(dtype):
        return str(value)
    if pa.types.is_timestamp(dtype) or pa.types.is_date(dtype) \
            or pa.types.is_time(dtype) or pa.types.is_duration(dtype):
        integer_type = (pa.int32() if pa.types.is_date32(dtype) or pa.types.is_time32(dtype)
                        else pa.int64())
        return pa.scalar(value, type=dtype).cast(integer_type).as_py()
    if pa.types.is_floating(dtype) and not math.isfinite(value):
        return str(value)
    return value


def _value_from_json(value, dtype):
    if value is None:
        return None
    if pa.types.is_dictionary(dtype):
        return _value_from_json(value, dtype.value_type)
    if pa.types.is_struct(dtype):
        return {field.name: _value_from_json(value[field.name], field.type)
                for field in dtype}
    if pa.types.is_list(dtype) or pa.types.is_large_list(dtype) \
            or pa.types.is_fixed_size_list(dtype):
        return [_value_from_json(item, dtype.value_type) for item in value]
    if pa.types.is_map(dtype):
        return [(_value_from_json(key, dtype.key_type),
                 _value_from_json(item, dtype.item_type)) for key, item in value]
    if pa.types.is_binary(dtype) or pa.types.is_large_binary(dtype) \
            or pa.types.is_fixed_size_binary(dtype):
        return base64.b64decode(value)
    if pa.types.is_decimal(dtype):
        return Decimal(value)
    if pa.types.is_timestamp(dtype) or pa.types.is_date(dtype) \
            or pa.types.is_time(dtype) or pa.types.is_duration(dtype):
        integer_type = (pa.int32() if pa.types.is_date32(dtype) or pa.types.is_time32(dtype)
                        else pa.int64())
        return pa.scalar(value, type=integer_type).cast(dtype).as_py()
    if pa.types.is_floating(dtype) and value in ("nan", "inf", "-inf"):
        return float(value)
    return value


def _row_patch_json(source, stored, schema):
    return json.dumps({
        field.name: _value_json(source[field.name], field.type)
        for field in schema
        if field.name not in stored or source[field.name] != stored[field.name]
    }, ensure_ascii=False, separators=(",", ":"))


def _row_from_patch_json(patch, stored, schema):
    patch = json.loads(patch)
    return {
        field.name: (_value_from_json(patch[field.name], field.type)
                     if field.name in patch else stored[field.name])
        for field in schema
    }
