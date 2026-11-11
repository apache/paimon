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

"""LeRobot metadata validation and Arrow schema conversion."""

import json

import pyarrow as pa

from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.schema.schema_change import SchemaChange
from pypaimon.schema.column_directive_utils import VIDEO_FRAME_FIELD_DIRECTIVE


_SCALAR_DTYPES = {
    "bool": pa.bool_(),
    "boolean": pa.bool_(),
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.int16(),
    "uint16": pa.int32(),
    "uint32": pa.int64(),
    "float16": pa.float32(),
    "float32": pa.float32(),
    "float64": pa.float64(),
    "string": pa.string(),
}

_V3_REQUIRED_FEATURE_DTYPES = {
    "timestamp": "float32",
    "frame_index": "int64",
    "episode_index": "int64",
    "index": "int64",
    "task_index": "int64",
}


def _require_v3(info, source):
    version = str(info.get("codebase_version", ""))
    if not (version == "v3" or version.startswith("v3.")):
        raise ValueError(
            "load_from_lerobot supports LeRobot Dataset v3 only; %s reports "
            "codebase_version=%r. Upgrade the dataset to v3 first."
            % (source, version or None))


def _schema_from_info(info, legacy_vectors=False):
    features = info.get("features")
    if not isinstance(features, dict) or not features:
        raise ValueError("LeRobot metadata features must be a non-empty object.")

    return pa.schema([
        _feature_field(name, feature, legacy_vectors)
        for name, feature in features.items()
    ])


def _validate_v3_required_features(info):
    features = info.get("features")
    if not isinstance(features, dict):
        raise ValueError("LeRobot metadata features must be an object.")
    expected = dict(_V3_REQUIRED_FEATURE_DTYPES)
    if "subtask_index" in features:
        expected["subtask_index"] = "int64"
    for name, dtype in expected.items():
        feature = features.get(name)
        if not isinstance(feature, dict) \
                or str(feature.get("dtype", "")) != dtype \
                or _feature_shape(feature, name) != (1,):
            raise ValueError(
                "LeRobot V3 required feature %s must have dtype=%s and "
                "shape=[1]." % (name, dtype))


def _video_feature_names(info):
    return [
        name
        for name, feature in info.get("features", {}).items()
        if isinstance(feature, dict) and feature.get("dtype") == "video"
    ]


def _validate_lerobot_schema(source_schema, target_schema, source):
    """Require an existing table to preserve the LeRobot feature contract."""
    for source_field in source_schema:
        target_index = target_schema.get_field_index(source_field.name)
        if target_index < 0:
            # The shared schema validator reports missing columns consistently.
            continue
        target_field = target_schema.field(target_index)
        if source_field.type != target_field.type:
            raise ValueError(
                "LeRobot feature %s from %s cannot be converted to the "
                "table schema: expected %s, found %s."
                % (source_field.name, source, source_field.type,
                   target_field.type))

        source_description = _description(source_field)
        if not source_description.startswith("LeRobot dtype="):
            continue
        target_description = _description(target_field)
        if target_description != source_description:
            raise ValueError(
                "LeRobot feature %s from %s cannot be converted to the "
                "table schema: expected %s, found %s."
                % (source_field.name, source, source_description,
                   target_description or "no LeRobot feature metadata"))


def _description(field):
    if not field.metadata:
        return ""
    return field.metadata.get(b"description", b"").decode("utf-8")


def _feature_field(name, feature, legacy_vectors=False):
    if not isinstance(feature, dict):
        raise ValueError(
            "LeRobot feature %s metadata must be an object." % name)
    dtype = str(feature.get("dtype", ""))
    shape = _feature_shape(feature, name)
    if dtype in ("image", "video"):
        arrow_type = pa.large_binary()
    else:
        scalar_type = _SCALAR_DTYPES.get(dtype)
        if scalar_type is None:
            suffix = " (uint64 has no lossless Paimon integer mapping)" \
                if dtype == "uint64" else ""
            raise ValueError(
                "Unsupported LeRobot dtype %r for feature %s%s."
                % (dtype, name, suffix))
        if pa.types.is_string(scalar_type) and shape not in ((), (1,)):
            raise ValueError(
                "LeRobot string feature %s must be scalar." % name)
        arrow_type = _tensor_type(scalar_type, shape, legacy_vectors)
    description = "LeRobot dtype=%s, shape=%s, names=%s" % (
        dtype,
        json.dumps(list(shape), separators=(",", ":")),
        json.dumps(
            feature.get("names"),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ),
    )
    return pa.field(
        name,
        arrow_type,
        nullable=name == "subtask_index" or bool(feature.get("nullable", False)),
        metadata={b"description": description.encode("utf-8")},
    )


def _feature_shape(feature, name):
    shape = feature.get("shape", ())
    if shape is None:
        shape = ()
    if not isinstance(shape, (list, tuple)):
        raise ValueError(
            "LeRobot feature %s has an invalid shape: %r" % (name, shape))
    try:
        result = tuple(int(size) for size in shape)
    except (TypeError, ValueError) as error:
        raise ValueError(
            "LeRobot feature %s has an invalid shape: %r"
            % (name, shape)) from error
    if any(size <= 0 for size in result):
        raise ValueError(
            "LeRobot feature %s has an invalid shape: %r" % (name, shape))
    return result


def _tensor_type(scalar_type, shape, legacy_vectors=False):
    if shape in ((), (1,)):
        return scalar_type
    result = pa.list_(scalar_type, shape[-1]) if legacy_vectors else scalar_type
    for unused_size in (shape[:-1] if legacy_vectors else shape):
        result = pa.list_(result)
    return result


def _merge_schemas(schemas, schema_evolution=True, required=()):
    """Merge top-level columns without promoting types or changing nested schemas."""
    fields = {}
    first_names = set(schemas[0].names)
    for ordinal, schema in enumerate(schemas):
        missing = set(required) - set(schema.names)
        if missing:
            raise ValueError("LeRobot source %d is missing required fields: %s."
                             % (ordinal, sorted(missing)))
        for field in schema:
            if not schema_evolution and field.name not in first_names:
                raise ValueError("LeRobot source %d adds field %s with schema_evolution disabled."
                                 % (ordinal, field.name))
            if field.name in fields and not fields[field.name].type.equals(field.type):
                raise ValueError("LeRobot source %d has incompatible type for field %s: %s versus %s."
                                 % (ordinal, field.name, field.type, fields[field.name].type))
            fields.setdefault(field.name, field)
    return pa.schema([
        field.with_nullable(any(name not in schema.names or schema.field(name).nullable
                                for schema in schemas))
        for name, field in fields.items()
    ], metadata=schemas[0].metadata)


def _plan_schema_evolution(target_schema, incoming_schema, schema_evolution,
                           required=(), video_fields=()):
    """Validate an append and plan nullable column additions without changing types."""
    # Compare the storage representation, including Arrow aliases for STRING/ARRAY.
    incoming_schema = PyarrowFieldParser.from_paimon_schema(
        PyarrowFieldParser.to_paimon_schema(incoming_schema))
    merged = _merge_schemas([target_schema, incoming_schema], schema_evolution, required)
    changes = []
    for field in merged:
        if field.name not in target_schema.names:
            comment = _description(field) or None
            if field.name in video_fields:
                comment = VIDEO_FRAME_FIELD_DIRECTIVE + ("; " + comment if comment else "")
            changes.append(SchemaChange.add_column(
                field.name, PyarrowFieldParser.to_paimon_type(field.type, True),
                comment=comment))
        elif field.name not in required and field.nullable \
                and not target_schema.field(field.name).nullable:
            changes.append(SchemaChange.update_column_nullability(field.name, True))
    return changes
