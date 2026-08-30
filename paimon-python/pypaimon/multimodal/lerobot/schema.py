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

import pyarrow as pa


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


def _require_v3(info, source):
    version = str(info.get("codebase_version", ""))
    if not (version == "v3" or version.startswith("v3.")):
        raise ValueError(
            "load_from_lerobot supports LeRobot Dataset v3 only; %s reports "
            "codebase_version=%r. Upgrade the dataset to v3 first."
            % (source, version or None))


def _schema_from_info(info, include_task):
    features = info.get("features")
    if not isinstance(features, dict) or not features:
        raise ValueError("LeRobot metadata features must be a non-empty object.")

    fields = []
    for name, feature in features.items():
        fields.append(_feature_field(name, feature))
    if include_task:
        fields.append(pa.field(
            "task",
            pa.string(),
            nullable=False,
            metadata={b"description": b"LeRobot task"},
        ))
    return pa.schema(fields)


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


def _feature_field(name, feature):
    if not isinstance(feature, dict):
        raise ValueError(
            "LeRobot feature %s metadata must be an object." % name)
    dtype = str(feature.get("dtype", ""))
    shape = _feature_shape(feature, name)
    if dtype == "video":
        raise ValueError(
            "LeRobot video feature %s is not supported yet; use an "
            "image-based dataset." % name)
    if dtype == "image":
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
        arrow_type = _tensor_type(scalar_type, shape)
    description = "LeRobot dtype=%s, shape=%s" % (dtype, list(shape))
    return pa.field(
        name,
        arrow_type,
        nullable=False,
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


def _tensor_type(scalar_type, shape):
    if shape in ((), (1,)):
        return scalar_type
    if len(shape) == 1:
        return pa.list_(scalar_type, shape[0])
    result = pa.list_(scalar_type, shape[-1])
    for unused_size in reversed(shape[1:-1]):
        result = pa.list_(result)
    return pa.list_(result)
