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

"""Vindex identifiers and Java-compatible option normalization."""

import math
from typing import Dict, Mapping, Optional

from pypaimon.schema.data_types import ArrayType, AtomicType, DataType, VectorType

VINDEX_IDENTIFIERS = ("ivf-flat", "ivf-pq", "ivf-sq", "ivf-rq", "diskann")
NPROBE_PARAMETER = "ivf.nprobe"
L_SEARCH_PARAMETER = "diskann.l_search"
READ_PARALLELISM_PARAMETER = "vindex.read.parallelism"
DEFAULT_DIMENSION = 128


def native_options(
    data_type: DataType,
    table_options: Mapping[str, object],
    index_type: str,
    field_name: str,
    user_options: Optional[Mapping[str, object]] = None,
) -> Dict[str, str]:
    if user_options is None:
        user_options, table_options = table_options, {}

    result: Dict[str, str] = {}
    _collect_native_options(
        result, table_options, index_type, field_name, validate=False)
    _collect_native_options(
        result, user_options, index_type, field_name, validate=True)

    result["index.type"] = index_type.replace('-', '_')
    result["dimension"] = str(_dimension(data_type, result, index_type))
    result.setdefault("metric", "inner_product")
    return result


def train_sample_ratio(
    table_options: Mapping[str, object],
    index_type: str,
    field_name: str,
    user_options: Optional[Mapping[str, object]] = None,
) -> float:
    sources = (table_options,) if user_options is None else (user_options, table_options)
    field_key = "fields.%s.train.sample-ratio" % field_name
    index_key = "%s.train.sample-ratio" % index_type
    for options in sources:
        key = field_key if field_key in options else index_key
        if key in options:
            break
    else:
        return 1.0

    value = options[key]
    try:
        ratio = float(value)
    except (TypeError, ValueError) as e:
        raise ValueError(
            "Invalid value for '%s': %s. Must be greater than 0 and less "
            "than or equal to 1." % (key, value)
        ) from e
    if not math.isfinite(ratio) or ratio <= 0 or ratio > 1:
        raise ValueError(
            "Invalid value for '%s': %s. Must be greater than 0 and less "
            "than or equal to 1." % (key, value)
        )
    return ratio


def validate_vector_type(data_type: DataType) -> None:
    if isinstance(data_type, VectorType):
        element_type = data_type.element
        if _is_float_type(element_type):
            return
        raise ValueError(
            "Vector index requires float vector, but got: %s" % element_type)

    if isinstance(data_type, ArrayType):
        element_type = data_type.element
        if _is_float_type(element_type):
            return
        raise ValueError(
            "Vector index requires float array, but got: %s" % element_type)

    raise ValueError(
        "Vector index requires VectorType or ArrayType<FLOAT>, but got: %s"
        % data_type)


def _collect_native_options(
    result, options, index_type, field_name, validate
):
    option_prefix = "%s." % index_type
    field_prefix = "fields.%s." % field_name

    for key, value in options.items():
        key = str(key)
        native_key = _native_option_key(key)
        if key == native_key and _is_050_build_option(native_key):
            _put_native_option(
                result, key, key, value, index_type, validate)
    for key, value in options.items():
        key = str(key)
        if key.startswith(option_prefix):
            _put_native_option(
                result,
                key,
                key[len(option_prefix):],
                value,
                index_type,
                validate,
            )
    for key, value in options.items():
        key = str(key)
        if key.startswith(field_prefix):
            _put_native_option(
                result,
                key,
                key[len(field_prefix):],
                value,
                index_type,
                validate,
            )


def _native_option_key(option_key: str) -> Optional[str]:
    if option_key in ("index.dimension", "dimension"):
        return "dimension"
    if option_key in ("distance.metric", "metric"):
        return "metric"
    if option_key in (
        "nlist",
        "expected-vector-count",
        "ivf.coarse-assignment",
        "ivf.pq-encoding",
        "ivf.train.max-points-per-centroid",
        "pq.train.max-points-per-centroid",
        "pq.m",
        "pq.code-ratio",
        "pq.bits",
        "rq.bits",
        "target-recall",
        "max-bytes-per-vector",
        "deployment-profile",
    ):
        return option_key
    if option_key in ("pq.use-opq", "use-opq"):
        return "use-opq"
    diskann_options = {
        "build-preset": "diskann.build-preset",
        "diskann.build-preset": "diskann.build-preset",
        "max-degree": "diskann.max-degree",
        "diskann.max-degree": "diskann.max-degree",
        "build-search-list-size": "diskann.build-search-list-size",
        "diskann.build-search-list-size": "diskann.build-search-list-size",
        "alpha": "diskann.alpha",
        "diskann.alpha": "diskann.alpha",
        "seed": "diskann.seed",
        "diskann.seed": "diskann.seed",
        "memory-budget-bytes": "diskann.memory-budget-bytes",
        "diskann.memory-budget-bytes": "diskann.memory-budget-bytes",
        "storage-layout": "diskann.storage-layout",
        "diskann.storage-layout": "diskann.storage-layout",
        "raw-vector-encoding": "diskann.raw-vector-encoding",
        "diskann.raw-vector-encoding": "diskann.raw-vector-encoding",
        "build-distance": "diskann.build-distance",
        "diskann.build-distance": "diskann.build-distance",
    }
    if option_key in diskann_options:
        return diskann_options[option_key]
    return None


def _put_native_option(
    result, option_key, option_suffix, value, index_type, validate
):
    native_key = _native_option_key(option_suffix)
    if native_key is None:
        return
    if (_is_050_build_option(native_key)
            and not _is_allowed_050_build_option(native_key, index_type)):
        if validate:
            raise ValueError(
                "Option '%s' is not supported for index type '%s'."
                % (option_key, index_type)
            )
        return
    result[native_key] = str(value)


def _is_050_build_option(key):
    return key in (
        "ivf.coarse-assignment",
        "ivf.pq-encoding",
        "ivf.train.max-points-per-centroid",
        "pq.train.max-points-per-centroid",
    )


def _is_allowed_050_build_option(key, index_type):
    if key == "ivf.pq-encoding":
        return index_type == "ivf-pq"
    if key == "pq.train.max-points-per-centroid":
        return index_type in ("ivf-pq", "diskann")
    return index_type != "diskann"


def _dimension(
    data_type: DataType, native_options_map: Mapping[str, str], index_type: str
) -> int:
    if isinstance(data_type, VectorType):
        return data_type.length

    dimension = native_options_map.get("dimension")
    value = DEFAULT_DIMENSION if dimension is None else int(dimension)
    if value <= 0:
        raise ValueError(
            "Invalid value for '%s.dimension': %s. Must be a positive integer."
            % (index_type, value)
        )
    return value


def _is_float_type(data_type: DataType) -> bool:
    return (
        isinstance(data_type, AtomicType)
        and data_type.type.upper() == "FLOAT"
    )
