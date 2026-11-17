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

"""Vector parameter resolution shared by primary-key and Data Evolution indexes."""

from dataclasses import dataclass
from typing import Optional

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options


@dataclass(frozen=True)
class VectorIndexOptions:
    index_type: Optional[str]
    options: Options
    metric: str

    @staticmethod
    def columns(options, index_type=None):
        core = CoreOptions(options)
        if index_type is not None:
            declared = core.global_index_columns(index_type)
            return declared if declared else [column for column in VectorIndexOptions._legacy_columns(options)
                                              if VectorIndexOptions._legacy_index_type(options, column) == index_type]
        types = VectorIndexOptions._index_types(core)
        if types:
            return [column for kind in types for column in VectorIndexOptions.columns(options, kind)]
        return VectorIndexOptions._legacy_columns(options)

    @staticmethod
    def _legacy_columns(options):
        columns = options.to_map().get('pk-vector.index.columns')
        return [] if columns is None else [column.strip() for column in str(columns).split(',')]

    @staticmethod
    def columns_option_key(options):
        core = CoreOptions(options)
        types = VectorIndexOptions._index_types(core)
        if not types:
            return 'pk-vector.index.columns'
        return ', '.join(core.global_index_columns_option_key(kind) for kind in types)

    @staticmethod
    def _index_types(core):
        return [kind for kind in core.global_index_types() if CoreOptions._is_vector_index_type(kind)]

    @staticmethod
    def _legacy_index_type(options, column):
        value = options.to_map().get('fields.%s.pk-vector.index.type' % column)
        return None if value is None else str(value).lower().strip()

    @staticmethod
    def resolve(options, index_type, column, *, default_metric=True):
        values = dict(options.to_map())
        if index_type is None:
            core = CoreOptions(options)
            declared = [kind for kind in VectorIndexOptions._index_types(core)
                        if column in core.global_index_columns(kind)]
            index_type = ((declared[0] if len(declared) == 1 else None) if declared
                          else VectorIndexOptions._legacy_index_type(options, column))
            if index_type is None:
                return VectorIndexOptions(
                    None, Options(values),
                    _normalize_metric(values.get('fields.%s.pk-vector.distance.metric' % column)))
        prefix = index_type + '.'
        field_prefix = 'fields.%s.' % column
        aliases = (field_prefix + 'distance.metric', field_prefix + 'metric',
                   prefix + 'distance.metric', prefix + 'metric')
        metric = next((values[key] for key in (
            field_prefix + 'distance.metric', field_prefix + 'metric',
            field_prefix + 'pk-vector.distance.metric', prefix + 'distance.metric',
            prefix + 'metric', 'distance.metric', 'metric') if values.get(key) is not None), None)
        # Resolve aliases after generic merging, so COSINE/cosine cannot be
        # mistaken for conflicting generic strings when options are resolved twice.
        for key in aliases:
            values.pop(key, None)
        resolved = CoreOptions(Options(values)).global_index_options(index_type, column).to_map()
        metric = next((resolved[key] for key in aliases if resolved.get(key) is not None), metric)
        if metric is not None or default_metric:
            normalized = _normalize_metric(metric)
            for key in aliases:
                resolved[key] = normalized
        return VectorIndexOptions(index_type, Options(resolved), _normalize_metric(metric))


def _normalize_metric(metric):
    return str('inner_product' if metric is None else metric).lower().replace('-', '_')
