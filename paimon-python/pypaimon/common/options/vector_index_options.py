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

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options


class VectorIndexOptions:
    @staticmethod
    def index_type(options, column):
        core = CoreOptions(options)
        declared = [kind for kind in core.global_index_types()
                    if CoreOptions._is_vector_index_type(kind) and column in core.global_index_columns(kind)]
        if declared:
            return declared[0] if len(declared) == 1 else None
        for key in ('fields.%s.pk-vector.index.type' % column, 'index_type', 'index-type',
                    'vector.index-type', 'fields.%s.index-type' % column):
            value = options.to_map().get(key)
            if value is not None:
                return str(value).lower().strip()
        return None

    @staticmethod
    def resolve(options, index_type, column, *, default_metric=True):
        values = dict(options.to_map())
        if index_type is None:
            index_type = VectorIndexOptions.index_type(options, column)
            if index_type is None:
                return Options(values)
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
        return Options(resolved)

    @staticmethod
    def metric(options, index_type, column):
        if index_type is None:
            index_type = VectorIndexOptions.index_type(options, column)
        if index_type is None:
            return _normalize_metric(options.to_map().get('fields.%s.pk-vector.distance.metric' % column))
        return VectorIndexOptions.resolve(options, index_type, column).to_map()[index_type + '.metric']


def _normalize_metric(metric):
    return str('inner_product' if metric is None else metric).lower().replace('-', '_')
