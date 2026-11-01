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

"""Transport resolved index configuration through the existing native schema contract."""

import json
from collections.abc import Mapping

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.options.options import Options
from pypaimon.common.options.vector_index_options import VectorIndexOptions


def native_index_options(schema):
    return _resolve(schema)[0]


def native_search_options(options):
    """Do not let an older SDK interpret removed external type hints."""
    return {key: value for key, value in options.items()
            if key not in ('index_type', 'index-type', 'vector.index-type')
            and not (key.startswith('fields.') and key.endswith('.index-type'))}


def configure_vector_index_type(native, options, column):
    """Use the required native API and propagate missing-method errors."""
    if column is None:
        return
    index_type = VectorIndexOptions.index_type(Options(options), column)
    if index_type is not None:
        native.with_vector_index_type(column, index_type)


def native_scalar_index_read_supported(table):
    if not table.options.global_index_enabled():
        return True
    return _resolve(table.table_schema)[1]


def _resolve(schema):
    if not isinstance(getattr(schema, 'options', None), Mapping):
        return {}, True
    values = native_search_options(schema.options)
    core = CoreOptions(Options(values))
    result = dict(values)
    declarations = core.global_index_types()
    # Native's older shared declaration keys enable automatic maintenance. Python's
    # declarations select implementations and keep the explicit DE build lifecycle.
    for key in list(result):
        if key.startswith('global-index.') and key.endswith(('.index-columns', '.index-column')):
            del result[key]
    primary_key = bool(schema.primary_keys)
    families = ['btree', 'bitmap', 'multivalue', 'fm', 'full-text']
    definitions = [(family, core.global_index_columns(family)) for family in families]
    definitions += [(kind, core.global_index_columns(kind)) for kind in declarations
                    if kind not in families]
    if primary_key and not any(kind not in families for kind in declarations):
        definitions += [(VectorIndexOptions.index_type(core.options, column), [column])
                        for column in core.primary_key_vector_index_columns()]
    known_columns = set()
    native_columns = {}
    profiles = {}
    compatible = True
    for kind, columns in definitions:
        if kind is None:
            continue
        vector = kind not in families
        family = 'vector' if vector else kind
        prefix = kind + ('.' if vector or kind == 'full-text' else '-index.')
        if primary_key and columns:
            native_columns.setdefault(family, []).extend(columns)
            result['pk-%s.index.columns' % family] = ','.join(native_columns[family])
        for column in columns:
            known_columns.add(column)
            resolved = (VectorIndexOptions.resolve(core.options, kind, column, default_metric=False).to_map()
                        if vector else core.global_index_options(kind, column).to_map())
            resolved = native_search_options(resolved)
            profile = {key: value for key, value in resolved.items()
                       if key.startswith(prefix) and value is not None}
            field_prefix = 'fields.%s.' % column
            if primary_key:
                if vector:
                    result[field_prefix + 'pk-vector.index.type'] = kind
                    result[field_prefix + 'pk-vector.distance.metric'] = VectorIndexOptions.metric(
                        core.options, kind, column)
                    profile.update({key: value for key, value in resolved.items()
                                    if key.startswith(field_prefix) and not key.startswith(field_prefix + 'pk-')
                                    and key != field_prefix + 'index-options' and value is not None})
                if vector or kind == 'full-text':
                    # Native ranked readers consume flat options, unlike scalar definitions.
                    result.update(profile)
                result[field_prefix + 'pk-%s.index.options' % family] = json.dumps(profile)
                result.pop(field_prefix + 'index-options', None)
            elif vector:
                result.update({key: value for key, value in resolved.items()
                               if key.startswith(field_prefix) and key != field_prefix + 'index-options'})
            elif kind != 'full-text':
                # Native scalar scanners accept one set of read options per family.
                if kind in profiles and profiles[kind] != profile:
                    compatible = False
                profiles[kind] = profile
        if primary_key and columns and not vector and kind != 'full-text':
            for key in list(result):
                if key.startswith(prefix):
                    del result[key]
    if not primary_key:
        for profile in profiles.values():
            result.update(profile)
        for key in values:
            if key.startswith('fields.') and key.endswith('.index-options'):
                column = key[len('fields.'):-len('.index-options')]
                if column not in known_columns:
                    compatible = False
    return result, compatible
