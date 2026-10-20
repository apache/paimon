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

"""Unused CatalogContext attributes must not select Python vector search."""

from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from pypaimon.catalog.catalog_context import CatalogContext
from pypaimon.catalog.rest.rest_catalog_loader import RESTCatalogLoader
from pypaimon.common.predicate import Predicate
from pypaimon.table.source import native_vector_search


@pytest.mark.parametrize('option', ['hadoop_conf', 'prefer_io_loader', 'fallback_io_loader'])
@pytest.mark.parametrize('batch', [False, True], ids=['single', 'batch'])
def test_unused_context_attributes_preserve_native_vector_execution(option, batch):
    context = CatalogContext.create_from_options({'metastore': 'rest', 'uri': 'http://unused'})
    unused = Mock(side_effect=AssertionError('An unused IO loader was invoked'))
    setattr(context, option, {'unused': 'value'} if option == 'hadoop_conf' else unused)
    loader = RESTCatalogLoader(context)
    table = SimpleNamespace(
        options=SimpleNamespace(native_read_enabled=lambda: True, query_auth_enabled=False),
        catalog_environment=SimpleNamespace(catalog_loader=loader))
    builder = SimpleNamespace(
        _table=table, _vector_column=SimpleNamespace(name='embedding'), _limit=2,
        _options={'ivf-flat.metric': 'l2'}, _filter=Predicate('equal', 0, 'id', [7]),
        _partition_filter=Predicate('equal', 1, 'pt', [1]), _is_primary_key_vector_search=lambda: False)
    core = Mock()
    core.with_limit.return_value = core
    core.with_options.return_value = core
    native_table = Mock()
    native_table.new_vector_search_builder.return_value = core
    native_table.new_batch_vector_search_builder.return_value = core
    expected = [{7: -0.25, 9: -1.5}, {11: -2.0}]
    core.execute_local.return_value.row_ids.return_value = expected[0]
    core.execute_batch_local.return_value = [Mock(row_ids=Mock(return_value=scores)) for scores in expected]
    queries = [[1.0, 0.0], [0.0, 1.0]] if batch else [1.0, 0.0]
    with patch.object(loader, 'load', side_effect=AssertionError('Python catalog was reconstructed')), \
            patch.object(native_vector_search, 'native_runtime_available', return_value=True), \
            patch.object(native_vector_search, '_native_table', return_value=native_table) as resolve:
        result = native_vector_search.try_native_vector_search(builder, queries, batch=batch)
    assert result is not None
    resolve.assert_called_once_with(table)
    core.with_vector_column.assert_called_once_with('embedding')
    core.with_limit.assert_called_once_with(2)
    core.with_options.assert_called_once_with({'ivf-flat.metric': 'l2'})
    core.with_filter.assert_called_once_with({'method': 'equal', 'field': 'id', 'literals': [7]})
    core.with_partition_filter.assert_called_once_with({'method': 'equal', 'field': 'pt', 'literals': [1]})
    unused.assert_not_called()
    if batch:
        core.with_query_vectors.assert_called_once_with(queries)
        core.execute_batch_local.assert_called_once_with()
        core.execute_local.assert_not_called()
        results = result
    else:
        core.with_query_vector.assert_called_once_with(queries)
        core.execute_local.assert_called_once_with()
        core.execute_batch_local.assert_not_called()
        results = [result]
        expected = expected[:1]
    assert [{row_id: item.score_getter()(row_id) for row_id in item.results()} for item in results] == expected
