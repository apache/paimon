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

"""Batch index lookups driven by a Ray Dataset at one Paimon snapshot."""

from contextlib import closing

import pyarrow as pa
import pyarrow.compute as pc

from pypaimon.common.options.core_options import GlobalIndexSearchMode
from pypaimon.common.predicate import Predicate
from pypaimon.common.predicate_builder import PredicateBuilder
from pypaimon.execution.python.globalindex.data_evolution_global_index_scanner import (
    DataEvolutionGlobalIndexScanner,
)
from pypaimon.execution.python.globalindex.index_freshness import key_indexes_are_fresh
from pypaimon.schema.data_types import PyarrowFieldParser
from pypaimon.snapshot.time_travel_util import TimeTravelUtil


def _prepare_lookup(table, key):
    if (table.is_primary_key_table or not table.options.data_evolution_enabled()
            or not table.options.row_tracking_enabled()):
        raise ValueError("indexed_lookup_join requires a data-evolution table with row tracking")
    if table.options.query_auth_enabled:
        raise ValueError("indexed_lookup_join does not support query authorization; use an ordinary Ray join")
    if key not in table.field_dict:
        raise ValueError("Lookup key {!r} is not a table column".format(key))
    dtype = PyarrowFieldParser.from_paimon_schema(table.fields).field(key).type
    if not (pa.types.is_signed_integer(dtype) or pa.types.is_string(dtype)):
        raise ValueError("indexed_lookup_join supports only integer or string keys")
    if not table.options.global_index_enabled():
        raise ValueError("Global indexes are disabled; use an ordinary Ray join")
    snapshot = TimeTravelUtil.resolve_snapshot(table)
    if snapshot is None:
        raise ValueError("A complete single-column BTree or Bitmap index is required; use an ordinary Ray join")
    table = table._copy_with_snapshot(snapshot)
    # A past writer may have used IGNORE without persisting it in the table options.
    freshness_view = table.copy_without_time_travel({"global-index.column-update-action": "IGNORE"})
    if not key_indexes_are_fresh(freshness_view, snapshot, [key], None):
        raise ValueError("Lookup key index is stale; rebuild it or use an ordinary Ray join")
    predicate = table.new_read_builder().new_predicate_builder().is_not_null(key)
    scanner = DataEvolutionGlobalIndexScanner.create(table, predicate=predicate, snapshot=snapshot)
    if scanner is None:
        raise ValueError("A complete single-column BTree or Bitmap index is required; use an ordinary Ray join")
    with scanner:
        if scanner.unindexed_ranges(predicate, search_mode=GlobalIndexSearchMode.DETAIL):
            raise ValueError("Lookup index coverage is incomplete; rebuild it or use an ordinary Ray join")
    return table


def _output_schema(source_schema, table, source_on, lookup_on, projection):
    if len(set(source_schema.names)) != len(source_schema.names):
        raise ValueError("Source column names must be unique")
    lookup_schema = PyarrowFieldParser.from_paimon_schema(table.fields)
    if source_on not in source_schema.names:
        raise ValueError("Source key {!r} is not a column".format(source_on))
    if source_schema.field(source_on).type != lookup_schema.field(lookup_on).type:
        raise ValueError("Source and lookup key types must match exactly")
    if (not projection or len(set(projection)) != len(projection)
            or lookup_on not in projection or not set(projection).issubset(lookup_schema.names)):
        raise ValueError("lookup_projection must contain unique table columns including the lookup key")
    payload = [name for name in projection if name != lookup_on]
    if set(payload).intersection(source_schema.names):
        raise ValueError("Source and lookup non-key column names must not collide")
    return pa.schema(list(source_schema) + [lookup_schema.field(name) for name in payload])


class _LookupBatch:
    """One actor owns a fixed read view and its lazy scalar-index reader cache."""

    def __init__(self, table, source_schema, source_on, lookup_on, projection,
                 lookup_filter, output_batch_size):
        self.table = table
        self.source_schema = source_schema
        self.source_on = source_on
        self.lookup_on = lookup_on
        self.projection = projection
        self.lookup_filter = lookup_filter
        self.output_batch_size = output_batch_size
        self.output_schema = _output_schema(source_schema, table, source_on, lookup_on, projection)
        self.predicates = table.new_read_builder().new_predicate_builder()
        self.scanner = DataEvolutionGlobalIndexScanner.create(
            table, predicate=self.predicates.is_not_null(lookup_on), snapshot=table._read_snapshot)

    def __call__(self, batch):
        if batch.schema != self.source_schema:
            raise ValueError("Source schema or key type changed during indexed_lookup_join")
        keys = pc.unique(pc.drop_null(batch[self.source_on])).to_pylist()
        if not keys:
            yield self.output_schema.empty_table()
            return
        key_filter = self.predicates.is_in(self.lookup_on, keys)
        result = self.scanner.scan(key_filter)
        if result is None or not result.is_exact():
            raise ValueError("Lookup index cannot evaluate these keys exactly; use an ordinary Ray join")
        predicate = (key_filter if self.lookup_filter is None else
                     PredicateBuilder.and_predicates([key_filter, self.lookup_filter]))
        builder = self.table.new_read_builder().with_filter(predicate).with_projection(self.projection)
        # Only the proven key index selects candidates; other filters are checked on data.
        splits = builder.new_scan().with_global_index_result(result).plan().splits()
        left = pa.table({"key": batch[self.source_on], "source_row": pa.array(range(batch.num_rows))})
        with closing(builder.new_read().to_arrow_batch_reader(splits)) as reader:
            for record_batch in reader:
                lookup = pa.Table.from_batches([record_batch])
                right = pa.table({"key": lookup[self.lookup_on],
                                  "lookup_row": pa.array(range(lookup.num_rows))})
                pairs = left.join(right, keys="key", join_type="inner")
                # Join row positions so nested payload columns need no Arrow join support.
                for start in range(0, pairs.num_rows, self.output_batch_size):
                    positions = pairs.slice(start, self.output_batch_size)
                    source_rows = batch.take(positions["source_row"])
                    lookup_rows = lookup.drop([self.lookup_on]).take(positions["lookup_row"])
                    yield pa.Table.from_arrays(source_rows.columns + lookup_rows.columns,
                                               schema=self.output_schema)

    def close(self):
        if self.scanner is not None:
            self.scanner.close()
            self.scanner = None

    def __del__(self):
        if getattr(self, "scanner", None) is not None:
            self.close()


def indexed_lookup_join(source, lookup_table, catalog_options, *, source_on, lookup_on,
                        lookup_projection=None, lookup_filter=None, batch_size=1024,
                        concurrency=1, output_batch_size=4096):
    """Inner-join a Ray Dataset to a completely indexed data-evolution table.

    Both keys must be single integer or string columns of the same type. The
    result retains source columns and lookup non-key columns, with SQL equality
    semantics: duplicate matches multiply and NULL keys do not match. Column
    names must not collide. ``lookup_filter`` is a Paimon Predicate evaluated
    together with the lookup keys; projections must retain the lookup key.

    The lookup snapshot is fixed when this function is called. Missing, stale,
    disabled or incomplete indexes raise instead of repeatedly scanning an
    unindexed tail. Use a normal Ray join for those tables. ``batch_size`` bounds
    source rows per lookup; ``output_batch_size`` bounds emitted batches, but
    high-fanout keys can still require substantial memory within a worker.
    """
    from pypaimon.catalog.catalog_factory import CatalogFactory
    from pypaimon.ray.ray_paimon import _require_ray_data

    ray_data = _require_ray_data()
    if not isinstance(source, ray_data.Dataset):
        raise TypeError("source must be a Ray Dataset")
    for name, value in (("batch_size", batch_size), ("concurrency", concurrency),
                        ("output_batch_size", output_batch_size)):
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError("{} must be a positive integer".format(name))
    if not isinstance(source_on, str) or not isinstance(lookup_on, str):
        raise ValueError("source_on and lookup_on must each name one column")
    if lookup_filter is not None and not isinstance(lookup_filter, Predicate):
        raise TypeError("lookup_filter must be a Paimon Predicate")
    table = _prepare_lookup(CatalogFactory.create(catalog_options).get_table(lookup_table), lookup_on)
    projection = table.field_names if lookup_projection is None else list(lookup_projection)
    source_schema = source.schema()
    if source_schema is None or not isinstance(source_schema.base_schema, pa.Schema):
        raise ValueError("source must have an Arrow schema, including when empty")
    source_schema = source_schema.base_schema
    schema = _output_schema(source_schema, table, source_on, lookup_on, projection)
    result = source.map_batches(
        _LookupBatch, batch_format="pyarrow", batch_size=batch_size, concurrency=concurrency,
        fn_constructor_args=(table, source_schema, source_on, lookup_on, projection,
                             lookup_filter, output_batch_size))
    # Ray may skip map_batches entirely for an empty source; retain its join schema.
    return result.union(ray_data.from_arrow(schema.empty_table()))
