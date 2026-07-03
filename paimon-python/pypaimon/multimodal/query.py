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

from typing import Callable, Dict, List, Optional, Tuple

import pyarrow as pa

from pypaimon.common.where_parser import parse_where_clause
from pypaimon.table.special_fields import SpecialFields


class ScanQuery:
    """Chainable scan wrapper for MultimodalTable."""

    def __init__(
            self,
            table,
            result_factory: Optional[Callable] = None):
        self._table = table
        self._predicate = None
        self._projection = None
        self._limit = None
        self._include_row_id = False
        self._result_factory = result_factory

    def where(self, predicate):
        predicate = self._coerce_predicate(predicate, "where()")
        if predicate is not None:
            self._predicate = self._and_predicate(self._predicate, predicate)
        return self

    def select(self, columns):
        if isinstance(columns, str):
            columns = [columns]
        self._projection = list(columns)
        return self

    def with_row_id(self):
        self._include_row_id = True
        return self

    def limit(self, limit: int):
        self._limit = limit
        return self

    def to_arrow(self):
        if self._result_factory is not None:
            return self._read_global_index_result(self._result_factory(self))

        read_builder = self._configured_read_builder()
        scan = read_builder.new_scan()
        plan = scan.plan()
        return read_builder.new_read().to_arrow(plan.splits())

    def _configured_read_builder(self):
        read_builder = self._table.new_read_builder()
        if self._predicate is not None:
            read_builder = read_builder.with_filter(self._predicate)
        projection = self._effective_projection()
        if projection is not None:
            read_builder = read_builder.with_projection(projection)
        if self._limit is not None:
            read_builder = read_builder.with_limit(self._limit)
        return read_builder

    def _effective_projection(self):
        if self._projection is None:
            if not self._include_row_id:
                return None
            projection = [field.name for field in self._table.fields]
        else:
            projection = list(self._projection)
        if self._include_row_id and SpecialFields.ROW_ID.name not in projection:
            projection.append(SpecialFields.ROW_ID.name)
        return projection

    def _read_global_index_result(self, result):
        read_builder = self._configured_read_builder()
        scan = read_builder.new_scan().with_global_index_result(result)
        plan = scan.plan()
        return read_builder.new_read().to_arrow(plan.splits())

    def to_pandas(self):
        return self.to_arrow().to_pandas()

    def to_list(self) -> List[dict]:
        return self.to_arrow().to_pylist()

    def read_blobs(
            self, columns=None, *, parallelism: int = 64
    ) -> Tuple[pa.Table, Dict[str, List[Optional[bytes]]]]:
        """Materialise BLOB column(s) for the filtered rows with concurrent,
        coalesced ranged reads. Reads via blob-as-descriptor to skip the slow
        row-by-row blob resolution on multi-group data-evolution splits.

        ``columns`` picks the BLOB column(s) (default: all, intersected with
        ``select(...)``). Returns ``(scalar_arrow_table, {column: [bytes|None]})``,
        row-aligned. Use :meth:`stream_blobs` for a memory-bounded read.

        Unresolved blob-view columns are not supported and raise ``ValueError``.
        """
        blob_cols = self._resolve_blob_columns(columns)
        read_builder, file_io = self._blob_descriptor_read_builder(blob_cols)
        arrow = read_builder.new_read().to_arrow(
            read_builder.new_scan().plan().splits())
        bodies = self._fetch_bodies(
            file_io, arrow.select(blob_cols).to_pydict(), blob_cols, parallelism)
        scalar = arrow.select(self._scalar_columns(arrow.column_names))
        return scalar, bodies

    def stream_blobs(self, columns=None, *, parallelism: int = 64):
        """Memory-bounded streaming variant of :meth:`read_blobs`: yield
        ``(scalar_batch, {column: [bytes|None]})`` per Arrow batch, so peak memory
        is one batch rather than the whole result.
        """
        # Validate eagerly so a bad column raises here, not on the first next().
        blob_cols = self._resolve_blob_columns(columns)
        read_builder, file_io = self._blob_descriptor_read_builder(blob_cols)
        return self._iter_blobs(read_builder, file_io, blob_cols, parallelism)

    def _iter_blobs(self, read_builder, file_io, blob_cols, parallelism):
        reader = read_builder.new_read().to_arrow_batch_reader(
            read_builder.new_scan().plan().splits())
        try:
            for batch in reader:
                bodies = self._fetch_bodies(
                    file_io, batch.select(blob_cols).to_pydict(), blob_cols, parallelism)
                scalar = batch.select(self._scalar_columns(batch.schema.names))
                yield scalar, bodies
        finally:
            # Close the reader even if the caller breaks out early.
            if hasattr(reader, "close"):
                reader.close()

    def _blob_descriptor_read_builder(self, blob_cols: List[str]):
        """Blob-as-descriptor read builder with this query's filter/projection/limit;
        returns ``(read_builder, file_io)``."""
        from pypaimon.common.options.core_options import CoreOptions
        read_table = self._table.copy({
            CoreOptions.BLOB_AS_DESCRIPTOR.key(): "true"
        })
        read_builder = read_table.new_read_builder()
        if self._predicate is not None:
            read_builder = read_builder.with_filter(self._predicate)
        projection = self._blob_read_projection(blob_cols)
        if projection is not None:
            read_builder = read_builder.with_projection(projection)
        if self._limit is not None:
            read_builder = read_builder.with_limit(self._limit)
        return read_builder, read_table.file_io

    @staticmethod
    def _fetch_bodies(file_io, data, blob_cols, parallelism):
        # Decode each descriptor to a (uri, offset, length) range and read them all in
        # one coalesced pass on ``file_io`` -- the read table's FileIO, which already
        # carries the merged DLF/OSS token. Going through Blob.from_bytes here would
        # build a BlobRef whose uri_reader re-resolves the URI via
        # ``FileIO.get(uri, catalog_options)`` off the raw options (no merged token),
        # failing with "endpoint should be non-empty" / "Init credential failed" unless
        # the caller also passes fs.oss.* -- which users should not have to.
        from pypaimon.table.row.blob import BlobDescriptor, BlobViewStruct
        ranges = []
        inline = {}  # cell index -> blob stored inline (returned as-is, no ranged read)
        i = 0
        for col in blob_cols:
            for value in data[col]:
                if value is None:
                    ranges.append(None)
                else:
                    raw = bytes(value)
                    if BlobViewStruct.is_blob_view_struct(raw):
                        raise ValueError(
                            "read_blobs does not support unresolved blob-view columns; "
                            "read such a column on its own, or enable blob-view resolution.")
                    if BlobDescriptor.is_blob_descriptor(raw):
                        d = BlobDescriptor.deserialize(raw)
                        ranges.append((d.uri, d.offset, d.length))
                    else:  # blob stored inline: the bytes are the payload
                        ranges.append(None)
                        inline[i] = raw
                i += 1
        fetched = file_io.read_ranges_coalesced(ranges, parallelism)
        for idx, raw in inline.items():
            fetched[idx] = raw
        bodies = {}
        offset = 0
        for col in blob_cols:
            n = len(data[col])
            bodies[col] = fetched[offset:offset + n]
            offset += n
        return bodies

    def _all_blob_columns(self) -> List[str]:
        return [
            field.name for field in self._table.fields
            if getattr(field.type, "type", None) == "BLOB"
        ]

    def _resolve_blob_columns(self, columns) -> List[str]:
        all_blob = self._all_blob_columns()
        if columns is None:
            selected = all_blob
            if self._projection is not None:
                selected = [c for c in all_blob if c in self._projection]
            if not selected:
                raise ValueError("No BLOB column to read; pass columns=.")
            return selected
        if isinstance(columns, str):
            columns = [columns]
        columns = list(dict.fromkeys(columns))  # de-dup, keep order
        for name in columns:
            if name not in all_blob:
                raise ValueError(f"Column {name!r} is not a BLOB column.")
        return columns

    def _blob_read_projection(self, blob_cols: List[str]) -> List[str]:
        # Base on _effective_projection() (honours with_row_id), drop BLOB
        # descriptors, then append the requested BLOB columns and every predicate
        # column -- including predicate columns that are themselves BLOB (read as
        # descriptor) -- so SplitRead keeps the row-level filter for where().
        blob_set = set(self._all_blob_columns())
        effective = self._effective_projection()
        if effective is None:
            base = [f.name for f in self._table.fields if f.name not in blob_set]
        else:
            base = [name for name in effective if name not in blob_set]
        for name in self._predicate_fields():
            if name not in base and name not in blob_cols:
                base.append(name)
        return base + list(blob_cols)

    def _scalar_columns(self, available) -> List[str]:
        # Non-BLOB columns to expose, based on _effective_projection() so
        # with_row_id() is honoured; drop BLOBs and predicate-only helpers, and
        # skip unknown projected names to match to_arrow()'s silent drop.
        blob_set = set(self._all_blob_columns())
        effective = self._effective_projection()
        if effective is None:
            return [name for name in available if name not in blob_set]
        available = set(available)
        return [name for name in effective
                if name in available and name not in blob_set]

    def _predicate_fields(self):
        if self._predicate is None:
            return ()
        from pypaimon.read.push_down_utils import _get_all_fields
        return _get_all_fields(self._predicate)

    def _coerce_predicate(self, predicate, method):
        if predicate is None:
            return None
        if isinstance(predicate, str):
            return parse_where_clause(predicate, self._table.fields)
        raise ValueError("%s expects a SQL-like string." % method)

    def _and_predicate(self, left, right):
        if left is None:
            return right
        from pypaimon.common.predicate_builder import PredicateBuilder
        return PredicateBuilder.and_predicates([left, right])


class _PreFilterQuery(ScanQuery):

    def __init__(
            self,
            table,
            result_factory: Optional[Callable] = None,
            pre_filter=None):
        self._pre_filter = None
        super().__init__(table, result_factory=result_factory)
        if pre_filter is not None:
            self.pre_filter(pre_filter)

    def pre_filter(self, predicate):
        predicate = self._coerce_predicate(predicate, "pre_filter()")
        if predicate is not None:
            self._pre_filter = self._and_predicate(self._pre_filter, predicate)
        return self

    # Blob reads bypass the search result_factory, so reject them (else a plain scan).
    def read_blobs(self, *args, **kwargs):
        raise TypeError("read_blobs is only supported on scan(), not search queries.")

    def stream_blobs(self, *args, **kwargs):
        raise TypeError("stream_blobs is only supported on scan(), not search queries.")


class VectorQuery(_PreFilterQuery):
    """Chainable query wrapper for vector global-index search."""

    def __init__(
            self,
            table,
            vector,
            vector_column,
            vector_options=None,
            pre_filter=None):
        self._vector = vector
        self._vector_column = vector_column
        self._vector_options = dict(vector_options or {})
        super().__init__(
            table, result_factory=self._execute_vector, pre_filter=pre_filter)

    def _execute_vector(self, query):
        limit = query._limit if query._limit is not None else 10
        builder = (
            self._table.new_vector_search_builder()
            .with_vector_column(self._vector_column)
            .with_query_vector(self._vector)
            .with_limit(limit)
            .with_options(self._vector_options)
        )
        if query._pre_filter is not None:
            builder = builder.with_filter(query._pre_filter)
        return builder.execute_local()


class TextQuery(_PreFilterQuery):
    """Chainable query wrapper for full-text global-index search."""

    def __init__(self, table, text_query, pre_filter=None):
        self._text_query = text_query
        super().__init__(
            table, result_factory=self._execute_fts, pre_filter=pre_filter)

    def _execute_fts(self, query):
        limit = query._limit if query._limit is not None else 10
        builder = (
            self._table.new_full_text_search_builder()
            .with_query(self._text_query)
            .with_limit(limit)
        )
        if query._pre_filter is not None:
            builder = builder.with_partition_filter(query._pre_filter)
        return builder.execute_local()


class HybridQuery(_PreFilterQuery):
    """Chainable query wrapper for hybrid global-index search."""

    def __init__(
            self,
            table,
            vector_routes=None,
            text_routes=None,
            ranker="rrf",
            route_limit=None,
            pre_filter=None):
        self._vector_routes = list(vector_routes or [])
        self._text_routes = list(text_routes or [])
        self._ranker = ranker
        self._route_limit = route_limit
        super().__init__(
            table, result_factory=self._execute_hybrid, pre_filter=pre_filter)

    def rerank(self, ranker):
        self._ranker = ranker
        return self

    def _execute_hybrid(self, query):
        final_limit = query._limit if query._limit is not None else 10
        route_limit = self._route_limit or final_limit
        builder = (
            self._table.new_hybrid_search_builder()
            .with_limit(final_limit)
            .with_ranker(self._ranker)
        )
        for route in self._vector_routes:
            builder = builder.add_vector_route(
                route["column"],
                route["vector"],
                limit=route.get("limit") or route_limit,
                weight=route["weight"],
                options=route["options"],
            )
        for route in self._text_routes:
            builder = builder.add_full_text_route(
                route["query"].to_json(),
                limit=route.get("limit") or route_limit,
                weight=route["weight"],
                options=route["options"],
            )
        if query._pre_filter is not None:
            builder = builder.with_filter(query._pre_filter)
        return builder.execute_local()


class BatchVectorQuery(_PreFilterQuery):
    """Chainable query wrapper for batch vector global-index search."""

    def __init__(
            self,
            table,
            vectors,
            vector_column,
            vector_options=None,
            pre_filter=None):
        self._vectors = vectors
        self._vector_column = vector_column
        self._vector_options = dict(vector_options or {})
        super().__init__(table, pre_filter=pre_filter)

    def to_arrow(self):
        return [
            self._read_global_index_result(result)
            for result in self._execute_batch_vector(self)
        ]

    def to_pandas(self):
        return [table.to_pandas() for table in self.to_arrow()]

    def to_list(self) -> List[List[dict]]:
        return [table.to_pylist() for table in self.to_arrow()]

    def _execute_batch_vector(self, query):
        limit = query._limit if query._limit is not None else 10
        builder = (
            self._table.new_batch_vector_search_builder()
            .with_vector_column(self._vector_column)
            .with_query_vectors(self._vectors)
            .with_limit(limit)
            .with_options(self._vector_options)
        )
        if query._pre_filter is not None:
            builder = builder.with_filter(query._pre_filter)
        return builder.execute_batch_local()
