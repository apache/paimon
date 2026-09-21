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

import json
import re
from dataclasses import dataclass
from typing import Dict, Mapping, Optional, Sequence

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.multimodal.query import (
    BatchVectorQuery,
    HybridQuery,
    ScanQuery,
    TextQuery,
    VectorQuery,
)
from pypaimon.schema.data_types import PyarrowFieldParser, is_blob_type
from pypaimon.table.data_evolution_merge_into import (
    WhenMatched,
    WhenNotMatched,
    source_col as _source_col,
)
from pypaimon.table.special_fields import SpecialFields


_ALL_SOURCE_COLUMNS = object()
_MERGE_REF_PATTERN = re.compile(r'\b(source|target)\.(\w+)\b')
_STRING_LITERAL_PATTERN = re.compile(r"'(?:[^']|'')*'")


@dataclass(frozen=True)
class VectorRoute:
    """Vector route spec for hybrid search."""

    column: Optional[str]
    vector: object
    weight: float = 1.0
    limit: Optional[int] = None
    options: Optional[Dict[str, str]] = None


@dataclass(frozen=True)
class TextRoute:
    """Full-text route spec for hybrid search."""

    query: object
    column: Optional[str] = None
    weight: float = 1.0
    limit: Optional[int] = None
    options: Optional[Dict[str, str]] = None


def vector_route(column, vector, *, weight: float = 1.0,
                 limit: Optional[int] = None,
                 options: Optional[Dict[str, str]] = None) -> VectorRoute:
    return VectorRoute(
        column=column,
        vector=vector,
        weight=weight,
        limit=limit,
        options=dict(options or {}),
    )


def text_route(query, *, column: Optional[str] = None, weight: float = 1.0,
               limit: Optional[int] = None,
               options: Optional[Dict[str, str]] = None) -> TextRoute:
    return TextRoute(
        query=query,
        column=column,
        weight=weight,
        limit=limit,
        options=dict(options or {}),
    )


class MultimodalTable:
    """High-level table facade for mutable multimodal application data."""

    def __init__(self, catalog, identifier: str, raw_table):
        self.catalog = catalog
        self.identifier = identifier
        self.name = identifier
        self.raw_table = raw_table
        self.table = raw_table

    def add(self, data):
        arrow_table = _to_arrow_table(data, _target_schema(self.raw_table))
        write_builder = self.raw_table.new_batch_write_builder()
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        try:
            table_write.write_arrow(arrow_table)
            table_commit.commit(table_write.prepare_commit())
        finally:
            table_write.close()
            table_commit.close()
        return self

    def add_video(
            self,
            video,
            frames,
            *,
            video_column=None,
            first_frame=0):
        """Append logical frame rows backed by one complete encoded video.

        ``frames`` supplies every table column except the configured
        ``video-frame-field``. Frame ordinals are generated from
        ``first_frame`` and stored in the video descriptor, not in the normal
        data file.
        """
        return self.add_videos(
            [(video, frames, first_frame)], video_column=video_column
        )

    def add_videos(self, videos, *, video_column=None):
        """Append several encoded videos with one writer and one commit.

        Each item is ``(video, frames)`` or ``(video, frames, first_frame)``.
        Keeping one writer open lets a single ``.video`` object pack multiple
        complete encoded videos up to the configured rolling target.
        """
        column = self._resolve_video_frame_column(video_column)
        target_schema = _target_schema(self.raw_table)

        def frame_batches():
            for item in videos:
                try:
                    item = tuple(item)
                except TypeError as error:
                    raise ValueError(
                        "Each videos item must be (video, frames) or "
                        "(video, frames, first_frame)."
                    ) from error
                if len(item) == 2:
                    video, frames = item
                    first_frame = 0
                elif len(item) == 3:
                    video, frames, first_frame = item
                else:
                    raise ValueError(
                        "Each videos item must be (video, frames) or "
                        "(video, frames, first_frame)."
                    )
                yield _video_frame_table(
                    video,
                    frames,
                    column,
                    first_frame,
                    target_schema,
                )

        return self.add_batches(frame_batches())

    def _resolve_video_frame_column(self, requested):
        configured = self.raw_table.options.video_frame_fields()
        if not configured:
            raise ValueError(
                "add_video requires table option 'video-frame-field'."
            )
        if requested is None and len(configured) > 1:
            raise ValueError(
                "video_column is required when 'video-frame-field' configures "
                "multiple fields."
            )
        column = requested or next(iter(configured))
        if column not in configured:
            raise ValueError(
                "Video column %r is not configured by 'video-frame-field'."
                % column
            )
        return column

    def add_batches(self, batches):
        """Append an iterable of batches with one writer and one commit.

        Keeping the writer open across batches also preserves video payload
        groups and lets one ``.video`` file pack several encoded videos.
        """
        try:
            iterator = iter(batches)
        except TypeError as error:
            raise ValueError("batches must be an iterable of input batches.") from error

        target_schema = _target_schema(self.raw_table)
        table_write = None
        table_commit = None
        commit_started = False
        try:
            for data in iterator:
                arrow_table = _to_arrow_table(data, target_schema)
                if arrow_table.num_rows == 0:
                    continue
                if table_write is None:
                    write_builder = self.raw_table.new_batch_write_builder()
                    table_write = write_builder.new_write()
                    table_commit = write_builder.new_commit()
                table_write.write_arrow(arrow_table)

            close_iterator = getattr(iterator, "close", None)
            iterator = None
            if close_iterator is not None:
                close_iterator()
            if table_write is None:
                return self
            commit_messages = table_write.prepare_commit()
            commit_started = True
            table_commit.commit(commit_messages)
            return self
        except BaseException:
            if table_write is not None and not commit_started:
                table_write.abort()
            raise
        finally:
            if iterator is not None:
                close_iterator = getattr(iterator, "close", None)
                if close_iterator is not None:
                    close_iterator()
            try:
                if table_write is not None:
                    table_write.close()
            finally:
                if table_commit is not None:
                    table_commit.close()

    def overwrite(self, data, partition: Optional[Mapping[str, object]] = None):
        arrow_table = _to_arrow_table(data, _target_schema(self.raw_table))
        overwrite_partition = dict(partition) if partition is not None else None
        write_builder = (
            self.raw_table.new_batch_write_builder()
            .overwrite(overwrite_partition)
        )
        table_write = write_builder.new_write()
        table_commit = write_builder.new_commit()
        try:
            table_write.write_arrow(arrow_table)
            table_commit.commit(table_write.prepare_commit())
        finally:
            table_write.close()
            table_commit.close()
        return self

    def update(self, where, values):
        video_columns = self.raw_table.options.video_frame_fields()
        if isinstance(values, Mapping):
            assigned_video_columns = video_columns.intersection(values)
            if assigned_video_columns:
                raise ValueError(
                    "update() cannot write video-frame-field %r; use "
                    "replace_video() with a complete encoded video."
                    % sorted(assigned_video_columns)[0]
                )
        query = self.scan().where(where)
        predicate = query._predicate
        write_builder = self.raw_table.new_batch_write_builder()
        table_update = write_builder.new_update()
        table_commit = write_builder.new_commit()
        try:
            messages = table_update.update_by_predicate(predicate, values)
            table_commit.commit(messages)
        finally:
            table_commit.close()
        return self

    def replace_video(
            self,
            where,
            video,
            *,
            video_column=None,
            first_frame=0):
        """Replace the video backing the logical frame rows matching ``where``.

        Matching rows are ordered by ``_ROW_ID`` and assigned consecutive
        frame ordinals starting at ``first_frame``. Only the configured video
        column is updated; ordinary columns and the normal data files remain
        untouched.
        """
        column = self._resolve_video_frame_column(video_column)
        target_schema = _target_schema(self.raw_table)
        payload, first_frame = _video_payload(video, first_frame)

        row_ids = (
            self.scan()
            .where(where)
            .select([])
            .with_row_id()
            .to_arrow()[SpecialFields.ROW_ID.name]
            .to_pylist()
        )
        row_ids.sort()
        if not row_ids:
            return self

        descriptors = _video_frame_descriptors(
            payload, len(row_ids), first_frame)
        update_data = pa.Table.from_arrays(
            [
                pa.array(row_ids, type=pa.int64()),
                pa.array(descriptors, type=target_schema.field(column).type),
            ],
            names=[SpecialFields.ROW_ID.name, column],
        )

        write_builder = self.raw_table.new_batch_write_builder()
        table_update = write_builder.new_update().with_update_type([column])
        table_commit = write_builder.new_commit()
        try:
            messages = table_update.update_by_arrow_with_row_id(update_data)
            table_commit.commit(messages)
        finally:
            table_commit.close()
        return self

    def delete(self, where):
        query = self.scan().where(where)
        predicate = query._predicate
        write_builder = self.raw_table.new_batch_write_builder()
        table_update = write_builder.new_update()
        table_commit = write_builder.new_commit()
        try:
            messages = table_update.delete_by_predicate(predicate)
            table_commit.commit(messages)
        finally:
            table_commit.close()
        return self

    def merge(self, on):
        return _MergeBuilder(self, on)

    def map_with_blobs(self, dataset, columns, fn, **kwargs):
        from pypaimon.ray import map_with_blobs

        return map_with_blobs(
            dataset,
            columns,
            fn,
            file_io=self.raw_table.file_io,
            all_blob_columns=_blob_columns(self.raw_table),
            **kwargs,
        )

    def scan(
            self,
            *,
            snapshot_id: Optional[int] = None,
            tag_name: Optional[str] = None):
        return ScanQuery(_time_travel_table(
            self.raw_table, snapshot_id=snapshot_id, tag_name=tag_name))

    def take_row_ids(
            self,
            row_ids,
            *,
            snapshot_id: Optional[int] = None,
            tag_name: Optional[str] = None):
        row_ids = _coerce_row_ids(row_ids)
        read_table = _time_travel_table(
            self.raw_table, snapshot_id=snapshot_id, tag_name=tag_name)
        read_builder = read_table.new_read_builder().with_projection(
            [field.name for field in read_table.fields]
            + [SpecialFields.ROW_ID.name]
        )
        predicate = read_builder.new_predicate_builder().is_in(
            SpecialFields.ROW_ID.name, row_ids)
        query = ScanQuery(read_table)
        query._predicate = predicate
        return query

    def blobs(self, *, column: Optional[str] = None, key_column: Optional[str] = None):
        from pypaimon.multimodal.blob_store import BlobStore
        return BlobStore(self, column=column, key_column=key_column)

    def search(
            self,
            query,
            *,
            column: Optional[str] = None,
            options: Optional[Dict[str, str]] = None,
            pre_filter=None,
            snapshot_id: Optional[int] = None,
            tag_name: Optional[str] = None):
        read_table = _time_travel_table(
            self.raw_table, snapshot_id=snapshot_id, tag_name=tag_name)
        schema = _target_schema(read_table)
        if isinstance(query, str):
            return TextQuery(
                read_table,
                text_query=_coerce_full_text_query(
                    query, "search", schema, column=column),
                pre_filter=pre_filter,
            )
        vector = _coerce_vector(query, "search")
        if _looks_like_batch_vectors(vector):
            raise ValueError(
                "search() accepts a single query; use search_vectors() for "
                "multiple vectors.")
        return VectorQuery(
            read_table,
            vector=vector,
            vector_column=_resolve_vector_column(
                schema, column, len(vector)),
            vector_options=options,
            pre_filter=pre_filter,
        )

    def search_vectors(
            self,
            vectors,
            *,
            column: Optional[str] = None,
            options: Optional[Dict[str, str]] = None,
            pre_filter=None,
            snapshot_id: Optional[int] = None,
            tag_name: Optional[str] = None):
        read_table = _time_travel_table(
            self.raw_table, snapshot_id=snapshot_id, tag_name=tag_name)
        schema = _target_schema(read_table)
        vectors = _coerce_vectors(vectors)
        dimension = len(vectors[0])
        if any(len(vector) != dimension for vector in vectors):
            raise ValueError(
                "search_vectors requires all query vectors to have the same "
                "dimension.")
        vector_column = _resolve_vector_column(
            schema, column, dimension)
        return BatchVectorQuery(
            read_table,
            vectors=vectors,
            vector_column=vector_column,
            vector_options=options,
            pre_filter=pre_filter,
        )

    def search_hybrid(
            self,
            routes,
            *,
            ranker: str = "rrf",
            route_limit: Optional[int] = None,
            pre_filter=None,
            snapshot_id: Optional[int] = None,
            tag_name: Optional[str] = None):
        read_table = _time_travel_table(
            self.raw_table, snapshot_id=snapshot_id, tag_name=tag_name)
        schema = _target_schema(read_table)
        vector_routes, text_routes = _normalize_hybrid_routes(
            schema,
            routes=routes,
            method="search_hybrid",
        )
        if not vector_routes and not text_routes:
            raise ValueError(
                "search_hybrid requires at least one route.")
        return HybridQuery(
            read_table,
            vector_routes=vector_routes,
            text_routes=text_routes,
            ranker=ranker,
            route_limit=route_limit,
            pre_filter=pre_filter,
        )

    def create_index(self, column, index_type, options=None):
        return self.raw_table.create_global_index(
            column, index_type=_normalize_index_type(index_type), options=options)

    def _merge(
            self,
            source,
            on,
            when_matched: Sequence[WhenMatched],
            when_not_matched: Sequence[WhenNotMatched],
            operation: str):
        target_cols, source_cols = _normalize_merge_on(on, operation)
        target_schema = _target_schema(self.raw_table)
        source_table = _to_arrow_table(source)
        _validate_merge_on(
            target_schema, source_table, target_cols, source_cols, operation)
        source_table = _cast_merge_on_columns(
            source_table, target_schema, target_cols, source_cols)
        when_matched, when_not_matched = _resolve_merge_clauses(
            when_matched,
            when_not_matched,
            target_schema,
            source_table,
            dict(zip(target_cols, source_cols)),
        )

        write_builder = self.raw_table.new_batch_write_builder()
        table_update = write_builder.new_update()
        table_commit = write_builder.new_commit()
        try:
            messages = table_update.merge_into(
                source_table,
                on=dict(zip(target_cols, source_cols)),
                when_matched=list(when_matched),
                when_not_matched=list(when_not_matched),
            )
            table_commit.commit(messages)
        finally:
            table_commit.close()
        return self


class _MergeBuilder:
    """Builder for idempotent merge writes."""

    def __init__(self, table: MultimodalTable, on):
        self._table = table
        self._on = on
        self._when_matched = []
        self._when_not_matched = []

    def when_matched_update(self, values=None, where: Optional[str] = None):
        self._when_matched.append(
            WhenMatched.update(
                _ALL_SOURCE_COLUMNS if values is None else values,
                condition=_normalize_merge_where(where),
            ))
        return self

    def when_matched_delete(self, where: Optional[str] = None):
        self._when_matched.append(
            WhenMatched.delete(
                condition=_normalize_merge_where(where),
            ))
        return self

    def when_not_matched_insert(self, values=None, where: Optional[str] = None):
        self._when_not_matched.append(
            WhenNotMatched(
                insert=_ALL_SOURCE_COLUMNS if values is None else values,
                condition=_normalize_merge_where(where),
            ))
        return self

    def execute(self, data):
        if not self._when_matched and not self._when_not_matched:
            raise ValueError(
                "merge requires at least one matched or not-matched clause.")
        return self._table._merge(
            data,
            self._on,
            when_matched=self._when_matched,
            when_not_matched=self._when_not_matched,
            operation="merge",
        )


def _blob_columns(table):
    return tuple(
        field.name for field in table.fields
        if is_blob_type(field.type)
    )


def _to_arrow_table(data, target_schema=None):
    if target_schema is not None:
        data = _serialize_blob_values(data, target_schema)
    if isinstance(data, pa.Table):
        table = data
    elif isinstance(data, pa.RecordBatch):
        table = pa.Table.from_batches([data])
    elif isinstance(data, list):
        table = pa.Table.from_pylist(data)
    elif isinstance(data, dict):
        table = pa.Table.from_pydict(data)
    elif hasattr(data, "__dataframe__") or data.__class__.__module__.startswith("pandas"):
        table = pa.Table.from_pandas(data, preserve_index=False)
    else:
        raise ValueError("Unsupported multimodal data type: %r" % type(data))

    if target_schema is None:
        return table
    return _align_to_schema(table, target_schema)


def _serialize_blob_values(data, target_schema):
    if isinstance(data, (pa.Table, pa.RecordBatch)):
        return data
    binary_fields = {
        field.name: field.type
        for field in target_schema
        if _contains_binary(field.type)
    }
    if not binary_fields:
        return data

    if isinstance(data, list):
        return [
            {
                name: _serialize_blob_value(value, binary_fields.get(name))
                for name, value in row.items()
            }
            if isinstance(row, Mapping)
            else row
            for row in data
        ]
    if isinstance(data, dict):
        converted = dict(data)
        for name, arrow_type in binary_fields.items():
            if name not in converted:
                continue
            column = converted[name]
            if isinstance(column, (pa.Array, pa.ChunkedArray)):
                column = column.to_pylist()
            converted[name] = [
                _serialize_blob_value(value, arrow_type)
                for value in column
            ]
        return converted
    if (
        hasattr(data, "__dataframe__")
        or data.__class__.__module__.startswith("pandas")
    ):
        converted = data.copy()
        for name, arrow_type in binary_fields.items():
            if name in converted.columns:
                converted[name] = converted[name].map(
                    lambda value: _serialize_blob_value(value, arrow_type)
                )
        return converted
    return data


def _contains_binary(arrow_type):
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return True
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return _contains_binary(arrow_type.value_type)
    if pa.types.is_map(arrow_type):
        return _contains_binary(arrow_type.item_type)
    return False


def _serialize_blob_value(value, arrow_type):
    if value is None or arrow_type is None:
        return value
    if pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        from pypaimon.table.row.blob import Blob, BlobDescriptor
        if isinstance(value, BlobDescriptor):
            return value.serialize()
        if isinstance(value, Blob):
            try:
                return value.to_descriptor().serialize()
            except RuntimeError:
                return value.to_data()
        return value
    if pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        return [
            _serialize_blob_value(element, arrow_type.value_type)
            for element in value
        ]
    if pa.types.is_map(arrow_type):
        entries = value.items() if isinstance(value, Mapping) else value
        return [
            (key, _serialize_blob_value(element, arrow_type.item_type))
            for key, element in entries
        ]
    return value


def _coerce_row_ids(row_ids):
    if row_ids is None or isinstance(row_ids, (str, bytes)):
        raise ValueError("row_ids must be an iterable of row id integers.")
    try:
        iterator = iter(row_ids)
    except TypeError:
        raise ValueError("row_ids must be an iterable of row id integers.")

    coerced = []
    for row_id in iterator:
        if hasattr(row_id, "as_py"):
            row_id = row_id.as_py()
        coerced.append(int(row_id))
    return coerced


def _time_travel_table(table, snapshot_id=None, tag_name=None):
    if snapshot_id is not None and tag_name is not None:
        raise ValueError(
            "snapshot_id and tag_name cannot be set at the same time")
    if snapshot_id is not None:
        return table.copy({
            CoreOptions.SCAN_SNAPSHOT_ID.key(): str(snapshot_id),
        })
    if tag_name is not None:
        return table.copy({
            CoreOptions.SCAN_TAG_NAME.key(): tag_name,
        })
    return table


def _target_schema(table):
    return PyarrowFieldParser.from_paimon_schema(table.table_schema.fields)


def _video_payload(video, first_frame):
    from pypaimon.table.row.blob import (
        Blob,
        BlobDescriptor,
        VideoFrameDescriptor,
    )

    if isinstance(first_frame, bool) or not isinstance(first_frame, int):
        raise ValueError("first_frame must be a non-negative int.")
    if first_frame < 0:
        raise ValueError("first_frame must be a non-negative int.")

    if isinstance(video, str):
        video = Blob.from_local(video)
    if isinstance(video, Blob):
        try:
            payload = video.to_descriptor()
        except RuntimeError as error:
            raise ValueError(
                "video must be descriptor-backed; inline video bytes are not "
                "accepted by video write APIs."
            ) from error
    elif isinstance(video, BlobDescriptor):
        payload = video
    else:
        raise ValueError(
            "video must be a path, Blob, or BlobDescriptor, got %r."
            % type(video)
        )
    if isinstance(payload, VideoFrameDescriptor):
        payload = payload.payload_descriptor

    return payload, first_frame


def _video_frame_descriptors(payload, count, first_frame):
    from pypaimon.table.row.blob import VideoFrameDescriptor

    return [
        VideoFrameDescriptor(
            payload.uri,
            payload.offset,
            payload.length,
            first_frame + index,
        ).serialize()
        for index in range(count)
    ]


def _video_frame_table(video, frames, video_column, first_frame, target_schema):
    payload, first_frame = _video_payload(video, first_frame)

    non_video_schema = pa.schema([
        field for field in target_schema if field.name != video_column
    ])
    frame_table = _to_arrow_table(frames, non_video_schema)
    descriptor_values = _video_frame_descriptors(
        payload, frame_table.num_rows, first_frame)
    arrays = []
    for field in target_schema:
        if field.name == video_column:
            arrays.append(pa.array(descriptor_values, type=field.type))
        else:
            arrays.append(frame_table[field.name])
    return pa.Table.from_arrays(arrays, schema=target_schema)


def _align_to_schema(
        table: pa.Table,
        schema: pa.Schema,
        column_mapping: Optional[Dict[str, str]] = None) -> pa.Table:
    column_mapping = column_mapping or {}
    arrays = []
    for field in schema:
        source_name = column_mapping.get(field.name, field.name)
        if source_name in table.column_names:
            array = table[source_name]
            if array.type != field.type:
                array = array.cast(field.type)
        else:
            array = pa.nulls(table.num_rows, type=field.type)
        arrays.append(array)
    return pa.Table.from_arrays(arrays, schema=schema)


def _normalize_merge_on(on, operation: str):
    if isinstance(on, Mapping):
        target_cols = list(on.keys())
        source_cols = list(on.values())
    elif isinstance(on, str):
        target_cols = [on]
        source_cols = [on]
    else:
        target_cols = list(on)
        source_cols = list(on)
    if not target_cols:
        raise ValueError("%s requires at least one ON column." % operation)
    if len(target_cols) != len(source_cols):
        raise ValueError(
            "%s ON target/source column counts do not match." % operation)
    return target_cols, source_cols


def _validate_merge_on(
        target_schema,
        source_table,
        target_cols,
        source_cols,
        operation: str):
    target_names = set(target_schema.names)
    source_names = set(source_table.column_names)
    missing_targets = [col for col in target_cols if col not in target_names]
    if missing_targets:
        raise ValueError(
            "%s ON target columns are not in table schema: %s"
            % (operation, missing_targets))
    missing_sources = [col for col in source_cols if col not in source_names]
    if missing_sources:
        raise ValueError(
            "%s ON source columns are not in source data: %s"
            % (operation, missing_sources))


def _cast_merge_on_columns(source_table, target_schema, target_cols, source_cols):
    result = source_table
    for target_col, source_col_name in zip(target_cols, source_cols):
        target_type = target_schema.field(target_col).type
        index = result.schema.get_field_index(source_col_name)
        if index < 0:
            continue
        array = result[source_col_name]
        if array.type == target_type:
            continue
        source_field = result.schema.field(index)
        result = result.set_column(
            index,
            pa.field(
                source_col_name,
                target_type,
                nullable=source_field.nullable,
                metadata=source_field.metadata,
            ),
            array.cast(target_type),
        )
    return result


def _resolve_merge_clauses(
        when_matched,
        when_not_matched,
        target_schema,
        source_table,
        on_map):
    target_names = list(target_schema.names)
    source_names = set(source_table.column_names)
    return (
        [
            (
                WhenMatched.delete(condition=clause.condition)
                if clause.delete
                else WhenMatched.update(
                    _resolve_merge_set_spec(
                        clause.update, target_names, source_names, on_map),
                    condition=clause.condition,
                )
            )
            for clause in when_matched
        ],
        [
            WhenNotMatched(
                insert=_resolve_merge_set_spec(
                    clause.insert, target_names, source_names, on_map),
                condition=clause.condition,
            )
            for clause in when_not_matched
        ],
    )


def _resolve_merge_set_spec(spec, target_names, source_names, on_map):
    if spec is not _ALL_SOURCE_COLUMNS:
        return spec
    return {
        target_name: _source_col(source_name)
        for target_name in target_names
        for source_name in [on_map.get(target_name, target_name)]
        if source_name in source_names
    }


def _normalize_merge_where(where):
    if where is None:
        return None
    if not isinstance(where, str):
        raise ValueError("merge where must be a string.")
    parts, last = [], 0
    for match in _STRING_LITERAL_PATTERN.finditer(where):
        parts.append(_rewrite_merge_refs(where[last:match.start()]))
        parts.append(match.group())
        last = match.end()
    parts.append(_rewrite_merge_refs(where[last:]))
    return ''.join(parts)


def _rewrite_merge_refs(text):
    return _MERGE_REF_PATTERN.sub(
        lambda m: "%s.%s" % (
            "s" if m.group(1) == "source" else "t",
            m.group(2),
        ),
        text,
    )


def _normalize_index_type(index_type):
    if not isinstance(index_type, str):
        return index_type
    normalized = index_type.strip().lower()
    if normalized == "full-text":
        return "full-text"
    return index_type


def _coerce_vector(value, method):
    if value is None or isinstance(value, str):
        raise ValueError("%s requires a vector query." % method)
    if hasattr(value, "tolist"):
        value = value.tolist()
    try:
        return list(value)
    except TypeError as e:
        raise ValueError("%s requires a vector query." % method) from e


def _coerce_vectors(values):
    method = "search_vectors"
    if values is None or isinstance(values, str):
        raise ValueError("%s requires query vectors." % method)
    if hasattr(values, "tolist"):
        values = values.tolist()
    try:
        vectors = list(values)
    except TypeError as e:
        raise ValueError("%s requires query vectors." % method) from e
    if not vectors:
        raise ValueError("%s requires at least one vector query." % method)
    return [_coerce_vector(vector, method) for vector in vectors]


def _looks_like_batch_vectors(value):
    if not value:
        return False
    first = value[0]
    return not isinstance(first, (int, float)) and hasattr(first, "__iter__")


def _normalize_hybrid_routes(
        schema,
        routes,
        method):
    vector_routes, text_routes = [], []
    for route in _route_specs(routes):
        if _is_text_route_spec(route):
            text_routes.append(_normalize_text_route(route, method, schema))
        else:
            vector_routes.append(
                _normalize_vector_route(route, method, schema))
    return vector_routes, text_routes


def _route_specs(routes):
    if routes is None:
        return []
    if isinstance(routes, (VectorRoute, TextRoute, Mapping)):
        return [routes]
    if _is_column_vector_pair(routes):
        return [routes]
    return list(routes)


def _is_text_route_spec(route):
    if isinstance(route, TextRoute):
        return True
    if isinstance(route, VectorRoute):
        return False
    if isinstance(route, Mapping):
        route_type = route.get("type") or route.get("route_type")
        if route_type is not None:
            return str(route_type).replace("-", "_").lower() in (
                "text", "full_text", "fulltext")
        return (
            "query" in route
            or "text" in route
            or "terms" in route
            or "full_text_query" in route
        ) and not _has_vector_query(route)
    return False


def _normalize_vector_route(route, method, schema):
    if isinstance(route, VectorRoute):
        column = route.column
        vector = route.vector
        weight = route.weight
        limit = route.limit
        options = route.options
    elif isinstance(route, Mapping):
        column = (
            route.get("column")
            or route.get("vector_column")
            or route.get("field")
            or route.get("anns_field")
        )
        vector = _mapping_vector(route)
        weight = route.get("weight", 1.0)
        limit = route.get("limit")
        options = _mapping_options(route)
    elif _is_column_vector_pair(route):
        column, vector = route
        weight = 1.0
        limit = None
        options = None
    else:
        raise ValueError(
            "%s vector routes require a route spec with a column and vector."
            % method)
    if not column:
        raise ValueError("%s vector routes require a column." % method)
    return {
        "column": column,
        "vector": _coerce_vector(vector, method),
        "weight": weight,
        "limit": limit,
        "options": dict(options or {}),
    }


def _normalize_text_route(route, method, schema):
    if isinstance(route, TextRoute):
        column = route.column
        query = route.query
        weight = route.weight
        limit = route.limit
        options = route.options
    elif isinstance(route, Mapping):
        column = route.get("column")
        query = (
            route.get("query")
            or route.get("text")
            or route.get("terms")
            or route.get("full_text_query")
        )
        weight = route.get("weight", 1.0)
        limit = route.get("limit")
        options = route.get("options")
    else:
        raise ValueError(
            "%s text routes require a route spec with a query."
            % method)
    text_query = _coerce_full_text_query(query, method, schema, column=column)
    return {
        "column": text_query["column"],
        "query": text_query["query"],
        "weight": weight,
        "limit": limit,
        "options": dict(options or {}),
    }


def _has_vector_query(route):
    return any(key in route for key in ("vector", "query_vector", "data"))


def _mapping_vector(route):
    if "vector" in route:
        return route["vector"]
    if "query_vector" in route:
        return route["query_vector"]
    if "data" in route:
        data = route["data"]
        if hasattr(data, "tolist"):
            data = data.tolist()
        values = list(data)
        if len(values) == 1 and hasattr(values[0], "__iter__"):
            return values[0]
        return values
    raise ValueError("Vector route requires vector, query_vector, or data.")


def _mapping_options(route):
    if "options" in route:
        return route["options"]
    if "param" in route:
        param = route["param"]
        if isinstance(param, Mapping) and isinstance(param.get("params"), Mapping):
            return param["params"]
        return param
    if "params" in route:
        return route["params"]
    return None


def _is_column_vector_pair(route):
    return (
        isinstance(route, (list, tuple))
        and len(route) == 2
        and isinstance(route[0], str)
    )


def _coerce_full_text_query(query, method, schema, column=None):
    field_name = column or _infer_text_column(schema, "text")
    if isinstance(query, str):
        stripped = query.lstrip()
        query_json = (
            query
            if stripped.startswith("{")
            else json.dumps({"match": {"query": query}}, separators=(",", ":"))
        )
        return {"column": field_name, "query": query_json}
    if isinstance(query, Mapping):
        return {
            "column": field_name,
            "query": json.dumps(query, separators=(",", ":")),
        }
    raise ValueError("%s requires a text string or query mapping." % method)


def _resolve_vector_column(
        schema: pa.Schema,
        column: Optional[str],
        dimension: int) -> str:
    if column is not None:
        try:
            field = schema.field(column)
        except KeyError as e:
            raise ValueError(
                "Vector column '%s' not found in table schema." % column) from e
        if not pa.types.is_fixed_size_list(field.type):
            raise ValueError(
                "Column '%s' is not a fixed-size vector column." % column)
        if field.type.list_size != dimension:
            raise ValueError(
                "Vector dimension %d does not match column '%s' dimension %d."
                % (dimension, column, field.type.list_size))
        return column

    candidates = [
        (field.name, field.type.list_size)
        for field in schema
        if pa.types.is_fixed_size_list(field.type)
    ]
    matches = [
        name
        for name, column_dimension in candidates
        if column_dimension == dimension
    ]
    if len(matches) == 1:
        return matches[0]
    if not matches:
        available = ", ".join(
            "%s(%d)" % candidate for candidate in candidates) or "none"
        raise ValueError(
            "No vector column found with dimension %d; available vector "
            "columns: %s."
            % (dimension, available))
    raise ValueError(
        "Multiple vector columns found with dimension %d: %s; pass column."
        % (dimension, ", ".join(matches)))


def _infer_text_column(schema: pa.Schema, parameter: str = "text_column"):
    columns = [
        field.name
        for field in schema
        if pa.types.is_string(field.type) or pa.types.is_large_string(field.type)
    ]
    return _infer_single_column(columns, "text", parameter)


def _infer_single_column(columns, kind, parameter):
    if len(columns) == 1:
        return columns[0]
    if not columns:
        raise ValueError("No %s column found; pass %s." % (kind, parameter))
    raise ValueError(
        "Multiple %s columns found %s; pass %s."
        % (kind, columns, parameter))
