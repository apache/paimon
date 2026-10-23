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

"""Streaming, materialized exports of multimodal scans to Lance datasets."""

import os
from copy import copy

import pyarrow as pa
from packaging.version import Version

from pypaimon.multimodal.blob_read import fetch_blob_bodies
from pypaimon.read.push_down_utils import rewrite_predicate_indices
from pypaimon.read.read_support import close_read_source
from pypaimon.schema.data_types import (
    ArrayType,
    AtomicType,
    MapType,
    MultisetType,
    RowType,
    VectorType,
    is_array_blob_type,
    is_blob_file_type,
    is_blob_type,
    is_map_blob_type,
)
from pypaimon.snapshot.time_travel_util import SCAN_KEYS, TimeTravelUtil
from pypaimon.table.file_store_table import FileStoreTable
from pypaimon.table.row.blob import BlobDescriptorSerde, VideoFrameDescriptor


def export_lance(query, uri, *, mode, blob_mode, storage_options, blob_parallelism):
    if mode not in ("create", "overwrite"):
        raise ValueError("export_lance mode must be 'create' or 'overwrite'.")
    if blob_mode != "materialize":
        raise ValueError("export_lance blob_mode must be 'materialize'.")
    if not isinstance(blob_parallelism, int) or isinstance(blob_parallelism, bool) or blob_parallelism < 1:
        raise ValueError("blob_parallelism must be a positive integer.")
    uri = os.fspath(uri)
    if not uri:
        raise ValueError("export_lance requires a non-empty destination URI.")
    try:
        import lance
    except ImportError as error:
        raise ImportError("export_lance requires Lance; install 'pypaimon[lance]'.") from error
    if Version(lance.__version__) < Version("13.0.0"):
        raise ImportError(
            "export_lance requires pylance >= 13.0.0 (Python >= 3.10) "
            "to preserve NULL Blob values across batches; "
            "upgrade with pip install --upgrade 'pypaimon[lance]'.")

    # Resolve once, including an empty table. All schema, planning and BLOB
    # reads must use the same view even if another writer commits meanwhile.
    query = copy(query)
    table = query._table
    snapshot = TimeTravelUtil.resolve_snapshot(table)
    historical = any(table.options.options.contains_key(key) for key in SCAN_KEYS)
    if (snapshot is not None and not hasattr(table, "_read_snapshot")
            and snapshot.schema_id != table.table_schema.id
            and (historical or snapshot.schema_id > table.table_schema.id)):
        # Follow moving tags and newer data commits, but preserve the logical
        # schema of latest or pinned scans after a schema-only ALTER. A tag's
        # schema can still be resolved when its snapshot manifest has expired.
        schema = table.schema_manager.get_schema(snapshot.schema_id)
        dynamic_options = getattr(table, "_applied_dynamic_options", {})
        options = table._exclude_current_schema_field_options(schema, table.options.options, set(dynamic_options))
        table = FileStoreTable(
            table.file_io, table.identifier, table.table_path,
            schema.copy(new_options=options), table.catalog_environment)
        table._applied_dynamic_options = dict(dynamic_options)
        # Predicates were bound when the query was created. Rebind after a
        # schema refresh so partition pruning uses the resolved field order.
        query._predicate = rewrite_predicate_indices(
            query._predicate, table.new_read_builder()._table_read_fields())
    query._table = table._copy_with_snapshot(snapshot)
    fields = query._configured_read_builder()._output_fields()
    names = [field.name for field in fields]
    if not names or len(names) != len(set(names)):
        raise ValueError("export_lance requires at least one column and unique output column names.")
    video_fields = query._table.options.video_frame_fields()
    # A nested projection's alias may match an unrelated source column.
    video_field_ids = {field.id for field in query._table.fields if field.name in video_fields}
    for field in fields:
        if field.id in video_field_ids:
            raise ValueError("export_lance does not support video-frame column %r; project it out." % field.name)
        if _contains_type(field.type, "VARIANT"):
            raise ValueError("export_lance does not support VARIANT column %r; project it out." % field.name)
        if _contains_type(field.type, "BLOB") and not is_blob_file_type(field.type):
            raise ValueError(
                "export_lance only supports BLOB, ARRAY<BLOB> and MAP<K, BLOB> output columns; "
                "project nested BLOB column %r separately." % field.name)

    blob_columns = [field.name for field in fields if is_blob_file_type(field.type)]
    map_blobs = [field.name for field in fields if is_map_blob_type(field.type)]
    array_blobs = [field.name for field in fields if is_array_blob_type(field.type)]
    descriptor_fields = query._table.options.blob_descriptor_fields()
    descriptor_field_ids = {field.id for field in query._table.fields if field.name in descriptor_fields}
    descriptor_columns = [field.name for field in fields if field.id in descriptor_field_ids]
    # Include predicate-only columns internally, then remove them before
    # materializing payloads and writing the requested projection.
    builder, file_io, _ = query._blob_descriptor_query_read_builder()
    splits = builder.new_scan().plan().splits()
    reader = builder.new_read()._to_managed_arrow_batch_reader(splits)
    batches = None
    failure = None
    try:
        export_fields = []
        for field in fields:
            output_field = reader.schema.field(field.name)
            output_field = output_field.with_type(_lance_arrow_type(output_field.type))
            # Native storage is limited to scalar output fields. Container
            # values retain their ordinary nested Arrow binary layout.
            if is_blob_type(field.type):
                output_field = output_field.with_metadata({
                    **(output_field.metadata or {}), b"lance-encoding:blob": b"true",
                })
            export_fields.append(output_field)
        schema = pa.schema(export_fields, metadata=reader.schema.metadata)
        batches = _export_batches(reader, names, schema, file_io, blob_columns,
                                  map_blobs, array_blobs, descriptor_columns, blob_parallelism)
        # The default 2.0 format loses nullable struct validity. Version 2.1
        # with the required Lance version preserves nested NULLs as well.
        return lance.write_dataset(
            batches, uri, schema=schema, mode=mode,
            storage_options=storage_options, data_storage_version="2.1")
    except BaseException as error:
        failure = error
        raise
    finally:
        try:
            close_read_source(batches, failure)
        finally:
            close_read_source(reader, failure)


def _export_batches(reader, names, schema, file_io, blob_columns, map_blobs,
                    array_blobs, descriptor_columns, parallelism):
    for batch in reader:
        batch = batch.select(names)
        bodies = {}
        if blob_columns:
            data = batch.select(blob_columns).to_pydict()
            for column in descriptor_columns:
                # Known descriptor fields also accept older wire versions
                # without a magic header. Normalize only those fields so raw
                # payloads that resemble legacy references remain unchanged.
                data[column] = [_export_descriptor(value, column) for value in data[column]]
            bodies = fetch_blob_bodies(
                file_io, data, blob_columns, parallelism, map_blobs, array_blobs)
        arrays = []
        for index, field in enumerate(schema):
            if field.name in bodies:
                array = pa.array(bodies[field.name], type=field.type)
            else:
                array = batch.column(index)
                if array.type != field.type:
                    array = pa.array(array.to_pylist(), type=field.type)
            arrays.append(array)
        yield pa.RecordBatch.from_arrays(arrays, schema=schema)


def _export_descriptor(value, column):
    if value is None:
        return None
    raw = bytes(value)
    if VideoFrameDescriptor.is_video_frame_descriptor(raw):
        raise ValueError("export_lance does not support video-frame column %r; project it out." % column)
    return BlobDescriptorSerde.deserialize(raw).serialize()


def _lance_arrow_type(data_type):
    # Lance does not accept Arrow MAP. Its entries can be represented without
    # losing ordering, NULLs or duplicate keys as a list of key/value structs.
    if pa.types.is_map(data_type):
        return pa.list_(pa.struct([
            data_type.key_field.with_type(_lance_arrow_type(data_type.key_type)),
            data_type.item_field.with_name("value").with_type(_lance_arrow_type(data_type.item_type)),
        ]))
    if pa.types.is_struct(data_type):
        return pa.struct([field.with_type(_lance_arrow_type(field.type)) for field in data_type])
    if (pa.types.is_list(data_type) or pa.types.is_large_list(data_type)
            or pa.types.is_fixed_size_list(data_type)):
        child = data_type.value_field.with_type(_lance_arrow_type(data_type.value_type))
        if pa.types.is_large_list(data_type):
            return pa.large_list(child)
        if pa.types.is_fixed_size_list(data_type):
            return pa.list_(child, data_type.list_size)
        return pa.list_(child)
    return data_type


def _contains_type(data_type, name):
    if isinstance(data_type, AtomicType):
        return data_type.type.upper() == name
    if isinstance(data_type, RowType):
        return any(_contains_type(field.type, name) for field in data_type.fields)
    if isinstance(data_type, MapType):
        return _contains_type(data_type.key, name) or _contains_type(data_type.value, name)
    if isinstance(data_type, (ArrayType, MultisetType, VectorType)):
        return _contains_type(data_type.element, name)
    return False
