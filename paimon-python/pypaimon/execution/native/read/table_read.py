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

"""Native reads and adaptation to the public Arrow and row contracts."""

import logging
from typing import List, Optional

import pyarrow

from pypaimon.casting.schema_evolution import read_arrow_schema
from pypaimon.read.read_support import close_read_source
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.predicate import Predicate
from pypaimon.execution.native import runtime
from pypaimon.execution.native import table as native_table
from pypaimon.execution.native.read import builder as native_builder
from pypaimon.read.read_type import OutputProjection, adapter_output_paths, extract_array, output_schema
from pypaimon.read.split import Split
from pypaimon.read.variant_read_type import has_variant_extractions
from pypaimon.schema.data_types import DataField

logger = logging.getLogger(__name__)
_NATIVE_READ_FILE_FORMATS = frozenset({
    CoreOptions.FILE_FORMAT_PARQUET, CoreOptions.FILE_FORMAT_ORC,
    CoreOptions.FILE_FORMAT_AVRO, CoreOptions.FILE_FORMAT_ROW,
    CoreOptions.FILE_FORMAT_MOSAIC,
})
_NATIVE_READ_FILE_SUFFIXES = tuple('.%s' % file_format for file_format in _NATIVE_READ_FILE_FORMATS)
_NATIVE_BLOB_FILE_SUFFIXES = ('.blob', '.video')


def _prepare_native_read(table, predicate: Optional[Predicate] = None,
                         limit: Optional[int] = None,
                         projection: Optional[List[str]] = None,
                         blob_parallelism: Optional[int] = None,
                         read_type: Optional[List[DataField]] = None,
                         include_row_kind: bool = False):
    """Create the configured Core reader entrypoint."""
    if not runtime.native_runtime_available():
        raise RuntimeError(
            "read.native.enabled needs the pypaimon-rust extension")
    if not native_table._native_query_auth_supported(table):
        raise RuntimeError('Native reads cannot reconstruct custom query authorization')
    if read_type is None:
        from pypaimon.read.read_builder import ReadBuilder
        resolved = ReadBuilder(table)
        if projection is not None:
            resolved.with_projection(projection)
        read_type = resolved.read_type()
    builder = native_builder._configure_native_read_builder(
        native_builder._native_read_builder(table), predicate, limit, projection,
        read_type=read_type,
        include_row_kind=include_row_kind)
    if blob_parallelism is not None:
        builder = builder.with_blob_parallelism(blob_parallelism)
    reader = builder.new_read()
    return reader.read_arrow


def native_read(table, splits, predicate: Optional[Predicate] = None,
                limit: Optional[int] = None,
                projection: Optional[List[str]] = None,
                blob_parallelism: Optional[int] = None,
                read_type: Optional[List[DataField]] = None,
                include_row_kind: bool = False,
                parallelism: int = 1,
                ordered: bool = False):
    """Run one Core read; ``ordered`` retains the supplied split order."""
    read_splits = _prepare_native_read(
        table,
        predicate=predicate,
        limit=limit,
        projection=projection,
        blob_parallelism=blob_parallelism,
        read_type=read_type,
        include_row_kind=include_row_kind)
    return read_splits(splits, parallelism=parallelism, ordered=ordered)


class NativeTableReadMixin:
    def _native_fallback(
        self,
        reason: str,
        error: Optional[Exception] = None,
        log: bool = False,
    ):
        if has_variant_extractions(self.read_type):
            raise RuntimeError(
                "Variant extraction requires a compatible native reader: %s"
                % reason) from error
        if log:
            logger.warning(
                "Native read failed, falling back to the Python reader: %s",
                reason)
        return None

    def _try_native_batches(
            self,
            splits: List[Split],
            schema: pyarrow.Schema,
            parallelism: Optional[int] = None,
            blob_parallelism: Optional[int] = None,
            ordered: bool = False):
        """Return Rust-read batches, or ``None`` when this read must fall back."""
        if not self.table.options.native_read_enabled():
            return self._native_fallback("read.native.enabled is false")
        if self.table.options.file_format() not in _NATIVE_READ_FILE_FORMATS:
            return self._native_fallback(
                "the table file format is not supported")
        if not splits:
            return []
        if not self._native_blob_view_supported():
            return self._native_fallback(
                "the table uses an unsupported BLOB view")
        try:
            from pypaimon.execution.native.read.split_adapter import native_split_from_python
        except Exception as e:
            return self._native_fallback(str(e), e, log=True)
        rust_splits = []
        for split in splits:
            if not self._native_split_files_supported(split):
                return self._native_fallback(
                    "a split contains a file unsupported by the native reader")
            rust_split = getattr(split, '_native_split', None)
            if rust_split is None:
                try:
                    rust_split = native_split_from_python(split, ranges_use_row_ids=(
                        self.table.options.row_tracking_enabled() or self.table.options.data_evolution_enabled()))
                except Exception as e:
                    return self._native_fallback(
                        "native split conversion failed: %s" % e,
                        e,
                        log=True)
            rust_splits.append(rust_split)
        # Python adapts the public configuration; Core owns split grouping,
        # bounded concurrency, the row quota and cancellation of every stream.
        effective = 1 if parallelism is None else parallelism
        try:
            batches = native_read(
                self.table, rust_splits,
                parallelism=effective,
                ordered=ordered,
                **self._native_read_kwargs(blob_parallelism))
        except Exception as error:
            runtime._raise_if_native_fork_safety_error(error)
            return self._native_fallback(str(error), error, log=True)
        return self._convert_native_batches(batches, schema)

    def _native_read_kwargs(self, blob_parallelism=None):
        kwargs = {
            'predicate': self.predicate,
            'limit': self.limit,
        }
        if blob_parallelism is not None:
            kwargs['blob_parallelism'] = blob_parallelism
        kwargs['read_type'] = self.read_type
        if self.include_row_kind:
            kwargs['include_row_kind'] = True
        return kwargs

    @staticmethod
    def _native_split_files_supported(split):
        for data_file in split.files:
            file_name = data_file.file_name.lower()
            if (not file_name.endswith(_NATIVE_READ_FILE_SUFFIXES)
                    and not file_name.endswith(_NATIVE_BLOB_FILE_SUFFIXES)):
                return False
        return True

    def _convert_native_batches(self, batches, schema):
        failure = None
        try:
            for batch in batches:
                if batch.num_rows == 0:
                    continue
                batch = self._prepare_native_batch(batch, schema)
                batch = self._project_batch_to_output(batch)
                yield self._try_to_pad_batch_by_schema(batch, schema)
        except BaseException as error:
            failure = error
            raise
        finally:
            close_read_source(batches, failure)

    def _prepare_native_batch(self, batch, schema):
        if self.include_row_kind and 'rowkind' in batch.schema.names:
            index = batch.schema.get_field_index('rowkind')
            batch = batch.rename_columns([
                self._row_kind_column if position == index else name
                for position, name in enumerate(batch.schema.names)
            ])
        if self.nested_name_paths:
            batch = self._flatten_native_nested_batch(batch, schema)
        return batch

    def _flatten_native_nested_batch(self, batch, schema):
        _, paths = self._reader_adapter()
        # Rust already evaluated the complete predicate and projected away
        # filter-only fields. Flatten just the user output dependencies; the
        # Python split reader retains the additional adapter inputs internally.
        outputs = adapter_output_paths(self.output_projection, self._adapter_read_type, paths)
        needed = {path[0] for _, path in outputs}
        fields_and_paths = [(field, path) for field, path in zip(self._adapter_read_type, paths)
                            if field.name in needed]
        adapter_fields = [field for field, _ in fields_and_paths]
        paths = [path for _, path in fields_and_paths]
        arrays = [extract_array(batch, path) for path in paths]
        fields = list(read_arrow_schema(adapter_fields))
        # Authorization can mask a NOT NULL field. Keep the captured source
        # nullability when flattening, including whole-parent/MAP projections.
        projected = output_schema(batch.schema, OutputProjection([
            (field.name, path) for field, path in zip(fields, paths)]))
        fields = [field.with_nullable(field.nullable or projected[i].nullable) for i, field in enumerate(fields)]
        if self.include_row_kind:
            arrays.insert(0, batch.column(self._row_kind_column))
            fields.insert(0, batch.schema.field(self._row_kind_column))
        return pyarrow.RecordBatch.from_arrays(arrays, schema=pyarrow.schema(fields))

    def _native_blob_view_supported(self) -> bool:
        from pypaimon.execution.native.table import _native_blob_view_supported
        return _native_blob_view_supported(self.table, (field.name for field in self._scan_read_type))

    def _try_native_rows(self, splits):
        from copy import copy
        from pypaimon.execution.native.read.row_iterator import native_rows
        from pypaimon.read.read_type import output_fields

        # InternalRow carries row kind separately from projected fields.
        read = copy(self)
        read.include_row_kind = True
        read._row_kind_column = 'rowkind'
        schema = read._add_row_kind_to_schema(read._output_arrow_schema(splits), read._row_kind_column)
        fields = output_fields(self.read_type, self.output_projection)
        batches = read._try_native_batches(splits, schema, parallelism=1, ordered=True)
        if batches is None:
            return None

        def rows():
            failure = None
            try:
                yield from native_rows(batches, self.table, fields)
            except BaseException as error:
                failure = error
                raise
            finally:
                close_read_source(batches, failure)

        return rows()
