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

from typing import Any, Dict, List, Optional, Set

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.common.file_io import FileIO
from pypaimon.common.options.core_options import CoreOptions
from pypaimon.data.variant_shredding import (
    VariantSchema,
    assemble_shredded_column,
    build_variant_schema,
    is_shredded_variant,
)
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.schema.data_types import AtomicType, DataField, PyarrowFieldParser
from pypaimon.table.special_fields import SpecialFields


class FormatPyArrowReader(RecordBatchReader):
    """
    A Format Reader that reads record batch from a Parquet or ORC file using PyArrow,
    and filters it based on the provided predicate and projection.

    When a VARIANT column is stored in the shredded Parquet format (a struct with
    ``metadata``, ``value``, and ``typed_value`` fields), this reader transparently
    reconstructs the standard ``struct<value: binary, metadata: binary>`` representation.
    """

    def __init__(self, file_io: FileIO, file_format: str, file_path: str,
                 read_fields: List[DataField],
                 push_down_predicate: Any, batch_size: int = 1024,
                 options: CoreOptions = None,
                 nested_name_paths: Optional[List[List[str]]] = None,
                 predicate_field_names: Optional[Set[str]] = None):
        self._predicate_field_names = predicate_field_names or set()
        file_path_for_pyarrow = file_io.to_filesystem_path(file_path)
        self.dataset = ds.dataset(file_path_for_pyarrow, format=file_format, filesystem=file_io.filesystem)
        self._file_format = file_format
        self.read_fields = read_fields
        self._read_field_names = [f.name for f in read_fields]

        if nested_name_paths is not None and len(nested_name_paths) != len(read_fields):
            raise ValueError(
                "nested_name_paths length {} does not match read_fields length {}".format(
                    len(nested_name_paths), len(read_fields)))
        self._nested_name_paths = nested_name_paths
        has_nested_path = bool(
            nested_name_paths and any(len(p) > 1 for p in nested_name_paths))

        file_schema = self.dataset.schema
        if has_nested_path:
            self.existing_fields = []
            self.missing_fields = []
            for f, path in zip(read_fields, nested_name_paths):
                if _path_exists_in_arrow_schema(file_schema, path):
                    self.existing_fields.append(f.name)
                else:
                    self.missing_fields.append(f.name)
        else:
            file_schema_names = set(file_schema.names)
            self.existing_fields = [f.name for f in read_fields if f.name in file_schema_names]
            self.missing_fields = [f.name for f in read_fields if f.name not in file_schema_names]

        self._shredded_schemas: Dict[str, VariantSchema] = {}
        if options is None or options.variant_shredding_enabled():
            top_level_names = set(file_schema.names)
            for name in self.existing_fields:
                if name not in top_level_names:
                    continue
                field_type = file_schema.field(name).type
                if is_shredded_variant(field_type):
                    self._shredded_schemas[name] = build_variant_schema(field_type)

        if has_nested_path:
            existing_set = set(self.existing_fields)
            columns_dict = {}
            for f, path in zip(read_fields, nested_name_paths):
                if f.name in existing_set:
                    columns_dict[f.name] = ds.field(*path)
            self._scan_columns = columns_dict
        else:
            # Only pass existing fields to PyArrow scanner to avoid errors
            self._scan_columns = self.existing_fields
        self._scan_filter = push_down_predicate
        self._scan_batch_size = batch_size

        self._output_schema = (
            PyarrowFieldParser.from_paimon_schema(read_fields) if read_fields else None
        )

        # A VARIANT column stored as a nested struct cannot be assembled across
        # row groups by a single dataset scanner ("Nested data conversions not
        # implemented for chunked array outputs"). For a multi-row-group file
        # with a projected VARIANT, read row groups via ParquetFile (a codepath
        # that avoids that assembly), pruning row groups by statistics and
        # reading only projected/filter columns.
        self._parquet_file = None
        if (self._file_format == 'parquet'
                and not has_nested_path
                and self._has_projected_variant()):
            import pyarrow.parquet as pq
            # NB: ParquetFile(filesystem=...) is unsupported on old PyArrow
            # (e.g. 6.0.1); open the file handle via the filesystem instead.
            self._parquet_file = pq.ParquetFile(
                file_io.filesystem.open_input_file(file_path_for_pyarrow))
        if self._parquet_file is not None and self._parquet_file.num_row_groups > 1:
            self._raw_batches = self._iter_row_group_batches()
        else:
            reader = self.dataset.scanner(
                columns=self._scan_columns,
                filter=self._scan_filter,
                batch_size=self._scan_batch_size,
            ).to_reader()
            self._raw_batches = self._iter_reader_batches(reader)

    def _has_projected_variant(self) -> bool:
        return any(
            f.name in self.existing_fields
            and isinstance(f.type, AtomicType)
            and f.type.type == 'VARIANT'
            for f in self.read_fields)

    @staticmethod
    def _iter_reader_batches(reader):
        while True:
            try:
                yield reader.read_next_batch()
            except StopIteration:
                return

    def _iter_row_group_batches(self):
        # Read projected columns plus any filter-only columns (needed for the
        # in-memory predicate), one row group at a time. Filter via an in-memory
        # dataset scanner (Table.filter(Expression) is rejected by older PyArrow)
        # and project back to the requested columns.
        columns = self._row_group_read_columns()
        for row_group in self._surviving_row_group_ids():
            for batch in self._parquet_file.iter_batches(
                    row_groups=[row_group],
                    columns=columns,
                    batch_size=self._scan_batch_size):
                if self._scan_filter is None:
                    yield batch
                    continue
                table = ds.dataset(
                    pa.Table.from_batches([batch])
                ).scanner(filter=self._scan_filter).to_table()
                if self.existing_fields:
                    table = table.select(self.existing_fields)
                for out in table.to_batches():
                    if out.num_rows:
                        yield out

    def _row_group_read_columns(self):
        if not self.existing_fields:
            return None
        columns = list(self.existing_fields)
        if self._scan_filter is not None:
            file_names = set(self.dataset.schema.names)
            for name in self._predicate_field_names:
                if name in file_names and name not in columns:
                    columns.append(name)
        return columns

    def _surviving_row_group_ids(self):
        # Row-group pruning by column statistics via fragment metadata; falls
        # back to all row groups when pruning is unavailable.
        total = self._parquet_file.num_row_groups
        if self._scan_filter is None:
            return range(total)
        try:
            ids = set()
            for fragment in self.dataset.get_fragments(filter=self._scan_filter):
                for row_group in fragment.split_by_row_group(self._scan_filter):
                    ids.update(info.id for info in row_group.row_groups)
            return sorted(ids)
        except Exception:
            return range(total)

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        batch = next(self._raw_batches, None)
        if batch is None:
            return None
        return self._post_process_batch(batch)

    def _post_process_batch(self, batch: RecordBatch) -> RecordBatch:
        if True:
            if self._file_format == 'orc' and self._output_schema is not None:
                batch = self._cast_orc_time_columns(batch)

            if self._shredded_schemas:
                batch = self._assemble_shredded_variants(batch)

            if not self.missing_fields:
                return batch

            def _type_for_missing(name: str) -> pa.DataType:
                if self._output_schema is not None:
                    idx = self._output_schema.get_field_index(name)
                    if idx >= 0:
                        return self._output_schema.field(idx).type
                return pa.null()

            missing_columns = [
                pa.nulls(batch.num_rows, type=_type_for_missing(name))
                for name in self.missing_fields
            ]

            # Reconstruct the batch with all fields in the correct order
            all_columns = []
            out_fields = []
            for field_name in self._read_field_names:
                if field_name in self.existing_fields:
                    # Get the column from the existing batch
                    column_idx = self.existing_fields.index(field_name)
                    all_columns.append(batch.column(column_idx))
                    out_fields.append(batch.schema.field(column_idx))
                else:
                    # Get the column from missing fields
                    column_idx = self.missing_fields.index(field_name)
                    col_type = _type_for_missing(field_name)
                    all_columns.append(missing_columns[column_idx])
                    nullable = not SpecialFields.is_system_field(field_name)
                    out_fields.append(pa.field(field_name, col_type, nullable=nullable))
            # Create a new RecordBatch with all columns
            return pa.RecordBatch.from_arrays(all_columns, schema=pa.schema(out_fields))

    def _assemble_shredded_variants(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """Replace shredded VARIANT columns with standard struct<value, metadata>."""
        changed = False
        columns = list(batch.columns)
        fields = list(batch.schema)

        for i, f in enumerate(fields):
            if f.name in self._shredded_schemas:
                schema = self._shredded_schemas[f.name]
                new_col = assemble_shredded_column(columns[i], schema)
                columns[i] = new_col
                fields[i] = pa.field(f.name, new_col.type, nullable=f.nullable)
                changed = True

        if not changed:
            return batch
        return pa.RecordBatch.from_arrays(columns, schema=pa.schema(fields))

    def _cast_orc_time_columns(self, batch):
        """Cast int32 TIME columns back to time32('ms') when reading ORC.
        """
        columns = []
        fields = []
        changed = False
        for i, name in enumerate(batch.schema.names):
            col = batch.column(i)
            idx = self._output_schema.get_field_index(name)
            if idx >= 0 and pa.types.is_int32(col.type) \
                    and pa.types.is_time(self._output_schema.field(idx).type):
                col = col.cast(self._output_schema.field(idx).type)
                fields.append(self._output_schema.field(idx))
                changed = True
            else:
                fields.append(batch.schema.field(i))
            columns.append(col)
        if changed:
            return pa.RecordBatch.from_arrays(columns, schema=pa.schema(fields))
        return batch

    def close(self):
        self._raw_batches = None


def _path_exists_in_arrow_schema(schema: pa.Schema, path: List[str]) -> bool:
    """Check whether a name path is fully resolvable in the given schema."""
    if not path:
        return False
    if path[0] not in schema.names:
        return False
    current_type = schema.field(path[0]).type
    for name in path[1:]:
        if not pa.types.is_struct(current_type):
            return False
        idx = current_type.get_field_index(name)
        if idx < 0:
            return False
        current_type = current_type.field(idx).type
    return True
