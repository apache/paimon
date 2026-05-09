################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

from typing import Any, Dict, List, Optional

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
from pypaimon.schema.data_types import DataField, PyarrowFieldParser
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
                 nested_name_paths: Optional[List[List[str]]] = None):
        file_path_for_pyarrow = file_io.to_filesystem_path(file_path)
        self.dataset = ds.dataset(file_path_for_pyarrow, format=file_format, filesystem=file_io.filesystem)
        self._file_format = file_format
        self.read_fields = read_fields
        self._read_field_names = [f.name for f in read_fields]

        # ``nested_name_paths`` is parallel to ``read_fields``; when
        # any path has length > 1 the scanner is invoked with a
        # ``{flat_name: ds.field(*path)}`` column dict.
        if nested_name_paths is not None and len(nested_name_paths) != len(read_fields):
            raise ValueError(
                "nested_name_paths length {} does not match read_fields length {}".format(
                    len(nested_name_paths), len(read_fields)))
        self._nested_name_paths = nested_name_paths
        has_nested_path = bool(
            nested_name_paths and any(len(p) > 1 for p in nested_name_paths))

        # Identify which fields exist in the file and which are missing.
        # For nested projection, "exists" is determined by walking the
        # whole path against the file schema; sub-field schema evolution
        # (a leaf renamed or removed) shows up as ``missing`` and is
        # served as a NULL column, mirroring the top-level handling.
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

        # column name → VariantSchema for shredded columns that need assembly.
        # In nested mode we still want to reassemble shredded VARIANTs
        # that were projected at the top level — only the columns actually
        # reached via a length>1 path are skipped (those are sub-fields of
        # some other struct, not VARIANTs themselves).
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
            # Dict-form columns let PyArrow read leaf fields out of nested
            # structs without materialising the parent. The dict keys
            # become the output column names — they're already flattened
            # to ``a_b`` form by the upstream projection utility.
            existing_set = set(self.existing_fields)
            columns_dict = {}
            for f, path in zip(read_fields, nested_name_paths):
                if f.name in existing_set:
                    columns_dict[f.name] = ds.field(*path)
            self.reader = self.dataset.scanner(
                columns=columns_dict,
                filter=push_down_predicate,
                batch_size=batch_size
            ).to_reader()
        else:
            # Only pass existing fields to PyArrow scanner to avoid errors
            self.reader = self.dataset.scanner(
                columns=self.existing_fields,
                filter=push_down_predicate,
                batch_size=batch_size
            ).to_reader()

        self._output_schema = (
            PyarrowFieldParser.from_paimon_schema(read_fields) if read_fields else None
        )

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        try:
            batch = self.reader.read_next_batch()

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

        except StopIteration:
            return None

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
        if self.reader is not None:
            self.reader = None


def _path_exists_in_arrow_schema(schema: pa.Schema, path: List[str]) -> bool:
    """Walk ``path`` (a list of field names) through a PyArrow schema and
    return whether every step exists. The first step is a top-level field
    name; subsequent steps are struct child names. Missing leaves at any
    depth (e.g. a renamed sub-field) yield ``False`` so the caller can
    fall back to a NULL column instead of raising during scan setup.
    """
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
