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

import os
from abc import ABC, abstractmethod
from functools import partial
from typing import Callable, List, Optional, Tuple

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.common.predicate import Predicate
from pypaimon.deletionvectors import ApplyDeletionVectorReader
from pypaimon.deletionvectors.deletion_vector import DeletionVector
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.read.interval_partition import IntervalPartition, SortedRun
from pypaimon.read.partition_info import PartitionInfo
from pypaimon.read.push_down_utils import trim_predicate_by_fields
from pypaimon.read.reader.concat_batch_reader import (ConcatBatchReader,
                                                      MergeAllBatchReader, DataEvolutionMergeReader)
from pypaimon.read.reader.concat_record_reader import ConcatRecordReader

from pypaimon.read.reader.data_file_batch_reader import DataFileBatchReader
from pypaimon.read.reader.drop_delete_reader import DropDeleteRecordReader
from pypaimon.read.reader.empty_record_reader import EmptyFileRecordReader
from pypaimon.read.reader.field_bunch import BlobBunch, DataBunch, FieldBunch
from pypaimon.read.reader.filter_record_reader import FilterRecordReader
from pypaimon.read.reader.format_avro_reader import FormatAvroReader
from pypaimon.read.reader.format_blob_reader import FormatBlobReader
from pypaimon.read.reader.format_lance_reader import FormatLanceReader
from pypaimon.read.reader.format_pyarrow_reader import FormatPyArrowReader
from pypaimon.read.reader.iface.record_batch_reader import (RecordBatchReader,
                                                            RowPositionReader)
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.read.reader.key_value_unwrap_reader import \
    KeyValueUnwrapRecordReader
from pypaimon.read.reader.key_value_wrap_reader import KeyValueWrapReader
from pypaimon.read.reader.shard_batch_reader import ShardBatchReader
from pypaimon.read.reader.sort_merge_reader import SortMergeReaderWithMinHeap
from pypaimon.read.split import Split
from pypaimon.schema.data_types import DataField
from pypaimon.table.special_fields import SpecialFields

KEY_PREFIX = "_KEY_"
KEY_FIELD_ID_START = 1000000
NULL_FIELD_INDEX = -1


class SplitRead(ABC):
    """Abstract base class for split reading operations."""

    def __init__(
            self,
            table,
            predicate: Optional[Predicate],
            read_type: List[DataField],
            split: Split,
            row_tracking_enabled: bool):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.predicate = predicate
        self.push_down_predicate = self._push_down_predicate()
        self.split = split
        self.row_tracking_enabled = row_tracking_enabled
        self.value_arity = len(read_type)

        self.trimmed_primary_key = self.table.trimmed_primary_keys
        self.read_fields = read_type
        if isinstance(self, MergeFileSplitRead):
            self.read_fields = self._create_key_value_fields(read_type)
        self.schema_id_2_fields = {}
        self.deletion_file_readers = {}

    def _push_down_predicate(self) -> Optional[Predicate]:
        if self.predicate is None:
            return None
        elif self.table.is_primary_key_table:
            pk_predicate = trim_predicate_by_fields(self.predicate, self.table.primary_keys)
            if not pk_predicate:
                return None
            return pk_predicate
        else:
            return self.predicate

    @abstractmethod
    def create_reader(self) -> RecordReader:
        """Create a record reader for the given split."""

    def file_reader_supplier(self, file: DataFileMeta, for_merge_read: bool,
                             read_fields: List[str], row_tracking_enabled: bool) -> RecordBatchReader:
        (read_file_fields, read_arrow_predicate) = self._get_fields_and_predicate(file.schema_id, read_fields)

        # Use external_path if available, otherwise use file_path
        file_path = file.external_path if file.external_path else file.file_path
        _, extension = os.path.splitext(file_path)
        file_format = extension[1:]

        format_reader: RecordBatchReader
        if file_format == CoreOptions.FILE_FORMAT_AVRO:
            format_reader = FormatAvroReader(self.table.file_io, file_path, read_file_fields,
                                             self.read_fields, read_arrow_predicate)
        elif file_format == CoreOptions.FILE_FORMAT_BLOB:
            blob_as_descriptor = CoreOptions.blob_as_descriptor(self.table.options)
            format_reader = FormatBlobReader(self.table.file_io, file_path, read_file_fields,
                                             self.read_fields, read_arrow_predicate, blob_as_descriptor)
        elif file_format == CoreOptions.FILE_FORMAT_LANCE:
            format_reader = FormatLanceReader(self.table.file_io, file_path, read_file_fields,
                                              read_arrow_predicate)
        elif file_format == CoreOptions.FILE_FORMAT_PARQUET or file_format == CoreOptions.FILE_FORMAT_ORC:
            format_reader = FormatPyArrowReader(self.table.file_io, file_format, file_path,
                                                read_file_fields, read_arrow_predicate)
        else:
            raise ValueError(f"Unexpected file format: {file_format}")

        index_mapping = self.create_index_mapping()
        partition_info = self._create_partition_info()
        system_fields = SpecialFields.find_system_fields(self.read_fields)
        table_schema_fields = (
            SpecialFields.row_type_with_row_tracking(self.table.table_schema.fields)
            if row_tracking_enabled else self.table.table_schema.fields
        )
        if for_merge_read:
            return DataFileBatchReader(
                format_reader,
                index_mapping,
                partition_info,
                self.trimmed_primary_key,
                table_schema_fields,
                file.max_sequence_number,
                file.first_row_id,
                row_tracking_enabled,
                system_fields)
        else:
            return DataFileBatchReader(
                format_reader,
                index_mapping,
                partition_info,
                None,
                table_schema_fields,
                file.max_sequence_number,
                file.first_row_id,
                row_tracking_enabled,
                system_fields)

    def _get_fields_and_predicate(self, schema_id: int, read_fields):
        key = (schema_id, tuple(read_fields))
        if key not in self.schema_id_2_fields:
            schema = self.table.schema_manager.get_schema(schema_id)
            schema_fields = (
                SpecialFields.row_type_with_row_tracking(schema.fields)
                if self.row_tracking_enabled else schema.fields
            )
            schema_field_names = set(field.name for field in schema_fields)
            if self.table.is_primary_key_table:
                schema_field_names.add('_SEQUENCE_NUMBER')
                schema_field_names.add('_VALUE_KIND')
            read_file_fields = [read_field for read_field in read_fields if read_field in schema_field_names]
            read_predicate = trim_predicate_by_fields(self.push_down_predicate, read_file_fields)
            read_arrow_predicate = read_predicate.to_arrow() if read_predicate else None
            self.schema_id_2_fields[key] = (read_file_fields, read_arrow_predicate)
        return self.schema_id_2_fields[key]

    @abstractmethod
    def _get_all_data_fields(self):
        """Get all data fields"""

    def _get_read_data_fields(self):
        read_data_fields = []
        read_field_ids = {field.id for field in self.read_fields}
        for data_field in self._get_all_data_fields():
            if data_field.id in read_field_ids:
                read_data_fields.append(data_field)
        return read_data_fields

    def _create_key_value_fields(self, value_field: List[DataField]):
        all_fields: List[DataField] = self.table.fields
        all_data_fields = []

        for field in all_fields:
            if field.name in self.trimmed_primary_key:
                key_field_name = f"{KEY_PREFIX}{field.name}"
                key_field_id = field.id + KEY_FIELD_ID_START
                key_field = DataField(key_field_id, key_field_name, field.type)
                all_data_fields.append(key_field)

        all_data_fields.append(SpecialFields.SEQUENCE_NUMBER)
        all_data_fields.append(SpecialFields.VALUE_KIND)

        for field in value_field:
            all_data_fields.append(field)

        return all_data_fields

    def create_index_mapping(self):
        base_index_mapping = self._create_base_index_mapping(self.read_fields, self._get_read_data_fields())
        trimmed_key_mapping, _ = self._get_trimmed_fields(self._get_read_data_fields(), self._get_all_data_fields())
        if base_index_mapping is None:
            mapping = trimmed_key_mapping
        elif trimmed_key_mapping is None:
            mapping = base_index_mapping
        else:
            combined = [0] * len(base_index_mapping)
            for i in range(len(base_index_mapping)):
                if base_index_mapping[i] < 0:
                    combined[i] = base_index_mapping[i]
                else:
                    combined[i] = trimmed_key_mapping[base_index_mapping[i]]
            mapping = combined

        if mapping is not None:
            for i in range(len(mapping)):
                if mapping[i] != i:
                    return mapping

        return None

    def _create_base_index_mapping(self, table_fields: List[DataField], data_fields: List[DataField]):
        index_mapping = [0] * len(table_fields)
        field_id_to_index = {field.id: i for i, field in enumerate(data_fields)}

        for i, table_field in enumerate(table_fields):
            field_id = table_field.id
            data_field_index = field_id_to_index.get(field_id)
            if data_field_index is not None:
                index_mapping[i] = data_field_index
            else:
                index_mapping[i] = NULL_FIELD_INDEX

        for i in range(len(index_mapping)):
            if index_mapping[i] != i:
                return index_mapping

        return None

    def _get_final_read_data_fields(self) -> List[str]:
        _, trimmed_fields = self._get_trimmed_fields(
            self._get_read_data_fields(), self._get_all_data_fields()
        )
        return self._remove_partition_fields(trimmed_fields)

    def _remove_partition_fields(self, fields: List[DataField]) -> List[str]:
        partition_keys = self.table.partition_keys
        if not partition_keys:
            return [field.name for field in fields]

        fields_without_partition = []
        for field in fields:
            if field.name not in partition_keys:
                fields_without_partition.append(field)

        return [field.name for field in fields_without_partition]

    def _get_trimmed_fields(self, read_data_fields: List[DataField],
                            all_data_fields: List[DataField]) -> Tuple[List[int], List[DataField]]:
        trimmed_mapping = [0] * len(read_data_fields)
        trimmed_fields = []

        field_id_to_field = {field.id: field for field in all_data_fields}
        position_map = {}
        for i, field in enumerate(read_data_fields):
            is_key_field = field.name.startswith(KEY_PREFIX)
            if is_key_field:
                original_id = field.id - KEY_FIELD_ID_START
            else:
                original_id = field.id
            original_field = field_id_to_field.get(original_id)

            if original_id in position_map:
                trimmed_mapping[i] = position_map[original_id]
            else:
                position = len(trimmed_fields)
                position_map[original_id] = position
                trimmed_mapping[i] = position
                if is_key_field:
                    trimmed_fields.append(original_field)
                else:
                    trimmed_fields.append(field)

        return trimmed_mapping, trimmed_fields

    def _create_partition_info(self):
        if not self.table.partition_keys:
            return None
        partition_mapping = self._construct_partition_mapping()
        if not partition_mapping:
            return None
        return PartitionInfo(partition_mapping, self.split.partition)

    def _construct_partition_mapping(self) -> List[int]:
        _, trimmed_fields = self._get_trimmed_fields(
            self._get_read_data_fields(), self._get_all_data_fields()
        )
        partition_names = self.table.partition_keys

        mapping = [0] * (len(trimmed_fields) + 1)
        p_count = 0

        for i, field in enumerate(trimmed_fields):
            if field.name in partition_names:
                partition_index = partition_names.index(field.name)
                mapping[i] = -(partition_index + 1)
                p_count += 1
            else:
                mapping[i] = (i - p_count) + 1

        return mapping

    def _genarate_deletion_file_readers(self):
        self.deletion_file_readers = {}
        if self.split.data_deletion_files:
            for data_file, deletion_file in zip(self.split.files, self.split.data_deletion_files):
                if deletion_file is not None:
                    # Create a callable method to read the deletion vector
                    self.deletion_file_readers[data_file.file_name] = lambda df=deletion_file: DeletionVector.read(
                        self.table.file_io, df)


class RawFileSplitRead(SplitRead):
    def raw_reader_supplier(self, file: DataFileMeta, dv_factory: Optional[Callable] = None) -> Optional[RecordReader]:
        read_fields = self._get_final_read_data_fields()
        # If the current file needs to be further divided for reading, use ShardBatchReader
        if file.file_name in self.split.shard_file_idx_map:
            (start_row, end_row) = self.split.shard_file_idx_map[file.file_name]
            if (start_row, end_row) == (-1, -1):
                return None
            else:
                file_batch_reader = ShardBatchReader(self.file_reader_supplier(
                    file=file,
                    for_merge_read=False,
                    read_fields=read_fields,
                    row_tracking_enabled=True), start_row, end_row)
        else:
            file_batch_reader = self.file_reader_supplier(
                file=file,
                for_merge_read=False,
                read_fields=read_fields,
                row_tracking_enabled=True)
        dv = dv_factory() if dv_factory else None
        if dv:
            return ApplyDeletionVectorReader(RowPositionReader(file_batch_reader), dv)
        else:
            return file_batch_reader

    def create_reader(self) -> RecordReader:
        self._genarate_deletion_file_readers()
        data_readers = []
        for file in self.split.files:
            supplier = partial(
                self.raw_reader_supplier,
                file=file,
                dv_factory=self.deletion_file_readers.get(file.file_name, None)
            )
            data_readers.append(supplier)

        if not data_readers:
            return EmptyFileRecordReader()

        concat_reader = ConcatBatchReader(data_readers)
        # if the table is appendonly table, we don't need extra filter, all predicates has pushed down
        if self.table.is_primary_key_table and self.predicate:
            return FilterRecordReader(concat_reader, self.predicate)
        else:
            return concat_reader

    def _get_all_data_fields(self):
        if self.row_tracking_enabled:
            return SpecialFields.row_type_with_row_tracking(self.table.fields)
        return self.table.fields


class MergeFileSplitRead(SplitRead):
    def kv_reader_supplier(self, file: DataFileMeta, dv_factory: Optional[Callable] = None) -> RecordReader:
        file_batch_reader = self.file_reader_supplier(file, True, self._get_final_read_data_fields(), False)
        dv = dv_factory() if dv_factory else None
        if dv:
            return ApplyDeletionVectorReader(
                KeyValueWrapReader(RowPositionReader(file_batch_reader),
                                   len(self.trimmed_primary_key), self.value_arity), dv)
        else:
            return KeyValueWrapReader(file_batch_reader, len(self.trimmed_primary_key), self.value_arity)

    def section_reader_supplier(self, section: List[SortedRun]) -> RecordReader:
        readers = []
        for sorter_run in section:
            data_readers = []
            for file in sorter_run.files:
                supplier = partial(self.kv_reader_supplier, file, self.deletion_file_readers.get(file.file_name, None))
                data_readers.append(supplier)
            readers.append(ConcatRecordReader(data_readers))
        return SortMergeReaderWithMinHeap(readers, self.table.table_schema)

    def create_reader(self) -> RecordReader:
        # Create a dict mapping data file name to deletion file reader method
        self._genarate_deletion_file_readers()
        section_readers = []
        sections = IntervalPartition(self.split.files).partition()
        for section in sections:
            supplier = partial(self.section_reader_supplier, section)
            section_readers.append(supplier)
        concat_reader = ConcatRecordReader(section_readers)
        kv_unwrap_reader = KeyValueUnwrapRecordReader(DropDeleteRecordReader(concat_reader))
        if self.predicate:
            return FilterRecordReader(kv_unwrap_reader, self.predicate)
        else:
            return kv_unwrap_reader

    def _get_all_data_fields(self):
        return self._create_key_value_fields(self.table.fields)


class DataEvolutionSplitRead(SplitRead):

    def create_reader(self) -> RecordReader:
        files = self.split.files
        suppliers = []

        # Split files by row ID
        split_by_row_id = self._split_by_row_id(files)

        for need_merge_files in split_by_row_id:
            if len(need_merge_files) == 1 or not self.read_fields:
                # No need to merge fields, just create a single file reader
                suppliers.append(
                    lambda f=need_merge_files[0]: self._create_file_reader(f, self._get_final_read_data_fields())
                )
            else:
                suppliers.append(
                    lambda files=need_merge_files: self._create_union_reader(files)
                )

        return ConcatBatchReader(suppliers)

    def _split_by_row_id(self, files: List[DataFileMeta]) -> List[List[DataFileMeta]]:
        """Split files by firstRowId for data evolution."""

        # Sort files by firstRowId and then by maxSequenceNumber
        def sort_key(file: DataFileMeta) -> tuple:
            first_row_id = file.first_row_id if file.first_row_id is not None else float('-inf')
            is_blob = 1 if self._is_blob_file(file.file_name) else 0
            max_seq = file.max_sequence_number
            return (first_row_id, is_blob, -max_seq)

        sorted_files = sorted(files, key=sort_key)

        # Split files by firstRowId
        split_by_row_id = []
        last_row_id = -1
        check_row_id_start = 0
        current_split = []

        for file in sorted_files:
            first_row_id = file.first_row_id
            if first_row_id is None:
                split_by_row_id.append([file])
                continue

            if not self._is_blob_file(file.file_name) and first_row_id != last_row_id:
                if current_split:
                    split_by_row_id.append(current_split)
                if first_row_id < check_row_id_start:
                    raise ValueError(
                        f"There are overlapping files in the split: {files}, "
                        f"the wrong file is: {file}"
                    )
                current_split = []
                last_row_id = first_row_id
                check_row_id_start = first_row_id + file.row_count
            current_split.append(file)

        if current_split:
            split_by_row_id.append(current_split)

        return split_by_row_id

    def _create_union_reader(self, need_merge_files: List[DataFileMeta]) -> RecordReader:
        """Create a DataEvolutionFileReader for merging multiple files."""
        # Split field bunches
        fields_files = self._split_field_bunches(need_merge_files)

        # Validate row counts and first row IDs
        row_count = fields_files[0].row_count()
        first_row_id = fields_files[0].files()[0].first_row_id

        for bunch in fields_files:
            if bunch.row_count() != row_count:
                raise ValueError("All files in a field merge split should have the same row count.")
            if bunch.files()[0].first_row_id != first_row_id:
                raise ValueError(
                    "All files in a field merge split should have the same first row id and could not be null."
                )

        # Create the union reader
        all_read_fields = self.read_fields
        file_record_readers = [None] * len(fields_files)
        read_field_index = [field.id for field in all_read_fields]

        # Initialize offsets
        row_offsets = [-1] * len(all_read_fields)
        field_offsets = [-1] * len(all_read_fields)

        for i, bunch in enumerate(fields_files):
            first_file = bunch.files()[0]

            # Get field IDs for this bunch
            if self._is_blob_file(first_file.file_name):
                # For blob files, we need to get the field ID from the write columns
                field_ids = [self._get_field_id_from_write_cols(first_file)]
            elif first_file.write_cols:
                field_ids = self._get_field_ids_from_write_cols(first_file.write_cols)
            else:
                # For regular files, get all field IDs from the schema
                field_ids = [field.id for field in self.table.fields]

            read_fields = []
            for j, read_field_id in enumerate(read_field_index):
                for field_id in field_ids:
                    if read_field_id == field_id:
                        if row_offsets[j] == -1:
                            row_offsets[j] = i
                            field_offsets[j] = len(read_fields)
                            read_fields.append(all_read_fields[j])
                        break

            if not read_fields:
                file_record_readers[i] = None
            else:
                read_field_names = self._remove_partition_fields(read_fields)
                table_fields = self.read_fields
                self.read_fields = read_fields  # create reader based on read_fields
                # Create reader for this bunch
                if len(bunch.files()) == 1:
                    suppliers = [lambda r=self._create_file_reader(
                        bunch.files()[0], read_field_names
                    ): r]
                    file_record_readers[i] = MergeAllBatchReader(suppliers)
                else:
                    # Create concatenated reader for multiple files
                    suppliers = [
                        partial(self._create_file_reader, file=file,
                                read_fields=read_field_names) for file in bunch.files()
                    ]
                    file_record_readers[i] = MergeAllBatchReader(suppliers)
                self.read_fields = table_fields

        # Validate that all required fields are found
        for i, field in enumerate(all_read_fields):
            if row_offsets[i] == -1:
                if not field.type.nullable:
                    raise ValueError(f"Field {field} is not null but can't find any file contains it.")

        return DataEvolutionMergeReader(row_offsets, field_offsets, file_record_readers)

    def _create_file_reader(self, file: DataFileMeta, read_fields: [str]) -> Optional[RecordReader]:
        """Create a file reader for a single file."""
        # If the current file needs to be further divided for reading, use ShardBatchReader
        if file.file_name in self.split.shard_file_idx_map:
            (begin_row, end_row) = self.split.shard_file_idx_map[file.file_name]
            if (begin_row, end_row) == (-1, -1):
                return None
            else:
                return ShardBatchReader(self.file_reader_supplier(
                    file=file,
                    for_merge_read=False,
                    read_fields=read_fields,
                    row_tracking_enabled=True), begin_row, end_row)
        else:
            return self.file_reader_supplier(
                file=file,
                for_merge_read=False,
                read_fields=read_fields,
                row_tracking_enabled=True)

    def _split_field_bunches(self, need_merge_files: List[DataFileMeta]) -> List[FieldBunch]:
        """Split files into field bunches."""

        fields_files = []
        blob_bunch_map = {}
        row_count = -1

        for file in need_merge_files:
            if self._is_blob_file(file.file_name):
                field_id = self._get_field_id_from_write_cols(file)
                if field_id not in blob_bunch_map:
                    blob_bunch_map[field_id] = BlobBunch(row_count)
                blob_bunch_map[field_id].add(file)
            else:
                # Normal file, just add it to the current merge split
                fields_files.append(DataBunch(file))
                row_count = file.row_count

        fields_files.extend(blob_bunch_map.values())
        return fields_files

    def _get_field_id_from_write_cols(self, file: DataFileMeta) -> int:
        """Get field ID from write columns for blob files."""
        if not file.write_cols or len(file.write_cols) == 0:
            raise ValueError("Blob file must have write columns")

        # Find the field by name in the table schema
        field_name = file.write_cols[0]
        for field in self.table.fields:
            if field.name == field_name:
                return field.id
        raise ValueError(f"Field {field_name} not found in table schema")

    def _get_field_ids_from_write_cols(self, write_cols: List[str]) -> List[int]:
        field_ids = []
        for field_name in write_cols:
            for field in self.table.fields:
                if field.name == field_name:
                    field_ids.append(field.id)
        field_ids.append(SpecialFields.ROW_ID.id)
        field_ids.append(SpecialFields.SEQUENCE_NUMBER.id)
        return field_ids

    @staticmethod
    def _is_blob_file(file_name: str) -> bool:
        """Check if a file is a blob file based on its extension."""
        return file_name.endswith('.blob')

    def _get_all_data_fields(self):
        return SpecialFields.row_type_with_row_tracking(self.table.fields)
