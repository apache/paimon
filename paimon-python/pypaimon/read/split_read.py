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
from typing import List, Optional, Tuple

from pypaimon.common.predicate import Predicate
from pypaimon.read.interval_partition import IntervalPartition, SortedRun
from pypaimon.read.partition_info import PartitionInfo
from pypaimon.read.reader.concat_batch_reader import ShardBatchReader, ConcatBatchReader
from pypaimon.read.reader.concat_record_reader import ConcatRecordReader
from pypaimon.read.reader.data_file_record_reader import DataFileBatchReader
from pypaimon.read.reader.drop_delete_reader import DropDeleteRecordReader
from pypaimon.read.reader.empty_record_reader import EmptyFileRecordReader
from pypaimon.read.reader.filter_record_reader import FilterRecordReader
from pypaimon.read.reader.format_avro_reader import FormatAvroReader
from pypaimon.read.reader.format_pyarrow_reader import FormatPyArrowReader
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.reader.iface.record_reader import RecordReader
from pypaimon.read.reader.key_value_unwrap_reader import \
    KeyValueUnwrapRecordReader
from pypaimon.read.reader.key_value_wrap_reader import KeyValueWrapReader
from pypaimon.read.reader.sort_merge_reader import SortMergeReaderWithMinHeap
from pypaimon.read.split import Split
from pypaimon.schema.data_types import AtomicType, DataField

KEY_PREFIX = "_KEY_"
KEY_FIELD_ID_START = 1000000
NULL_FIELD_INDEX = -1


class SplitRead(ABC):
    """Abstract base class for split reading operations."""

    def __init__(self, table, predicate: Optional[Predicate], push_down_predicate,
                 read_type: List[DataField], split: Split):
        from pypaimon.table.file_store_table import FileStoreTable

        self.table: FileStoreTable = table
        self.predicate = predicate
        self.push_down_predicate = push_down_predicate
        self.split = split
        self.value_arity = len(read_type)

        self.trimmed_primary_key = [field.name for field in self.table.table_schema.get_trimmed_primary_key_fields()]
        self.read_fields = read_type
        if isinstance(self, MergeFileSplitRead):
            self.read_fields = self._create_key_value_fields(read_type)

    @abstractmethod
    def create_reader(self) -> RecordReader:
        """Create a record reader for the given split."""

    def file_reader_supplier(self, file_path: str, for_merge_read: bool):
        _, extension = os.path.splitext(file_path)
        file_format = extension[1:]

        format_reader: RecordBatchReader
        if file_format == "avro":
            format_reader = FormatAvroReader(self.table.file_io, file_path, self._get_final_read_data_fields(),
                                             self.read_fields, self.push_down_predicate)
        elif file_format == "parquet" or file_format == "orc":
            format_reader = FormatPyArrowReader(self.table.file_io, file_format, file_path,
                                                self._get_final_read_data_fields(), self.push_down_predicate)
        else:
            raise ValueError(f"Unexpected file format: {file_format}")

        index_mapping = self.create_index_mapping()
        partition_info = self.create_partition_info()
        if for_merge_read:
            return DataFileBatchReader(format_reader, index_mapping, partition_info, self.trimmed_primary_key,
                                       self.table.table_schema.fields)
        else:
            return DataFileBatchReader(format_reader, index_mapping, partition_info, None,
                                       self.table.table_schema.fields)

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

        sequence_field = DataField(2147483646, "_SEQUENCE_NUMBER", AtomicType("BIGINT", nullable=False))
        all_data_fields.append(sequence_field)
        value_kind_field = DataField(2147483645, "_VALUE_KIND", AtomicType("TINYINT", nullable=False))
        all_data_fields.append(value_kind_field)

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

    def create_partition_info(self):
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


class RawFileSplitRead(SplitRead):

    def create_reader(self) -> RecordReader:
        data_readers = []
        for file_path in self.split.file_paths:
            supplier = partial(self.file_reader_supplier, file_path=file_path, for_merge_read=False)
            data_readers.append(supplier)

        if not data_readers:
            return EmptyFileRecordReader()
        if self.split.files[0].file_start_row is not None:
            concat_reader = ShardBatchReader(data_readers, self.split)
        else:
            concat_reader = ConcatBatchReader(data_readers)
        # if the table is appendonly table, we don't need extra filter, all predicates has pushed down
        if self.table.is_primary_key_table and self.predicate:
            return FilterRecordReader(concat_reader, self.predicate)
        else:
            return concat_reader

    def _get_all_data_fields(self):
        return self.table.fields


class MergeFileSplitRead(SplitRead):
    def kv_reader_supplier(self, file_path):
        reader_supplier = partial(self.file_reader_supplier, file_path=file_path, for_merge_read=True)
        return KeyValueWrapReader(reader_supplier(), len(self.trimmed_primary_key), self.value_arity)

    def section_reader_supplier(self, section: List[SortedRun]):
        readers = []
        for sorter_run in section:
            data_readers = []
            for file in sorter_run.files:
                supplier = partial(self.kv_reader_supplier, file.file_path)
                data_readers.append(supplier)
            readers.append(ConcatRecordReader(data_readers))
        return SortMergeReaderWithMinHeap(readers, self.table.table_schema)

    def create_reader(self) -> RecordReader:
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
