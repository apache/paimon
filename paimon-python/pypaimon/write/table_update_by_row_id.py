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

from abc import ABCMeta
from typing import TYPE_CHECKING, List, Optional

import pyarrow as pa

from pypaimon.write.commit_message import CommitMessage
from pypaimon.write.row_input import RowIdUpdateInputMixin

if TYPE_CHECKING:
    from pypaimon.execution.python.write.row_id_file_index import RowIdFileIndex


class TableUpdateByRowId(RowIdUpdateInputMixin, metaclass=ABCMeta):
    """Incrementally prepare updates addressed by stable row IDs."""

    FIRST_ROW_ID_COLUMN = '_FIRST_ROW_ID'

    def __init__(self, table, commit_user: str, commit_identifier: int,
                 _precomputed_files_info: Optional['RowIdFileIndex'] = None):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin.__init__(self, table, commit_user, commit_identifier, _precomputed_files_info)

    def update_columns(self, data: pa.Table, column_names: List[str]) -> List[CommitMessage]:
        """
        Add or update columns in the table.

        Args:
            data: Input data containing row_id and columns to update
            column_names: Names of columns to update (excluding row_id)

        Returns:
            List of commit messages
        """
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin.update_columns(self, data, column_names)

    def _snapshot_files_info(self):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin._snapshot_files_info(self)

    def _load_existing_files_info(self):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin._load_existing_files_info(self)

    def _write_row_columns(self, update_data, column_names, blob_object_columns):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin._write_row_columns(self, update_data, column_names, blob_object_columns)

    def _calculate_first_row_id(self, data):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin._calculate_first_row_id(self, data)

    def _write_by_first_row_id(self, data, column_names, blob_object_columns=None):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin._write_by_first_row_id(self, data, column_names, blob_object_columns)

    def _read_original_file_data(self, first_row_id, column_names):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin._read_original_file_data(self, first_row_id, column_names)

    def _original_file_read(self, first_row_id, column_names):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin._original_file_read(self, first_row_id, column_names)

    def _merged_batches(self, first_row_id, data, column_names):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin._merged_batches(self, first_row_id, data, column_names)

    def _write_group_streaming(self, partition, first_row_id, data, column_names):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin._write_group_streaming(self, partition, first_row_id, data, column_names)

    def _merge_update_with_original(self, original_data, update_data, column_names,
                                    first_row_id, blob_object_columns=None, blob_columns_with_baseline=None):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin._merge_update_with_original(
            self, original_data, update_data, column_names, first_row_id,
            blob_object_columns, blob_columns_with_baseline)

    def _normalize_write_data(self, data):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin._normalize_write_data(self, data)

    def _check_write_not_null(self, column_name, has_null):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin._check_write_not_null(self, column_name, has_null)

    @classmethod
    def _merge_chunked_column(cls, original_col, update_col, sorted_updates):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin._merge_chunked_column.__func__(cls, original_col, update_col, sorted_updates)

    @classmethod
    def _merge_chunk_with_updates(cls, original, update_col, update_chunk_offsets, updates):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin._merge_chunk_with_updates.__func__(
            cls, original, update_col, update_chunk_offsets, updates)

    @staticmethod
    def _chunk_offsets(column):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin._chunk_offsets(column)

    @staticmethod
    def _take_from_chunked_array(column, chunk_offsets, indices):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin._take_from_chunked_array(column, chunk_offsets, indices)

    @staticmethod
    def _is_offset_overflow(error):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin._is_offset_overflow(error)

    @staticmethod
    def _coerce_column(col, target_type):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin._coerce_column(col, target_type)

    def _blob_placeholder(self, column_name):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin._blob_placeholder(self, column_name)

    def _write_group(self, partition, first_row_id, data, column_names, blob_object_columns=None):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin._write_group(
            self, partition, first_row_id, data, column_names, blob_object_columns)

    @staticmethod
    def _assign_update_file_metadata(new_files, first_row_id, column_names, blob_columns):
        from pypaimon.execution.python.write.table_update_by_row_id import PythonRowIdUpdateMixin
        return PythonRowIdUpdateMixin._assign_update_file_metadata(new_files, first_row_id, column_names, blob_columns)
