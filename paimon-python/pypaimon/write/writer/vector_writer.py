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

import uuid
from typing import List, Tuple

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.data.timestamp import Timestamp
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.writer.append_only_data_writer import AppendOnlyDataWriter


class VectorWriter(AppendOnlyDataWriter):
    """Writer for all vector columns stored in a single file (.vector.<format>).

    Matches Java behavior: all vector columns go into one .vector.<format> file,
    not one file per column.
    """

    def __init__(self, table, partition: Tuple, bucket: int, max_seq_number: int,
                 vector_columns: List[str], vector_file_format: str, options: CoreOptions = None):
        super().__init__(table, partition, bucket, max_seq_number,
                         options, write_cols=vector_columns)
        self.vector_columns = vector_columns
        self.vector_file_format = vector_file_format
        self.file_format = vector_file_format
        self.target_file_size = options.vector_target_file_size()
        self.file_uuid = str(uuid.uuid4())
        self.file_count = 0

    def _write_data_to_file(self, data: pa.Table):
        if data.num_rows == 0:
            return

        file_name = (f"{CoreOptions.data_file_prefix(self.options)}"
                     f"{self.file_uuid}-{self.file_count}.vector.{self.vector_file_format}")
        self.file_count += 1
        file_path = self._generate_file_path(file_name)

        is_external_path = self.external_path_provider is not None
        external_path_str = file_path if is_external_path else None

        if self.vector_file_format == CoreOptions.FILE_FORMAT_LANCE:
            self.file_io.write_lance(file_path, data)
        elif self.vector_file_format == CoreOptions.FILE_FORMAT_PARQUET:
            self.file_io.write_parquet(file_path, data,
                                       compression=self.compression,
                                       zstd_level=self.zstd_level)
        elif self.vector_file_format == CoreOptions.FILE_FORMAT_VORTEX:
            self.file_io.write_vortex(file_path, data)
        else:
            raise ValueError(f"Unsupported vector file format: {self.vector_file_format}")

        min_seq = self.sequence_generator.start
        max_seq = self.sequence_generator.current
        self.sequence_generator.start = self.sequence_generator.current

        self.committed_files.append(DataFileMeta.create(
            file_name=file_name,
            file_size=self.file_io.get_file_size(file_path),
            row_count=data.num_rows,
            min_key=GenericRow([], []),
            max_key=GenericRow([], []),
            key_stats=SimpleStats.empty_stats(),
            value_stats=SimpleStats.empty_stats(),
            min_sequence_number=min_seq,
            max_sequence_number=max_seq,
            schema_id=self.table.table_schema.id,
            level=0,
            extra_files=[],
            creation_time=Timestamp.now(),
            delete_row_count=0,
            file_source=0,
            value_stats_cols=None,
            external_path=external_path_str,
            first_row_id=None,
            write_cols=self.write_cols,
            file_path=file_path,
        ))
