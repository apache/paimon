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

import logging
import uuid
from abc import abstractmethod
from typing import List, Optional, Tuple

import pyarrow as pa

from pypaimon.common.options.core_options import CoreOptions
from pypaimon.data.timestamp import Timestamp
from pypaimon.manifest.schema.data_file_meta import DataFileMeta
from pypaimon.manifest.schema.simple_stats import SimpleStats
from pypaimon.table.row.generic_row import GenericRow
from pypaimon.write.writer.data_writer import DataWriter
from pypaimon.write.writer import stats_mode
from pypaimon.write.writer.write_buffer import WriteBuffer


logger = logging.getLogger(__name__)


class CompositeDataWriter(DataWriter):
    """Shared metadata handoff for normal files and blob/vector sidecars."""

    CHECK_ROLLING_RECORD_CNT = 1000

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.record_count = 0
        self.closed = False
        self._normal_buffer = WriteBuffer(self._merge_data)
        self._pending_normal_meta: Optional[DataFileMeta] = None
        self._committed_files_to_delete_on_abort: List[DataFileMeta] = []

    def prepare_commit(self) -> List[DataFileMeta]:
        self._close_current_writers()
        return self.committed_files.copy()

    def close(self):
        if self.closed:
            return
        try:
            self._close_current_writers()
        except Exception as e:
            logger.error("Exception occurs when closing writer. Cleaning up.", exc_info=e)
            self.abort()
            raise
        finally:
            self.closed = True
            self._normal_buffer.reset()

    def _should_roll_normal(self) -> bool:
        # Runs on every write, so it answers from the running counts only.
        if self._normal_buffer.is_empty:
            return False
        if self._normal_buffer.num_rows >= self.target_file_row_num:
            return True
        if self.record_count % self.CHECK_ROLLING_RECORD_CNT != 0:
            return False
        return self._normal_buffer.nbytes > self.target_file_size

    def _require_finished_flush(self):
        """Refuse to buffer more rows while a flush is only half done.

        A composite flush writes the normal data file first and the sidecars
        after. Once that file is on disk it covers exactly the rows flushed so
        far, so rows appended before the retry finishes would belong to no file
        the resumed flush writes.
        """
        if self._pending_normal_meta is not None:
            raise RuntimeError(
                "Cannot write: a previous flush left a data file that no commit "
                "has taken yet. Retry prepare_commit() to finish that flush, or "
                "abort() this writer.")

    def _close_current_writers(self):
        normal_meta = self._pending_normal_meta
        if normal_meta is None:
            normal_data = self._normal_buffer.materialize()
            if normal_data is not None and normal_data.num_rows > 0:
                normal_meta = self._write_normal_data_to_file(normal_data)
                self._pending_normal_meta = normal_meta
                # The parent owns this file as soon as it lands, even if a
                # sidecar later fails and nothing can be published yet.
                self._committed_files_to_delete_on_abort.append(normal_meta)
            self._normal_buffer.reset()

        # Prepare and validate all sidecars before publishing any metadata.
        # On failure children retain their metadata for retry and abort; the
        # normal file is remembered so a retry never writes those rows twice.
        prepared = self._prepare_sidecar_commits(normal_meta)
        if normal_meta is not None:
            self.committed_files.append(normal_meta)
        for _, metas in prepared:
            self.committed_files.extend(metas)
        for writer, _ in prepared:
            self._committed_files_to_delete_on_abort.extend(writer._release_prepared_files())

        self._pending_normal_meta = None
        self.record_count = 0

    @abstractmethod
    def _prepare_sidecar_commits(
            self, normal_meta: Optional[DataFileMeta]
    ) -> List[Tuple[DataWriter, List[DataFileMeta]]]:
        """Return all children and metadata in order after preparation and validation.

        Children retain their metadata and cleanup responsibility until the parent
        accepts the complete result.
        """

    def _write_normal_data_to_file(self, data: pa.Table) -> Optional[DataFileMeta]:
        if data.num_rows == 0:
            return None

        file_name = f"{CoreOptions.data_file_prefix(self.options)}{uuid.uuid4()}-0.{self.file_format}"
        file_path = self._generate_file_path(file_name)

        try:
            if self.file_format == CoreOptions.FILE_FORMAT_BLOB:
                raise ValueError(f"Unsupported file format: {self.file_format}")
            shredding_stats = self._write_file(file_path, data, self.file_format)

            is_external_path = self.external_path_provider is not None
            external_path_str = file_path if is_external_path else None

            # Value stats honor metadata.stats-mode (not just full): none
            # records nothing, counts keeps null counts, truncate(N)
            # truncates min/max, full keeps them.
            value_stats_enabled = self._value_stats_on
            stats_columns = self.normal_columns if value_stats_enabled else []
            if value_stats_enabled and self._stats_mode_kind != stats_mode.FULL:
                column_stats = {
                    column.name: self._get_column_stats(data, column.name)
                    for column in stats_columns
                }
                value_stats = self._collect_value_stats(
                    data, stats_columns,
                    self._converted_value_column_stats(stats_columns, column_stats))
            else:
                value_stats = self._collect_value_stats(data, stats_columns)

            min_seq, max_seq = self._append_file_sequence_range(data.num_rows)

            meta = DataFileMeta.create(
                file_name=file_name,
                file_size=self.file_io.get_file_size(file_path),
                row_count=data.num_rows,
                min_key=GenericRow([], []),
                max_key=GenericRow([], []),
                key_stats=SimpleStats.empty_stats(),
                value_stats=value_stats,
                min_sequence_number=min_seq,
                max_sequence_number=max_seq,
                schema_id=self.table.table_schema.id,
                level=0,
                extra_files=[],
                creation_time=Timestamp.now(),
                delete_row_count=0,
                file_source=0,
                value_stats_cols=[column.name for column in stats_columns],
                external_path=external_path_str,
                file_path=file_path,
                write_cols=self.write_cols,
            )
            self._map_shared_shredding.file_completed(shredding_stats)
            return meta
        except Exception:
            self.file_io.delete_quietly(file_path)
            raise

    def abort(self):
        # Subclasses abort children first to clean up files not yet handed off.
        # This list holds normal files and sidecars whose cleanup was transferred
        # to the parent; externally owned BlobConsumer files are excluded.
        self._delete_committed_files(
            self._committed_files_to_delete_on_abort + self.committed_changelog_files)
        self._pending_normal_meta = None
        self._normal_buffer.reset()
        self._buffer.reset()
        self.committed_files.clear()
        self.committed_changelog_files.clear()
        self._committed_files_to_delete_on_abort.clear()
