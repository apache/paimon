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

"""Lance format writer implementation for Paimon."""

import logging
from typing import Any, Optional, Dict, List

logger = logging.getLogger(__name__)


class LanceFormatWriter:
    """
    Lance format writer for writing data to Lance-formatted files.

    This writer implements the Paimon format writer interface and handles
    writing data in Lance format, supporting batch accumulation and proper
    file finalization.
    """

    def __init__(self,
                 file_path: str,
                 schema: Any,
                 batch_size: int = 1024,
                 storage_options: Optional[Dict[str, str]] = None,
                 **kwargs: Any):
        """
        Initialize Lance format writer.

        Args:
            file_path: Output file path for the Lance file
            schema: PyArrow schema for the data
            batch_size: Maximum rows to accumulate before flushing
            storage_options: Optional storage backend configuration
            **kwargs: Additional options passed to underlying writer
        """
        self.file_path = file_path
        self.schema = schema
        self.batch_size = batch_size
        self.storage_options = storage_options or {}

        # Data accumulation for batching
        self._accumulated_data: List[Dict[str, Any]] = []
        self._written_bytes = 0
        self._native_writer = None
        self._closed = False

        try:
            from pypaimon.write.writer.lance.lance_native_writer import LanceNativeWriter
            self._LanceNativeWriter = LanceNativeWriter
        except ImportError:
            logger.error("Failed to import LanceNativeWriter")
            raise

    def add_row(self, row: Any) -> None:
        """
        Add a row to the writer.

        Args:
            row: Data row to write (typically InternalRow)
        """
        try:
            if row is None:
                return

            # Convert InternalRow to dict if needed
            if hasattr(row, 'to_dict'):
                row_dict = row.to_dict()
            elif isinstance(row, dict):
                row_dict = row
            else:
                logger.warning(f"Unsupported row type: {type(row)}")
                return

            self._accumulated_data.append(row_dict)

            # Flush if batch size exceeded
            if len(self._accumulated_data) >= self.batch_size:
                self._flush_batch()

        except Exception as e:
            logger.error(f"Error adding row: {e}")
            raise

    def write_batch(self, batch: Any) -> None:
        """
        Write a PyArrow RecordBatch.

        Args:
            batch: PyArrow RecordBatch to write
        """
        try:
            if batch is None or batch.num_rows == 0:
                return

            # Ensure native writer is initialized
            if self._native_writer is None:
                self._native_writer = self._LanceNativeWriter(
                    self.file_path,
                    mode='w',
                    storage_options=self.storage_options
                )

            # Write batch directly
            self._native_writer.write_batch(batch)
            self._written_bytes += batch.nbytes if hasattr(batch, 'nbytes') else 0

        except Exception as e:
            logger.error(f"Error writing batch: {e}")
            raise

    def _flush_batch(self) -> None:
        """Flush accumulated row data as a batch."""
        if not self._accumulated_data:
            return

        try:
            import pyarrow as pa

            # Ensure native writer is initialized
            if self._native_writer is None:
                self._native_writer = self._LanceNativeWriter(
                    self.file_path,
                    mode='w',
                    storage_options=self.storage_options
                )

            # Convert accumulated data to Arrow Table
            table = pa.Table.from_pylist(self._accumulated_data, schema=self.schema)
            self._native_writer.write_table(table)

            # Track bytes written
            if hasattr(table, 'nbytes'):
                self._written_bytes += table.nbytes

            # Clear accumulated data
            self._accumulated_data.clear()

            logger.debug(f"Flushed batch of {table.num_rows} rows")

        except Exception as e:
            logger.error(f"Error flushing batch: {e}")
            raise

    def reach_target_size(self, suggested_check: bool, target_size: int) -> bool:
        """
        Check if the writer has reached target file size.

        Args:
            suggested_check: Whether check is suggested
            target_size: Target file size in bytes

        Returns:
            True if target size reached, False otherwise
        """
        if not suggested_check:
            return False

        return self._written_bytes >= target_size

    def get_written_position(self) -> int:
        """
        Get the current written byte position.

        Returns:
            Number of bytes written
        """
        if self._native_writer is not None:
            # Native writer tracks row count, estimate bytes
            rows = self._native_writer.get_written_position()
            # Rough estimation: average row size estimation
            if rows > 0:
                return max(self._written_bytes, rows * 1024)

        return self._written_bytes

    def close(self) -> None:
        """
        Close the writer and finalize the file.
        Must be called to ensure data is properly written.
        """
        if self._closed:
            return

        try:
            # Flush any remaining accumulated data
            self._flush_batch()

            # Close native writer
            if self._native_writer is not None:
                self._native_writer.close()
                self._native_writer = None

            self._closed = True
            logger.info(f"Successfully closed Lance writer for {self.file_path}")

        except Exception as e:
            logger.error(f"Error closing Lance writer: {e}")
            raise

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def __del__(self):
        """Destructor to ensure cleanup."""
        try:
            if not self._closed:
                self.close()
        except Exception:
            pass
