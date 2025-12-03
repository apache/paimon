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

"""Lance format reader implementation for Paimon."""

import logging
from typing import List, Optional, Any

from pypaimon.common.file_io import FileIO
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.reader.lance.lance_native_reader import LanceNativeReader
from pypaimon.read.reader.lance.lance_utils import LanceUtils

logger = logging.getLogger(__name__)


class FormatLanceReader(RecordBatchReader):
    """
    Lance format reader for reading Lance-formatted data files.
    
    This reader integrates Lance format support into Paimon's read pipeline,
    handling column projection, predicate push-down, and batch reading.
    """

    def __init__(self,
                 file_io: FileIO,
                 file_path: str,
                 read_fields: List[str],
                 push_down_predicate: Any = None,
                 batch_size: int = 4096,
                 selection_ranges: Optional[List[tuple]] = None):
        """
        Initialize Lance format reader.
        
        Args:
            file_io: Paimon FileIO instance for file access
            file_path: Path to the Lance file
            read_fields: List of column names to read
            push_down_predicate: Optional predicate for filtering (not yet supported)
            batch_size: Number of rows per batch
            selection_ranges: Optional row ranges to select
        """
        self.file_io = file_io
        self.file_path = file_io.to_filesystem_path(file_path) if hasattr(file_io, 'to_filesystem_path') else str(file_path)
        self.read_fields = read_fields
        self.push_down_predicate = push_down_predicate
        self.batch_size = batch_size
        self.selection_ranges = selection_ranges
        
        self._native_reader: Optional[LanceNativeReader] = None
        self._initialized = False
        
        try:
            self._initialize_reader()
        except ImportError:
            logger.error("Lance library not available. Please install: pip install lance")
            raise

    def _initialize_reader(self) -> None:
        """Initialize the native Lance reader."""
        try:
            # Get storage options for cloud storage support
            storage_options = LanceUtils.convert_to_lance_storage_options(
                self.file_io, 
                self.file_path
            )
            
            # Create native reader with column projection
            self._native_reader = LanceNativeReader(
                file_path=self.file_path,
                columns=self.read_fields if self.read_fields else None,
                batch_size=self.batch_size,
                storage_options=storage_options
            )
            
            self._initialized = True
            logger.info(f"Successfully initialized Lance reader for {self.file_path}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Lance reader: {e}")
            raise

    def read_arrow_batch(self) -> Optional[Any]:
        """
        Read next batch of data from Lance file.
        
        Returns:
            PyArrow RecordBatch with selected columns, or None if EOF
        """
        if not self._initialized or self._native_reader is None:
            return None
        
        try:
            batch = self._native_reader.read_batch()
            
            if batch is None:
                return None
            
            # Apply row range selection if specified
            if self.selection_ranges:
                batch = self._apply_row_selection(batch)
            
            # Note: Predicate push-down is not yet implemented
            # Full batch filtering would be applied at a higher level
            
            return batch
            
        except Exception as e:
            logger.error(f"Error reading batch from Lance file: {e}")
            raise

    def _apply_row_selection(self, batch: Any) -> Optional[Any]:
        """
        Apply row range selection to the batch.
        
        Args:
            batch: PyArrow RecordBatch
            
        Returns:
            Filtered RecordBatch or None if no rows match
        """
        try:
            import pyarrow as pa
            
            if not self.selection_ranges or batch.num_rows == 0:
                return batch
            
            # Create a mask for selected rows
            mask = [False] * batch.num_rows
            for start, end in self.selection_ranges:
                for i in range(start, min(end, batch.num_rows)):
                    if i < batch.num_rows:
                        mask[i] = True
            
            # Apply mask to batch
            mask_array = pa.array(mask)
            filtered_batch = batch.filter(mask_array)
            
            return filtered_batch if filtered_batch.num_rows > 0 else None
            
        except Exception as e:
            logger.warning(f"Failed to apply row selection: {e}")
            return batch

    def close(self) -> None:
        """Close the reader and release resources."""
        if self._native_reader is not None:
            try:
                self._native_reader.close()
            except Exception as e:
                logger.warning(f"Error closing native reader: {e}")
            finally:
                self._native_reader = None
        
        self._initialized = False
        logger.debug(f"Closed Lance reader for {self.file_path}")

    def __del__(self):
        """Destructor to ensure cleanup."""
        self.close()
