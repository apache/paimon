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

"""Native Lance reader wrapper for reading Lance format files."""

import logging
from typing import List, Optional, Dict, Any, TYPE_CHECKING

if TYPE_CHECKING:
    import pyarrow as pa
    from pyarrow import RecordBatch
else:
    pa = None
    RecordBatch = None

logger = logging.getLogger(__name__)


class LanceNativeReader:
    """
    Wrapper for Lance native reader to read Lance format files.
    
    This class handles reading data from Lance-formatted files using the
    pylance library (Lance Python bindings).
    """

    def __init__(self,
                 file_path: str,
                 columns: Optional[List[str]] = None,
                 batch_size: int = 4096,
                 storage_options: Optional[Dict[str, str]] = None):
        """
        Initialize Lance native reader.
        
        Args:
            file_path: Path to the Lance file
            columns: List of columns to read (None means all columns)
            batch_size: Number of rows per batch
            storage_options: Storage backend options (for S3, OSS, etc.)
        """
        self.file_path = file_path
        self.columns = columns
        self.batch_size = batch_size
        self.storage_options = storage_options or {}
        
        self._table = None
        self._reader = None
        self._batch_index = 0
        
        try:
            import lance
            self._lance = lance
        except ImportError:
            raise ImportError(
                "Lance library is not installed. "
                "Please install it with: pip install lance"
            )
        
        self._initialize_reader()

    def _initialize_reader(self) -> None:
        """Initialize the Lance reader and load table metadata."""
        import pyarrow as pa
        
        try:
            # Open Lance dataset using lancedb API
            import lancedb
            self._table = lancedb.connect(self.file_path).open_table(
                self.file_path
            )
            logger.info(f"Successfully opened Lance file: {self.file_path}")
            logger.debug(f"Schema: {self._table.schema}")
            logger.debug(f"Number of rows: {len(self._table)}")
            
        except ImportError:
            # Fallback: Try using lance directly if lancedb not available
            try:
                self._table = self._lance.open(self.file_path)
                logger.info(f"Successfully opened Lance file: {self.file_path}")
            except Exception as e:
                logger.error(f"Failed to open Lance file {self.file_path}: {e}")
                raise
        except Exception as e:
            logger.error(f"Failed to open Lance file {self.file_path}: {e}")
            raise

    def read_batch(self) -> Optional[Any]:
        """
        Read next batch of data from Lance file.
        
        Returns:
            PyArrow RecordBatch with data, or None if EOF reached
        """
        try:
            if self._table is None:
                return None
            
            total_rows = len(self._table)
            if self._batch_index >= total_rows:
                return None
            
            # Calculate batch boundaries
            end_row = min(self._batch_index + self.batch_size, total_rows)
            
            # Read batch with optional column projection
            if self.columns:
                batch_table = self._table.select(self.columns)\
                    .slice(self._batch_index, end_row - self._batch_index)
            else:
                batch_table = self._table.slice(self._batch_index, 
                                               end_row - self._batch_index)
            
            self._batch_index = end_row
            
            # Convert to single RecordBatch
            if batch_table.num_rows > 0:
                return batch_table.to_batches()[0]
            else:
                return None
                
        except Exception as e:
            logger.error(f"Error reading batch from Lance file: {e}")
            raise

    def get_schema(self) -> Any:
        """Get the schema of the Lance file."""
        if self._table is None:
            raise RuntimeError("Reader not initialized")
        return self._table.schema

    def get_row_count(self) -> int:
        """Get the total number of rows in the Lance file."""
        if self._table is None:
            raise RuntimeError("Reader not initialized")
        return len(self._table)

    def close(self) -> None:
        """Close the reader and release resources."""
        try:
            if self._reader is not None:
                self._reader = None
            if self._table is not None:
                self._table = None
            logger.debug(f"Successfully closed Lance reader for {self.file_path}")
        except Exception as e:
            logger.warning(f"Error closing Lance reader: {e}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

    def __iter__(self):
        """Make reader iterable."""
        self._batch_index = 0
        return self

    def __next__(self) -> Any:
        """Get next batch."""
        batch = self.read_batch()
        if batch is None:
            raise StopIteration
        return batch
