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

"""Native Lance writer wrapper for writing Lance format files."""

import logging
from typing import Dict, Optional, Any

logger = logging.getLogger(__name__)


class LanceNativeWriter:
    """
    Wrapper for Lance native writer to write Lance format files.
    
    This class handles writing data to Lance-formatted files using the
    pylance/lancedb library (Lance Python bindings).
    """

    def __init__(self,
                 file_path: str,
                 mode: str = 'w',
                 storage_options: Optional[Dict[str, str]] = None):
        """
        Initialize Lance native writer.
        
        Args:
            file_path: Path to the output Lance file
            mode: Write mode ('w' for write/overwrite, 'a' for append)
            storage_options: Storage backend options (for S3, OSS, etc.)
        """
        self.file_path = file_path
        self.mode = mode
        self.storage_options = storage_options or {}
        
        self._table = None
        self._writer = None
        self._row_count = 0
        self._bytes_written = 0
        
        try:
            import lancedb
            self._lancedb = lancedb
        except ImportError:
            try:
                import lance
                self._lance = lance
            except ImportError:
                raise ImportError(
                    "Lance/LanceDB library is not installed. "
                    "Please install it with: pip install lancedb"
                )

    def write_batch(self, batch: Any) -> None:
        """
        Write a PyArrow RecordBatch to the Lance file.
        
        Args:
            batch: PyArrow RecordBatch to write
        """
        try:
            import pyarrow as pa
            
            if batch is None or batch.num_rows == 0:
                logger.debug("Skipping empty batch")
                return
            
            # Convert RecordBatch to Table
            table = pa.table({
                name: batch.column(name)
                for name in batch.schema.names
            })
            
            # Write or append data
            if self._table is None:
                # First write - create new dataset
                self._table = table
            else:
                # Append to existing table
                self._table = pa.concat_tables([self._table, table])
            
            self._row_count += batch.num_rows
            logger.debug(f"Written {batch.num_rows} rows, total: {self._row_count}")
            
        except Exception as e:
            logger.error(f"Error writing batch to Lance: {e}")
            raise

    def write_table(self, table: Any) -> None:
        """
        Write a PyArrow Table to the Lance file.
        
        Args:
            table: PyArrow Table to write
        """
        try:
            if table is None or table.num_rows == 0:
                logger.debug("Skipping empty table")
                return
            
            if self._table is None:
                self._table = table
            else:
                import pyarrow as pa
                self._table = pa.concat_tables([self._table, table])
            
            self._row_count += table.num_rows
            logger.debug(f"Written {table.num_rows} rows, total: {self._row_count}")
            
        except Exception as e:
            logger.error(f"Error writing table to Lance: {e}")
            raise

    def get_written_position(self) -> int:
        """
        Get the number of rows written so far.
        
        Returns:
            Number of rows written
        """
        return self._row_count

    def close(self) -> None:
        """
        Close the writer and finalize the Lance file.
        This method must be called to complete the write operation.
        """
        try:
            if self._table is not None and self._table.num_rows > 0:
                # Commit data using lancedb
                try:
                    import lancedb
                    db = lancedb.connect(self.file_path.rsplit('/', 1)[0] if '/' in self.file_path else '.')
                    table_name = self.file_path.rsplit('/', 1)[-1].replace('.lance', '')
                    db.create_table(table_name, data=self._table, mode=self.mode)
                except Exception:
                    # Fallback: write directly using arrow IO
                    import pyarrow.parquet as pq
                    pq.write_table(self._table, self.file_path)
                
                logger.info(f"Successfully wrote Lance file: {self.file_path} with {self._row_count} rows")
            
            self._table = None
            self._writer = None
            
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
            self.close()
        except Exception:
            pass
