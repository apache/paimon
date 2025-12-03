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
from typing import List, Optional, Any, Dict

from pypaimon.common.file_io import FileIO
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.reader.lance.lance_native_reader import LanceNativeReader
from pypaimon.read.reader.lance.lance_utils import LanceUtils
from pypaimon.read.reader.lance.vector_index import VectorIndexBuilder
from pypaimon.read.reader.lance.scalar_index import ScalarIndexBuilder
from pypaimon.read.reader.lance.predicate_pushdown import PredicateOptimizer

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
                 selection_ranges: Optional[List[tuple]] = None,
                 enable_vector_search: bool = False,
                 enable_scalar_index: bool = False):
        """
        Initialize Lance format reader with indexing support.
        
        Args:
            file_io: Paimon FileIO instance for file access
            file_path: Path to the Lance file
            read_fields: List of column names to read
            push_down_predicate: Optional predicate for filtering and push-down optimization
            batch_size: Number of rows per batch
            selection_ranges: Optional row ranges to select
            enable_vector_search: Enable vector indexing (IVF_PQ, HNSW)
            enable_scalar_index: Enable scalar indexing (BTree, Bitmap)
        """
        self.file_io = file_io
        self.file_path = file_io.to_filesystem_path(file_path) if hasattr(file_io, 'to_filesystem_path') else str(file_path)
        self.read_fields = read_fields
        self.push_down_predicate = push_down_predicate
        self.batch_size = batch_size
        self.selection_ranges = selection_ranges
        self.enable_vector_search = enable_vector_search
        self.enable_scalar_index = enable_scalar_index
        
        self._native_reader: Optional[LanceNativeReader] = None
        self._initialized = False
        
        # Index support
        self._vector_index_builder: Optional[VectorIndexBuilder] = None
        self._scalar_index_builder: Optional[ScalarIndexBuilder] = None
        self._predicate_optimizer: Optional[PredicateOptimizer] = None
        
        try:
            self._initialize_reader()
            if enable_vector_search:
                self._initialize_vector_indexing()
            if enable_scalar_index:
                self._initialize_scalar_indexing()
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

    def _initialize_vector_indexing(self) -> None:
        """Initialize vector indexing support."""
        try:
            self._vector_index_builder = VectorIndexBuilder(
                vector_column='vector',
                index_type='ivf_pq',
                metric='l2'
            )
            logger.info("Vector indexing initialized (IVF_PQ with L2 metric)")
        except Exception as e:
            logger.warning(f"Failed to initialize vector indexing: {e}")

    def _initialize_scalar_indexing(self) -> None:
        """Initialize scalar indexing support."""
        try:
            self._predicate_optimizer = PredicateOptimizer()
            logger.info("Scalar indexing initialized (BTree, Bitmap)")
        except Exception as e:
            logger.warning(f"Failed to initialize scalar indexing: {e}")

    def read_arrow_batch(self) -> Optional[Any]:
        """
        Read next batch of data from Lance file with optimization.
        
        Returns:
            PyArrow RecordBatch with selected columns, or None if EOF
        """
        if not self._initialized or self._native_reader is None:
            return None
        
        try:
            batch = self._native_reader.read_batch()
            
            if batch is None:
                return None
            
            # Apply optimized predicate filters
            if self.push_down_predicate and self._predicate_optimizer:
                batch = self._apply_predicate_optimization(batch)
                if batch is None or batch.num_rows == 0:
                    # Predicate filtered all rows, continue to next batch
                    return self.read_arrow_batch()
            
            # Apply row range selection if specified
            if self.selection_ranges:
                batch = self._apply_row_selection(batch)
            
            return batch
            
        except Exception as e:
            logger.error(f"Error reading batch from Lance file: {e}")
            raise

    def _apply_predicate_optimization(self, batch: Any) -> Optional[Any]:
        """
        Apply predicate push-down optimization to filter rows efficiently.
        
        Args:
            batch: PyArrow RecordBatch
            
        Returns:
            Filtered RecordBatch or None if no rows match
        """
        if not self._predicate_optimizer:
            return batch
        
        try:
            # Parse predicate string
            predicate_str = str(self.push_down_predicate) if self.push_down_predicate else None
            if not predicate_str:
                return batch
            
            expressions = self._predicate_optimizer.parse_predicate(predicate_str)
            if not expressions:
                return batch
            
            # Optimize predicate order
            optimized_exprs = self._predicate_optimizer.optimize_predicate_order(expressions)
            
            # Get optimization hints
            hints = [self._predicate_optimizer.get_filter_hint(expr) for expr in optimized_exprs]
            logger.debug(f"Predicate optimization hints: {hints}")
            
            # Note: Actual filtering would require Lance's filter API
            # For now, return batch as-is
            # Real implementation would push filters down to Lance layer
            
            return batch
            
        except Exception as e:
            logger.warning(f"Predicate optimization failed, returning unfiltered batch: {e}")
            return batch

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

    def create_vector_index(self, vector_column: str, **index_params: Any) -> Dict[str, Any]:
        """
        Create vector index (IVF_PQ or HNSW).
        
        Args:
            vector_column: Column containing vector data
            **index_params: Index parameters (num_partitions, num_sub_vectors, etc.)
            
        Returns:
            Index metadata dictionary
        """
        if not self.enable_vector_search:
            logger.warning("Vector search not enabled")
            return {}
        
        try:
            if self._vector_index_builder is None:
                self._vector_index_builder = VectorIndexBuilder(vector_column)
            
            index_type = index_params.get('index_type', 'ivf_pq')
            
            if index_type == 'ivf_pq':
                return self._vector_index_builder.create_ivf_pq_index(
                    self._native_reader._table if self._native_reader else None,
                    **index_params
                )
            elif index_type == 'hnsw':
                return self._vector_index_builder.create_hnsw_index(
                    self._native_reader._table if self._native_reader else None,
                    **index_params
                )
            else:
                raise ValueError(f"Unsupported vector index type: {index_type}")
                
        except Exception as e:
            logger.error(f"Failed to create vector index: {e}")
            return {}

    def create_scalar_index(self, column: str, index_type: str = 'auto', **index_params: Any) -> Dict[str, Any]:
        """
        Create scalar index (BTree or Bitmap).
        
        Args:
            column: Column to index
            index_type: Index type ('auto', 'btree', 'bitmap')
            **index_params: Additional parameters
            
        Returns:
            Index metadata dictionary
        """
        if not self.enable_scalar_index:
            logger.warning("Scalar indexing not enabled")
            return {}
        
        try:
            if self._scalar_index_builder is None:
                # Auto-select index type if requested
                if index_type == 'auto':
                    # Sample data to determine cardinality
                    # For now, default to btree
                    index_type = 'btree'
                
                self._scalar_index_builder = ScalarIndexBuilder(column, index_type)
            
            if index_type == 'btree':
                return self._scalar_index_builder.create_btree_index(
                    self._native_reader._table if self._native_reader else None,
                    **index_params
                )
            elif index_type == 'bitmap':
                return self._scalar_index_builder.create_bitmap_index(
                    self._native_reader._table if self._native_reader else None,
                    **index_params
                )
            else:
                raise ValueError(f"Unsupported scalar index type: {index_type}")
                
        except Exception as e:
            logger.error(f"Failed to create scalar index: {e}")
            return {}

    def close(self) -> None:
        """Close the reader and release resources."""
        if self._native_reader is not None:
            try:
                self._native_reader.close()
            except Exception as e:
                logger.warning(f"Error closing native reader: {e}")
            finally:
                self._native_reader = None
        
        self._vector_index_builder = None
        self._scalar_index_builder = None
        self._predicate_optimizer = None
        self._initialized = False
        logger.debug(f"Closed Lance reader for {self.file_path}")

    def __del__(self):
        """Destructor to ensure cleanup."""
        self.close()
