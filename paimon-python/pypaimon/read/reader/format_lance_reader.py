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
#  limitations under the License.
################################################################################

"""Lance format reader implementation for Paimon."""

import logging
from typing import List, Optional, Any, Dict

import pyarrow as pa
import pyarrow.dataset as ds
from pyarrow import RecordBatch

from pypaimon.common.file_io import FileIO
from pypaimon.read.reader.iface.record_batch_reader import RecordBatchReader
from pypaimon.read.reader.lance_utils import to_lance_specified
from pypaimon.read.reader.lance.vector_index import VectorIndexBuilder
from pypaimon.read.reader.lance.scalar_index import ScalarIndexBuilder
from pypaimon.read.reader.lance.predicate_pushdown import PredicateOptimizer

logger = logging.getLogger(__name__)


class FormatLanceReader(RecordBatchReader):
    """
    A Format Reader that reads record batch from a Lance file using PyArrow,
    and filters it based on the provided predicate and projection.
    Integrates Lance format support with vector and scalar indexing capabilities.
    """

    def __init__(self,
                 file_io: FileIO,
                 file_path: str,
                 read_fields: List[str],
                 push_down_predicate: Optional[Any] = None,
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
        import lance

        # Use upstream's file path conversion utility
        file_path_for_lance, storage_options = to_lance_specified(file_io, file_path)
        
        self.file_io = file_io
        self.file_path = file_path_for_lance
        self.read_fields = read_fields
        self.push_down_predicate = push_down_predicate
        self.batch_size = batch_size
        self.selection_ranges = selection_ranges
        self.enable_vector_search = enable_vector_search
        self.enable_scalar_index = enable_scalar_index

        # Initialize base reader from upstream
        columns_for_lance = read_fields if read_fields else None
        lance_reader = lance.file.LanceFileReader(  # type: ignore
            file_path_for_lance,
            storage_options=storage_options,
            columns=columns_for_lance)
        reader_results = lance_reader.read_all()
        pa_table = reader_results.to_table()

        # Setup reader with predicate push-down if provided
        if push_down_predicate is not None:
            in_memory_dataset = ds.InMemoryDataset(pa_table)
            scanner = in_memory_dataset.scanner(filter=push_down_predicate, batch_size=batch_size)
            self.reader = scanner.to_reader()
        else:
            self.reader = iter(pa_table.to_batches(max_chunksize=batch_size))

        # Enhanced indexing support
        self._vector_index_builder: Optional[VectorIndexBuilder] = None
        self._scalar_index_builder: Optional[ScalarIndexBuilder] = None
        self._predicate_optimizer: Optional[PredicateOptimizer] = None
        self._pa_table = pa_table

        try:
            if enable_vector_search:
                self._initialize_vector_indexing()
            if enable_scalar_index:
                self._initialize_scalar_indexing()
        except ImportError as e:
            logger.error(f"Lance library not fully available: {e}")
            # Continue with basic reading functionality

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

    def read_arrow_batch(self) -> Optional[RecordBatch]:
        """
        Read next batch of data from Lance file with optimization.

        Returns:
            PyArrow RecordBatch with selected columns, or None if EOF
        """
        try:
            # Handle both scanner reader and iterator
            if hasattr(self.reader, 'read_next_batch'):
                batch = self.reader.read_next_batch()  # type: ignore
            else:
                # Iterator of batches
                batch = next(self.reader)  # type: ignore

            if batch is None or batch.num_rows == 0:
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

        except StopIteration:
            return None
        except Exception as e:
            logger.error(f"Error reading batch from Lance file: {e}")
            raise

    def _apply_predicate_optimization(self, batch: RecordBatch) -> Optional[RecordBatch]:
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

            # Note: Actual Lance filter execution is handled by PyArrow's dataset API
            # which is already applied during initialization
            return batch

        except Exception as e:
            logger.warning(f"Predicate optimization failed, returning unfiltered batch: {e}")
            return batch

    def _apply_row_selection(self, batch: RecordBatch) -> Optional[RecordBatch]:
        """
        Apply row range selection to the batch.

        Args:
            batch: PyArrow RecordBatch

        Returns:
            Filtered RecordBatch or None if no rows match
        """
        try:
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
                    self._pa_table,
                    **index_params
                )
            elif index_type == 'hnsw':
                return self._vector_index_builder.create_hnsw_index(
                    self._pa_table,
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
                    try:
                        # Get column statistics to choose optimal index
                        if hasattr(self._pa_table, 'to_pandas'):
                            # Sample first 1000 rows to estimate cardinality
                            sample_df = self._pa_table.to_pandas().head(1000)
                            if column in sample_df.columns:
                                unique_ratio = sample_df[column].nunique() / len(sample_df)
                                # Use Bitmap for low cardinality (< 10% unique)
                                # Use BTree for high cardinality or numeric columns
                                if unique_ratio < 0.1 and sample_df[column].dtype == 'object':
                                    index_type = 'bitmap'
                                else:
                                    index_type = 'btree'
                            else:
                                index_type = 'btree'  # Default to BTree
                        else:
                            index_type = 'btree'
                    except Exception as auto_select_error:
                        logger.warning(f"Auto index type selection failed: {auto_select_error}, defaulting to btree")
                        index_type = 'btree'

                self._scalar_index_builder = ScalarIndexBuilder(column, index_type)

            if index_type == 'btree':
                return self._scalar_index_builder.create_btree_index(
                    self._pa_table,
                    **index_params
                )
            elif index_type == 'bitmap':
                return self._scalar_index_builder.create_bitmap_index(
                    self._pa_table,
                    **index_params
                )
            else:
                raise ValueError(f"Unsupported scalar index type: {index_type}")

        except Exception as e:
            logger.error(f"Failed to create scalar index: {e}")
            return {}

    def close(self) -> None:
        """Close the reader and release resources."""
        if self.reader is not None:
            try:
                self.reader = None
            except Exception as e:
                logger.warning(f"Error closing reader: {e}")

        self._vector_index_builder = None
        self._scalar_index_builder = None
        self._predicate_optimizer = None
        logger.debug(f"Closed Lance reader for {self.file_path}")

    def __del__(self) -> None:
        """Destructor to ensure cleanup."""
        try:
            self.close()
        except Exception:
            pass
