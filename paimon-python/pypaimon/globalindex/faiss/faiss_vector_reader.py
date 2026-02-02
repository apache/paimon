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

"""
FAISS Vector Global Index Reader.
"""

import os
import tempfile
import uuid
from typing import Dict, List, Optional

import numpy as np

from pypaimon.globalindex.global_index_reader import GlobalIndexReader
from pypaimon.globalindex.global_index_result import GlobalIndexResult
from pypaimon.globalindex.global_index_meta import GlobalIndexIOMeta
from pypaimon.globalindex.vector_search_result import DictBasedScoredIndexResult
from pypaimon.globalindex.roaring_bitmap import RoaringBitmap64
from pypaimon.globalindex.faiss.faiss_options import (
    FaissVectorIndexOptions,
    FaissVectorMetric,
    FaissIndexType,
)
from pypaimon.globalindex.faiss.faiss_index_meta import FaissIndexMeta
from pypaimon.globalindex.faiss.faiss_index import FaissIndex


class FaissVectorGlobalIndexReader(GlobalIndexReader):
    """
    Vector global index reader using FAISS.
    """

    def __init__(
        self,
        file_io: 'FileIO',
        index_path: str,
        io_metas: List[GlobalIndexIOMeta],
        options: FaissVectorIndexOptions
    ):
        self._file_io = file_io
        self._index_path = index_path
        self._io_metas = io_metas
        self._options = options
        
        self._indices: List[Optional[FaissIndex]] = []
        self._index_metas: List[FaissIndexMeta] = []
        self._local_index_files: List[str] = []
        
        self._metas_loaded = False
        self._indices_loaded = False

    def visit_vector_search(self, vector_search: 'VectorSearch') -> Optional[GlobalIndexResult]:
        """Perform vector similarity search."""
        try:
            # First load only metadata
            self._ensure_load_metas()
            
            include_row_ids = vector_search.include_row_ids
            
            # If include_row_ids is specified, check which indices contain matching rows
            if include_row_ids is not None:
                matching_indices = []
                for i, meta in enumerate(self._index_metas):
                    if self._has_overlap(meta.min_id, meta.max_id, include_row_ids):
                        matching_indices.append(i)
                
                # If no index contains matching rowIds, return empty
                if not matching_indices:
                    return None
                
                # Load only matching indices
                self._ensure_load_indices(matching_indices)
            else:
                # Load all indices
                self._ensure_load_all_indices()
            
            return self._search(vector_search)
            
        except Exception as e:
            raise RuntimeError(
                f"Failed to search FAISS vector index with field_name={vector_search.field_name}, "
                f"limit={vector_search.limit}"
            ) from e

    def _has_overlap(self, min_id: int, max_id: int, include_row_ids: RoaringBitmap64) -> bool:
        """Check if the range [min_id, max_id] has any overlap with include_row_ids."""
        for row_id in include_row_ids:
            if min_id <= row_id <= max_id:
                return True
            if row_id > max_id:
                break
        return False

    def _search(self, vector_search: 'VectorSearch') -> Optional[GlobalIndexResult]:
        """Perform the actual search across all loaded indices."""
        query_vector = np.array(vector_search.vector, dtype=np.float32)
        
        # L2 normalize the query vector if enabled
        if self._options.normalize:
            query_vector = self._normalize_l2(query_vector)
        
        limit = vector_search.limit
        include_row_ids = vector_search.include_row_ids
        
        # When filtering is enabled, fetch more results
        search_k = limit
        if include_row_ids is not None:
            search_k = max(
                limit * self._options.search_factor,
                include_row_ids.cardinality()
            )
        
        # Collect results from all indices using a priority queue approach
        results: Dict[int, float] = {}
        
        for index in self._indices:
            if index is None:
                continue
            
            # Configure search parameters based on index type
            self._configure_search_params(index)
            
            # Limit search_k to index size
            effective_k = min(search_k, max(1, index.size()))
            if effective_k <= 0:
                continue
            
            # Perform search
            distances, labels = index.search(query_vector, effective_k)
            
            for i in range(effective_k):
                row_id = int(labels[0, i])
                if row_id < 0:
                    # Invalid result
                    continue
                
                # Filter by include row IDs if specified
                if include_row_ids is not None and row_id not in include_row_ids:
                    continue
                
                # Convert distance to score (higher is better)
                score = self._convert_distance_to_score(float(distances[0, i]))
                
                # Keep top-k results
                if len(results) < limit:
                    results[row_id] = score
                else:
                    # Find minimum score in current results
                    min_row_id = min(results.keys(), key=lambda k: results[k])
                    if score > results[min_row_id]:
                        del results[min_row_id]
                        results[row_id] = score
        
        if not results:
            return None
        
        return DictBasedScoredIndexResult(results)

    def _configure_search_params(self, index: FaissIndex) -> None:
        """Configure search parameters based on index type."""
        if index.index_type == FaissIndexType.HNSW:
            index.set_hnsw_ef_search(self._options.ef_search)
        elif index.index_type in (FaissIndexType.IVF, FaissIndexType.IVF_PQ, FaissIndexType.IVF_SQ8):
            # For small indices, use higher nprobe
            effective_nprobe = max(
                self._options.nprobe,
                max(1, index.size() // 10)
            )
            index.set_ivf_nprobe(effective_nprobe)

    def _convert_distance_to_score(self, distance: float) -> float:
        """Convert distance to similarity score."""
        if self._options.metric == FaissVectorMetric.L2:
            # For L2 distance, smaller is better, so invert it
            return 1.0 / (1.0 + distance)
        else:
            # Inner product is already a similarity
            return distance

    @staticmethod
    def _normalize_l2(vector: np.ndarray) -> np.ndarray:
        """L2 normalize the vector."""
        norm = np.linalg.norm(vector)
        if norm > 0:
            return vector / norm
        return vector

    def _ensure_load_metas(self) -> None:
        """Load only metadata from all index files."""
        if self._metas_loaded:
            return
        
        for io_meta in self._io_metas:
            if io_meta.metadata:
                meta = FaissIndexMeta.deserialize(io_meta.metadata)
                self._index_metas.append(meta)
        
        self._metas_loaded = True

    def _ensure_load_all_indices(self) -> None:
        """Load all indices."""
        if self._indices_loaded:
            return
        
        for i in range(len(self._io_metas)):
            self._load_index_at(i)
        
        self._indices_loaded = True

    def _ensure_load_indices(self, positions: List[int]) -> None:
        """Load only the specified indices by their positions."""
        # Ensure indices list is large enough
        while len(self._indices) < len(self._io_metas):
            self._indices.append(None)
        
        for pos in positions:
            if self._indices[pos] is None:
                self._load_index_at(pos)

    def _load_index_at(self, position: int) -> None:
        """Load a single index at the specified position."""
        io_meta = self._io_metas[position]
        
        # Read index file from storage
        index_file_path = f"{self._index_path}/{io_meta.file_name}"
        
        # Create a temp file for the FAISS index
        # Add to tracking list immediately to ensure cleanup on any failure
        temp_path = None
        try:
            temp_file = tempfile.NamedTemporaryFile(
                prefix=f"paimon-faiss-{uuid.uuid4()}-",
                suffix=".faiss",
                delete=False
            )
            temp_path = temp_file.name
            # Track immediately after creation to prevent leaks
            self._local_index_files.append(temp_path)
            
            # Copy index data to temp file
            with self._file_io.new_input_stream(index_file_path) as input_stream:
                data = input_stream.read()
                temp_file.write(data)
                temp_file.close()
            
            # Load FAISS index from temp file
            index = FaissIndex.from_file(temp_path)
            
            # Ensure indices list is large enough
            while len(self._indices) <= position:
                self._indices.append(None)
            
            self._indices[position] = index
            
        except Exception as e:
            # Clean up on failure
            if temp_path is not None:
                try:
                    temp_file.close()
                except Exception:
                    pass
                if os.path.exists(temp_path):
                    os.unlink(temp_path)
                # Remove from tracking list since we've already cleaned it up
                if temp_path in self._local_index_files:
                    self._local_index_files.remove(temp_path)
            raise e

    def close(self) -> None:
        """Close the reader and release resources."""
        # Close all FAISS indices
        for index in self._indices:
            if index is not None:
                try:
                    index.close()
                except Exception:
                    pass
        self._indices.clear()
        
        # Delete local temporary files
        for local_file in self._local_index_files:
            try:
                if os.path.exists(local_file):
                    os.unlink(local_file)
            except Exception:
                pass
        self._local_index_files.clear()

    def __enter__(self) -> 'FaissVectorGlobalIndexReader':
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
