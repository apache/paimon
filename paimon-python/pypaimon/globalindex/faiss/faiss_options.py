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

"""Options for FAISS vector index."""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Any


class FaissVectorMetric(Enum):
    """Distance metric for FAISS vector search."""
    L2 = "L2"
    INNER_PRODUCT = "INNER_PRODUCT"


class FaissIndexType(Enum):
    """Type of FAISS index."""
    FLAT = "FLAT"
    HNSW = "HNSW"
    IVF = "IVF"
    IVF_PQ = "IVF_PQ"
    IVF_SQ8 = "IVF_SQ8"
    UNKNOWN = "UNKNOWN"


@dataclass
class FaissVectorIndexOptions:
    """
    Options for FAISS vector index.
    
    Corresponds to Java's FaissVectorIndexOptions class.
    """

    # Vector dimension
    dimension: int = 128

    # Distance metric (L2 or INNER_PRODUCT)
    metric: FaissVectorMetric = FaissVectorMetric.L2

    # Index type
    index_type: FaissIndexType = FaissIndexType.IVF_SQ8

    # HNSW parameters
    m: int = 32  # Number of connections per layer
    ef_construction: int = 40  # Dynamic candidate list size for construction
    ef_search: int = 16  # Dynamic candidate list size for search

    # IVF parameters
    nlist: int = 100  # Number of inverted lists (clusters)
    nprobe: int = 64  # Number of clusters to visit during search

    # PQ parameters
    pq_m: int = 8  # Number of sub-quantizers
    pq_nbits: int = 8  # Bits per sub-quantizer

    # Index file parameters
    size_per_index: int = 2000000  # Vectors stored in each index file
    training_size: int = 500000  # Vectors for training IVF indices

    # Search parameters
    search_factor: int = 10  # Multiplier for search limit when filtering

    # Normalization
    normalize: bool = False  # Whether to L2 normalize vectors

    @classmethod
    def from_options(cls, options: Dict[str, Any]) -> 'FaissVectorIndexOptions':
        """Create FaissVectorIndexOptions from a dictionary of options."""
        return cls(
            dimension=options.get('vector.dim', 128),
            metric=FaissVectorMetric(options.get('vector.metric', 'L2')),
            index_type=FaissIndexType(options.get('vector.index-type', 'IVF_SQ8')),
            m=options.get('vector.m', 32),
            ef_construction=options.get('vector.ef-construction', 40),
            ef_search=options.get('vector.ef-search', 16),
            nlist=options.get('vector.nlist', 100),
            nprobe=options.get('vector.nprobe', 64),
            pq_m=options.get('vector.pq-m', 8),
            pq_nbits=options.get('vector.pq-nbits', 8),
            size_per_index=options.get('vector.size-per-index', 2000000),
            training_size=options.get('vector.training-size', 500000),
            search_factor=options.get('vector.search-factor', 10),
            normalize=options.get('vector.normalize', False),
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert options to a dictionary."""
        return {
            'vector.dim': self.dimension,
            'vector.metric': self.metric.value,
            'vector.index-type': self.index_type.value,
            'vector.m': self.m,
            'vector.ef-construction': self.ef_construction,
            'vector.ef-search': self.ef_search,
            'vector.nlist': self.nlist,
            'vector.nprobe': self.nprobe,
            'vector.pq-m': self.pq_m,
            'vector.pq-nbits': self.pq_nbits,
            'vector.size-per-index': self.size_per_index,
            'vector.training-size': self.training_size,
            'vector.search-factor': self.search_factor,
            'vector.normalize': self.normalize,
        }
