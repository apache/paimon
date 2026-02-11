<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# DiskANN Parameter Tuning Guide

This document provides guidance on tuning DiskANN vector index parameters for optimal performance in Apache Paimon.

## Overview

DiskANN is a graph-based approximate nearest neighbor (ANN) search algorithm designed for efficient billion-point vector search. The implementation in Paimon provides several parameters to control the trade-offs between accuracy, speed, and resource usage.

## Key Parameters

### 1. Graph Construction Parameters

#### `vector.diskann.max-degree` (R)
- **Default**: 64
- **Range**: 32-128
- **Description**: Maximum degree (number of connections) for each node in the graph
- **Impact**:
  - Higher values → Better recall, higher memory usage, longer build time
  - Lower values → Faster build, lower memory, potentially lower recall
- **Recommendations**:
  - **32**: For memory-constrained environments or when build time is critical
  - **64**: Balanced default (Microsoft recommended)
  - **128**: For maximum recall when resources permit

#### `vector.diskann.build-list-size` (L)
- **Default**: 100
- **Range**: 50-200
- **Description**: Size of the candidate list during graph construction
- **Impact**:
  - Higher values → Better graph quality, longer build time
  - Lower values → Faster build, potentially lower recall
- **Recommendations**:
  - Use default 100 for most cases
  - Increase to 150-200 for very high-dimensional data (>512 dimensions)

### 2. Search Parameters

#### `vector.diskann.search-list-size` (L)
- **Default**: 100
- **Range**: 16-500
- **Description**: Size of the candidate list during search
- **Impact**:
  - Higher values → Better recall, higher latency
  - Lower values → Lower latency, potentially lower recall
- **Dynamic Behavior**: The implementation automatically adjusts this to be at least equal to the requested `k` (number of results)
- **Recommendations**:
  - **16-32**: For latency-critical applications (QPS > 5000)
  - **100**: Balanced default
  - **200-500**: For maximum recall (recall > 95%)

#### `vector.search-factor`
- **Default**: 10
- **Range**: 5-20
- **Description**: Multiplier for search limit when row filtering is applied
- **Impact**: When filtering by row IDs, fetches `limit * search-factor` results to ensure sufficient matches after filtering
- **Recommendations**:
  - **5**: When filtering is selective (<10% of data)
  - **10**: Default for typical filtering scenarios
  - **20**: When filtering is very broad (>50% of data)

### 3. Data Configuration

#### `vector.dim`
- **Default**: 128
- **Description**: Dimension of the vectors
- **Recommendations**:
  - Must match your embedding model
  - Common values: 128, 256, 384, 512, 768, 1024

#### `vector.metric`
- **Default**: L2
- **Options**: L2, INNER_PRODUCT, COSINE
- **Description**: Distance metric for similarity computation
- **Recommendations**:
  - **L2**: For Euclidean distance (most common)
  - **INNER_PRODUCT**: For dot product similarity
  - **COSINE**: For cosine similarity

### 4. Index Organization

#### `vector.size-per-index`
- **Default**: 2,000,000
- **Description**: Number of vectors per index file
- **Impact**:
  - Larger values → Fewer files, higher memory per index, better search efficiency
  - Smaller values → More files, lower memory per index, more overhead
- **Recommendations**:
  - **500,000**: For small datasets or memory-constrained environments
  - **2,000,000**: Default for balanced performance
  - **5,000,000+**: For large-scale production systems with ample resources

#### `vector.diskann.index-type`
- **Default**: MEMORY
- **Options**: MEMORY, DISK
- **Description**: Type of index structure
- **Recommendations**:
  - **MEMORY**: For datasets that fit in RAM (best performance)
  - **DISK**: For datasets exceeding RAM (requires SSD)

## Performance Tuning Guide

### High Recall (>95%)
```properties
vector.diskann.max-degree = 128
vector.diskann.build-list-size = 150
vector.diskann.search-list-size = 200
```

### Balanced (90-95% recall)
```properties
vector.diskann.max-degree = 64
vector.diskann.build-list-size = 100
vector.diskann.search-list-size = 100
```

### High QPS (Low Latency)
```properties
vector.diskann.max-degree = 32
vector.diskann.build-list-size = 75
vector.diskann.search-list-size = 32
```

### Memory-Constrained
```properties
vector.diskann.max-degree = 32
vector.diskann.build-list-size = 75
vector.size-per-index = 500000
vector.diskann.index-type = DISK
```

## Best Practices

1. **Start with defaults**: The default parameters are tuned for balanced performance
2. **Measure first**: Profile your workload before tuning
3. **Tune incrementally**: Change one parameter at a time and measure impact
4. **Consider trade-offs**: Higher recall typically means higher latency and resource usage
5. **Test with production data**: Parameter effectiveness depends on data characteristics

## Advanced Parameters (Future Enhancement)

The following parameters are documented in the official Microsoft DiskANN implementation but are not yet exposed in the current Rust-based native library:

- **alpha** (default: 1.2): Controls the graph construction pruning strategy
- **saturate_graph** (default: true): Whether to saturate the graph during construction

These parameters may be added in future versions when the underlying Rust DiskANN crate exposes them through its configuration API.

## Performance Metrics

When tuning parameters, monitor these metrics:
- **Recall**: Percentage of true nearest neighbors found
- **QPS (Queries Per Second)**: Throughput of search operations
- **Latency**: Time to complete a single query (p50, p95, p99)
- **Memory Usage**: RAM consumed by indices
- **Build Time**: Time to construct the index

## Recent Improvements

### Dynamic Search List Sizing (v1.0+)
The search list size is now automatically adjusted to be at least equal to the requested `k`. This follows Milvus best practices and ensures optimal recall without manual tuning.

### Memory-Efficient Loading (v1.0+)
Indices are now loaded through temporary files, allowing the OS to manage memory more efficiently for large indices. This is a step toward full mmap support.

## References

- [Microsoft DiskANN Paper](https://proceedings.neurips.cc/paper/2019/file/09853c7fb1d3f8ee67a61b6bf4a7f8e6-Paper.pdf)
- [Microsoft DiskANN Library](https://github.com/microsoft/DiskANN)
- [Milvus DiskANN Documentation](https://milvus.io/docs/diskann.md)
