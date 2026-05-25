## Paimon Lumina

This module integrates [Lumina](https://github.com/alibaba/paimon-cpp/tree/main/third_party/lumina)
as a vector index for Apache Paimon's global index framework.

Lumina vector search library is derived from an internal repository maintained by
Alibaba Storage Service Team. It is accessed via JNI through the `lumina-jni` artifact.

**Note:** Lumina native library only supports **x86_64 (AMD64)** architecture. It is not available on ARM (e.g., Apple Silicon, aarch64).

### Supported Index Types

| Index Type | Description |
|------------|-------------|
| **DISKANN** | DiskANN graph-based index (default) |

### Supported Vector Metrics

| Metric | Description |
|--------|-------------|
| **L2** | Euclidean distance |
| **COSINE** | Cosine distance |
| **INNER_PRODUCT** | Dot product (default) |

**Note:** PQ encoding does not support cosine metric. Use `rawf32` or `sq8` encoding with cosine, or switch to `l2` or `inner_product` metric.

### Configuration Options

All options use the `lumina.` prefix as table properties.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `lumina.index.dimension` | int | 128 | Vector dimension |
| `lumina.distance.metric` | string | inner_product | Distance metric (l2, cosine, inner_product) |
| `lumina.index.type` | string | diskann | Index type |
| `lumina.encoding.type` | string | pq | Encoding type (rawf32, sq8, pq) |
| `lumina.pretrain.sample_ratio` | double | 0.2 | Sample ratio for pretraining |
| `lumina.diskann.build.ef_construction` | int | 1024 | Size of the dynamic candidate list during graph construction |
| `lumina.diskann.build.neighbor_count` | int | 64 | Maximum number of neighbors per node in the graph |
| `lumina.diskann.build.thread_count` | int | 32 | Number of threads used for DiskANN index building |
| `lumina.diskann.search.list_size` | int | 1.5x topK | DiskANN search list size (auto-set to 1.5x topK if not specified) |
| `lumina.diskann.search.beam_width` | int | 4 | Beam width for DiskANN search |
| `lumina.encoding.pq.m` | int | 64 | Number of sub-quantizers for PQ encoding (auto-capped to dimension) |
| `lumina.search.parallel_number` | int | 5 | Parallel number for search |
