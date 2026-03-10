## Paimon Lumina

This module integrates [Lumina](https://github.com/alibaba/paimon-cpp/tree/main/third_party/lumina)
as a vector index for Apache Paimon's global index framework.

Lumina vector search library is derived from an internal repository maintained by
Alibaba Storage Service Team. It is accessed via JNI through the `lumina-jni` artifact.

### Supported Index Types

| Index Type | Description |
|------------|-------------|
| **DISKANN** | DiskANN graph-based index (default) |

### Supported Vector Metrics

| Metric | Description |
|--------|-------------|
| **L2** | Euclidean distance (default) |
| **COSINE** | Cosine distance |
| **INNER_PRODUCT** | Dot product |

### Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `vector.dim` | int | 128 | Vector dimension |
| `vector.metric` | enum | L2 | Distance metric |
| `vector.index-type` | enum | DISKANN | Index type |
| `vector.encoding-type` | string | rawf32 | Encoding type (rawf32, sq8, pq) |
| `vector.size-per-index` | int | 2,000,000 | Max vectors per index file |
| `vector.training-size` | int | 500,000 | Vectors used for pretraining |
| `vector.search-factor` | int | 10 | Multiplier for search limit when filtering |
| `vector.diskann.search-list-size` | int | 100 | DiskANN search list size |
| `vector.pretrain-sample-ratio` | double | 1.0 | Pretrain sample ratio |
