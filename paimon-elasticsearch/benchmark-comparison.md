# HNSW (Lucene) vs Lumina (DiskANN) 向量索引性能对比

## 1. 方案概述

| 维度 | ES (Lucene HNSW) | Lumina (DiskANN) |
|------|-------------------|------------------|
| 底层算法 | HNSW (Hierarchical Navigable Small World) | DiskANN (Disk-based ANN) |
| 实现库 | `com.aliyun.lucene:lucene-core:10.2.1-ali1.3.5` | `org.aliyun:lumina` (JNI) |
| Codec | `Lucene101Codec` + `Lucene99HnswVectorsFormat` | 自研 DiskANN codec |
| 向量编码 | 原始 float32 | rawf32 / PQ / SQ8 |
| 索引结构 | 多层图（skiplist 式层级） | 单层 Vamana 图 + 磁盘布局 |
| 文件格式 | Packed Directory（多 Lucene 文件合并为单文件） | 单文件（Lumina 原生格式） |
| 索引构建 | 单线程（Lucene IndexWriter） | 多线程（`DISKANN_BUILD_THREAD_COUNT`） |
| SPI 标识 | `es-vector-hnsw` | `lumina-vector` |
| 距离度量 | l2 / cosine / inner_product | l2 / cosine / inner_product |

## 2. 核心参数对比

### 2.1 构建参数

| 参数 | ES (HNSW) | Lumina (DiskANN) | 说明 |
|------|-----------|------------------|------|
| 向量数 | `BENCHMARK_NUM_VECTORS` | `BENCHMARK_NUM_VECTORS` | 相同 |
| 维度 | `BENCHMARK_DIMENSION` | `BENCHMARK_DIMENSION` | 相同 |
| 距离度量 | `BENCHMARK_DISTANCE_METRIC` (default: l2) | `BENCHMARK_DISTANCE_METRIC` (default: inner_product) | 默认值不同 |
| 最大连接数 | `BENCHMARK_HNSW_M` (default: 16) | `BENCHMARK_NEIGHBOR_COUNT` (default: 64) | 类似概念 |
| 构建扩展因子 | `BENCHMARK_HNSW_EF_CONSTRUCTION` (default: 200) | `BENCHMARK_EF_CONSTRUCTION` (default: 128) | 类似概念 |
| beam width | `BENCHMARK_HNSW_BEAM_WIDTH` (default: 100) | - | ES 专有 |
| 构建线程 | 单线程 | `BENCHMARK_BUILD_THREADS` (default: CPU cores) | Lumina 支持多线程 |
| 编码类型 | float32 (固定) | `BENCHMARK_ENCODING_TYPE` (pq/sq8/rawf32) | Lumina 支持量化 |
| PQ 参数 | - | `BENCHMARK_PQ_M` / `PQ_MAX_EPOCH` / `PQ_THREAD_COUNT` | Lumina 专有 |

### 2.2 查询参数

| 参数 | ES (HNSW) | Lumina (DiskANN) | 说明 |
|------|-----------|------------------|------|
| Top-K | `BENCHMARK_TOP_K` (default: 10) | `BENCHMARK_TOP_K` (default: 10) | 相同 |
| 查询次数 | `BENCHMARK_NUM_QUERIES` (default: 1000) | `BENCHMARK_NUM_QUERIES` (default: 1000) | 相同 |
| 搜索列表大小 | Lucene 内部管理 | 1.5x TOP_K | 不同策略 |

## 3. Benchmark 运行命令

### 3.1 统一测试参数

```bash
# 公共参数（确保两者对齐）
NUM_VECTORS=100000
DIMENSION=128
DISTANCE_METRIC=l2
TOP_K=10
NUM_QUERIES=1000
BENCHMARK_PATH=/tmp/vector-benchmark
```

### 3.2 ES (Lucene HNSW)

```bash
# 构建
mvn test -pl paimon-elasticsearch \
  -Dtest=ESVectorBenchmark#benchmarkBuild \
  -DextraJavaTestArgs="-Xmx8g \
    -DBENCHMARK_PATH=$BENCHMARK_PATH/es \
    -DBENCHMARK_NUM_VECTORS=$NUM_VECTORS \
    -DBENCHMARK_DIMENSION=$DIMENSION \
    -DBENCHMARK_DISTANCE_METRIC=$DISTANCE_METRIC \
    -DBENCHMARK_HNSW_M=16 \
    -DBENCHMARK_HNSW_EF_CONSTRUCTION=200 \
    -DBENCHMARK_HNSW_BEAM_WIDTH=100 \
    -DBENCHMARK_KEEP_INDEX=true"

# 查询（替换 INDEX_FILE 为实际路径）
mvn test -pl paimon-elasticsearch \
  -Dtest=ESVectorBenchmark#benchmarkQuery \
  -DextraJavaTestArgs="-Xmx8g \
    -DBENCHMARK_PATH=$BENCHMARK_PATH/es \
    -DBENCHMARK_INDEX_FILE=<build输出的索引路径> \
    -DBENCHMARK_DIMENSION=$DIMENSION \
    -DBENCHMARK_DISTANCE_METRIC=$DISTANCE_METRIC \
    -DBENCHMARK_NUM_QUERIES=$NUM_QUERIES \
    -DBENCHMARK_TOP_K=$TOP_K"
```

### 3.3 Lumina (DiskANN)

```bash
# 构建
mvn test -pl paimon-lumina \
  -Dtest=LuminaVectorBenchmark#benchmarkBuild \
  -DextraJavaTestArgs="-Xmx8g \
    -DBENCHMARK_PATH=$BENCHMARK_PATH/lumina \
    -DBENCHMARK_NUM_VECTORS=$NUM_VECTORS \
    -DBENCHMARK_DIMENSION=$DIMENSION \
    -DBENCHMARK_DISTANCE_METRIC=$DISTANCE_METRIC \
    -DBENCHMARK_ENCODING_TYPE=rawf32 \
    -DBENCHMARK_EF_CONSTRUCTION=128 \
    -DBENCHMARK_NEIGHBOR_COUNT=64 \
    -DBENCHMARK_BUILD_THREADS=16 \
    -DBENCHMARK_KEEP_INDEX=true"

# 查询（替换 INDEX_FILE 为实际路径）
mvn test -pl paimon-lumina \
  -Dtest=LuminaVectorBenchmark#benchmarkQuery \
  -DextraJavaTestArgs="-Xmx8g \
    -DBENCHMARK_PATH=$BENCHMARK_PATH/lumina \
    -DBENCHMARK_INDEX_FILE=<build输出的索引路径> \
    -DBENCHMARK_DIMENSION=$DIMENSION \
    -DBENCHMARK_DISTANCE_METRIC=$DISTANCE_METRIC \
    -DBENCHMARK_ENCODING_TYPE=rawf32 \
    -DBENCHMARK_NUM_QUERIES=$NUM_QUERIES \
    -DBENCHMARK_TOP_K=$TOP_K"
```

> **注意**: Lumina 使用 `rawf32` 编码与 ES 对齐（均为原始 float32），排除量化带来的差异。如需对比 PQ 量化效果，可额外增加 `BENCHMARK_ENCODING_TYPE=pq` 的测试组。

## 4. 性能对比结果

### 4.1 测试环境

| 项目 | 配置 |
|------|------|
| 机器 | <!-- 填写机器型号/配置 --> |
| CPU | <!-- 填写 CPU 型号 --> |
| 内存 | <!-- 填写内存大小 --> |
| 存储 | <!-- Local SSD / OSS --> |
| JVM | <!-- -Xmx 等参数 --> |
| 数据集 | 随机向量 (seed=12345) |

### 4.2 构建性能

| 指标 | ES (HNSW) | Lumina (DiskANN) | 比率 (ES/Lumina) |
|------|-----------|------------------|-----------------|
| **向量数** | | | — |
| **维度** | | | — |
| **构建总时间 (s)** | | | |
| **索引文件大小 (MB)** | | | |
| **写入吞吐 (vectors/s)** | | | |
| **索引构建时间 (s)** | | | |

### 4.3 查询延迟 (ms)

| 指标 | ES (HNSW) | Lumina (DiskANN) | 比率 (ES/Lumina) |
|------|-----------|------------------|-----------------|
| **Min** | | | |
| **Avg** | | | |
| **P50** | | | |
| **P90** | | | |
| **P95** | | | |
| **P99** | | | |
| **Max** | | | |
| **RPS (queries/s)** | | | |

### 4.4 I/O 统计 (每次查询平均)

#### Open 阶段 (索引加载)

| 指标 | ES (HNSW) | Lumina (DiskANN) | 比率 |
|------|-----------|------------------|------|
| Bytes read (MB) | | | |
| Read count | | | |
| Seek count | | | |
| Read time (ms) | | | |
| Seek time (ms) | | | |

#### Search 阶段 (向量检索)

| 指标 | ES (HNSW) | Lumina (DiskANN) | 比率 |
|------|-----------|------------------|------|
| Bytes read (KB) | | | |
| Read count | | | |
| Seek count | | | |
| Read time (ms) | | | |
| Seek time (ms) | | | |

## 5. 多规模对比

### 5.1 不同数据量 (dim=128, metric=l2)

| 向量数 | ES 构建 (s) | Lumina 构建 (s) | ES P50 (ms) | Lumina P50 (ms) | ES 索引大小 | Lumina 索引大小 |
|--------|------------|----------------|-------------|-----------------|------------|----------------|
| 10,000 | | | | | | |
| 100,000 | | | | | | |
| 1,000,000 | | | | | | |
| 10,000,000 | | | | | | |

### 5.2 不同维度 (vectors=100000, metric=l2)

| 维度 | ES 构建 (s) | Lumina 构建 (s) | ES P50 (ms) | Lumina P50 (ms) | ES 索引大小 | Lumina 索引大小 |
|------|------------|----------------|-------------|-----------------|------------|----------------|
| 64 | | | | | | |
| 128 | | | | | | |
| 256 | | | | | | |
| 512 | | | | | | |
| 1024 | | | | | | |

### 5.3 不同距离度量 (vectors=100000, dim=128)

| 度量 | ES 构建 (s) | Lumina 构建 (s) | ES P50 (ms) | Lumina P50 (ms) |
|------|------------|----------------|-------------|-----------------|
| l2 | | | | |
| cosine | | | | |
| inner_product | | | | |

### 5.4 Lumina PQ 量化 vs rawf32 vs ES (vectors=100000, dim=128)

| 编码 | 构建 (s) | P50 (ms) | 索引大小 (MB) | 备注 |
|------|---------|---------|-------------|------|
| ES HNSW (float32) | | | | 无量化 |
| Lumina rawf32 | | | | 无量化 |
| Lumina PQ (m=64) | | | | PQ 量化 |
| Lumina SQ8 | | | | 标量量化 |

## 6. OSS 远程查询对比

> 此部分用于测试 OSS 上索引的查询性能，需要配置 OSS 凭证。

| 指标 | ES (HNSW) - OSS | Lumina (DiskANN) - OSS | 本地 vs OSS 倍率 |
|------|-----------------|----------------------|-----------------|
| P50 (ms) | | | |
| P99 (ms) | | | |
| Open Bytes (MB) | | | |
| Open Read Time (ms) | | | |
| Search Bytes (KB) | | | |
| Search Read Time (ms) | | | |

## 7. 分析与结论

### 7.1 构建性能分析

<!-- 
填写观察到的差异，例如：
- ES 单线程构建 vs Lumina 多线程构建的时间差异
- 索引文件大小差异（HNSW 图结构 vs DiskANN 图结构）
- 不同数据规模下的扩展性
-->

### 7.2 查询性能分析

<!--
填写观察到的差异，例如：
- 延迟分布特征（P50 vs P99 差异是否显著）
- Open 阶段 vs Search 阶段的时间占比
- I/O 模式差异（seek 次数、读取粒度）
-->

### 7.3 适用场景建议

| 场景 | 推荐方案 | 原因 |
|------|---------|------|
| 小数据集 (<100K) | <!-- 待定 --> | |
| 大数据集 (>1M) | <!-- 待定 --> | |
| 低延迟优先 | <!-- 待定 --> | |
| 低存储空间 | <!-- 待定 --> | |
| OSS 远程查询 | <!-- 待定 --> | |
| 多线程构建 | <!-- 待定 --> | |

## 8. 附录

### 8.1 ES HNSW 参数调优建议

| 参数 | 范围 | 影响 |
|------|------|------|
| `es.hnsw.m` | 8-64 | 越大→图连接密度高→查询更准但构建更慢、索引更大 |
| `es.hnsw.ef_construction` | 32-500 | 越大→构建时搜索范围广→索引质量高但构建更慢 |
| `es.hnsw.beam_width` | 32-500 | 构建时 beam search 宽度，影响图质量 |

### 8.2 Lumina DiskANN 参数调优建议

| 参数 | 范围 | 影响 |
|------|------|------|
| `lumina.diskann.build.neighbor_count` | 16-128 | 越大→图连接密度高→查询更准但索引更大 |
| `lumina.diskann.build.ef_construction` | 32-256 | 构建时搜索范围，影响图质量 |
| `lumina.encoding.type` | rawf32/pq/sq8 | PQ/SQ8 减小索引体积，可能损失精度 |
| `lumina.encoding.pq.m` | 32-128 | PQ 子量化器数量，越大精度越高 |
