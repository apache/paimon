# 标量+向量过滤 Benchmark 测试记录

## 测试日期

2026-04-20

## 测试文件

`paimon-elasticsearch/src/test/java/org/apache/paimon/elasticsearch/index/LuceneVsESBenchmark.java`

## 测试设计

### 目标

验证 Lumina DiskANN 在纯向量搜索和标量过滤+向量搜索两种场景下的性能差异，使用**低层 API** 直接操作索引组件。

### 三个场景

| 场景 | 描述 | 过滤条件 | 预期通过率 |
|------|------|---------|-----------|
| A | 纯向量搜索 | 无 | 100% |
| B | 标量+向量（高选择性） | `id >= numRows * 0.8` | ~20% |
| C | 标量+向量（低选择性） | `id >= numRows / 2` | ~50% |

### 测试矩阵

| 行数 | 维度 |
|------|------|
| 1,000 | 32 |
| 5,000 | 32 |
| 10,000 | 32 |
| 1,000 | 128 |
| 5,000 | 128 |
| 10,000 | 128 |

### 关键参数

- TopK: 10
- Warmup: 5 次
- Iterations: 50 次
- 距离度量: L2
- 编码: rawf32
- 随机种子: 42 (数据) / 99999 (查询)

## 低层 API 调用链路

```
场景 A (纯向量):
  LuminaVectorGlobalIndexReader.visitVectorSearch(
      new VectorSearch(query, topK, "vec"))

场景 B/C (标量+向量):
  Step 1: GlobalIndexScanner.create(table, btreeIndexFiles)
          → scanner.scan(predicate)
          → GlobalIndexResult.results()
          → RoaringNavigableMap64 rowIds

  Step 2: LuminaVectorGlobalIndexReader.visitVectorSearch(
              new VectorSearch(query, topK, "vec")
                  .withIncludeRowIds(rowIds))
```

### 索引构建流程

1. 写入数据: `BatchTableWrite` → `BatchTableCommit`
2. 构建向量索引: `GlobalIndexBuilderUtils.createIndexWriter(table, "lumina-vector-ann", vecField, options)` → write → finish → commit
3. 构建 BTree 标量索引: `GlobalIndexBuilderUtils.createIndexWriter(table, "btree", idField, options)` → write → finish → commit
4. 从已提交的表获取索引文件: `table.store().newIndexFileHandler().scan(snapshot, filter)` → `IndexFileMeta` → `GlobalIndexIOMeta`

## 本地测试结果 (2026-04-20)

### 环境

| 项目 | 配置 |
|------|------|
| 机器 | MacBook (macOS Darwin 24.5.0) |
| JDK | Java 11+ |
| 分支 | paimon-es-test |
| Lumina native lib | 未安装 |

### 编译结果

```
mvn compile test-compile -pl paimon-elasticsearch -am -Dcheckstyle.skip=true
→ BUILD SUCCESS
```

### ES 单元测试

```
mvn test -pl paimon-elasticsearch -Dcheckstyle.skip=true
→ Tests run: 9, Failures: 0, Errors: 0, Skipped: 0
→ BUILD SUCCESS
```

### Benchmark 测试

```
mvn test -pl paimon-elasticsearch -Dtest=LuceneVsESBenchmark#benchmark -Dcheckstyle.skip=true
→ Tests run: 1, Failures: 0, Errors: 0, Skipped: 1
→ BUILD SUCCESS (auto-skipped: Lumina native library not available)
```

## 在远程机器运行

需要安装 Lumina native library (`liblumina.so` / `liblumina.dylib`)。

```bash
mvn test -pl paimon-elasticsearch \
  -Dtest=LuceneVsESBenchmark#benchmark \
  -Dcheckstyle.skip=true
```

## 远程机器测试结果

已完成 (2026-04-20)

### 环境

| 项目 | 配置 |
|------|------|
| 机器 | Alibaba Cloud ECS (root@11.160.72.171) |
| CPU | Intel Xeon Platinum 8475B |
| 内存 | 128 GB |
| OS | Alibaba Cloud Linux 3 (x86_64) |
| JDK | Alibaba Dragonwell 21.0.10 |
| Lumina | lumina-jni 0.1.0 (native lib auto-extracted from jar) |

### 性能数据

#### 32 维

| 行数 | 场景 | Avg (ms) | P50 (ms) | P90 (ms) | P99 (ms) | Overhead vs Pure |
|------|------|---------|---------|---------|---------|-----------------|
| 1,000 | A: 纯向量 | 0.24 | 0.21 | 0.29 | 1.14 | — |
| 1,000 | B: Filter~20% | 2.90 | 2.85 | 3.28 | 3.91 | +1114.0% |
| 1,000 | C: Filter~50% | 2.84 | 2.77 | 3.25 | 3.76 | +1089.3% |
| 5,000 | A: 纯向量 | 0.21 | 0.19 | 0.22 | 1.22 | — |
| 5,000 | B: Filter~20% | 2.54 | 2.53 | 2.74 | 3.36 | +1103.9% |
| 5,000 | C: Filter~50% | 2.86 | 2.85 | 3.14 | 3.69 | +1256.5% |
| 10,000 | A: 纯向量 | 0.20 | 0.17 | 0.21 | 1.43 | — |
| 10,000 | B: Filter~20% | 2.76 | 2.68 | 3.03 | 7.61 | +1257.5% |
| 10,000 | C: Filter~50% | 2.84 | 2.78 | 3.13 | 3.44 | +1294.0% |

#### 128 维

| 行数 | 场景 | Avg (ms) | P50 (ms) | P90 (ms) | P99 (ms) | Overhead vs Pure |
|------|------|---------|---------|---------|---------|-----------------|
| 1,000 | A: 纯向量 | 0.22 | 0.20 | 0.24 | 1.21 | — |
| 1,000 | B: Filter~20% | 2.35 | 2.33 | 2.51 | 3.07 | +949.1% |
| 1,000 | C: Filter~50% | 2.04 | 2.02 | 2.21 | 2.37 | +810.6% |
| 5,000 | A: 纯向量 | 0.24 | 0.21 | 0.24 | 1.53 | — |
| 5,000 | B: Filter~20% | 2.69 | 2.68 | 3.02 | 3.19 | +1035.0% |
| 5,000 | C: Filter~50% | 2.74 | 2.62 | 3.23 | 3.51 | +1056.8% |
| 10,000 | A: 纯向量 | 0.27 | 0.22 | 0.26 | 2.40 | — |
| 10,000 | B: Filter~20% | 3.46 | 3.36 | 3.86 | 5.14 | +1168.5% |
| 10,000 | C: Filter~50% | 3.70 | 3.61 | 4.14 | 4.20 | +1256.7% |

### 分析

- **纯向量搜索**: 延迟极低，Avg 0.20-0.27ms，P50 在 0.17-0.22ms
- **标量过滤+向量搜索**: 延迟约 2-4ms，主要开销来自 BTree 标量索引扫描 (`GlobalIndexScanner.scan`)
- **Overhead**: 约 800%~1300%，过滤操作开销是固定成本（BTree 扫描），与向量搜索本身无关
- **维度影响**: 32 维和 128 维的纯向量搜索延迟差异不大，说明 DiskANN 对维度不敏感
- **行数影响**: 行数增加对纯向量搜索影响很小，但过滤操作略有增长

## pom.xml 变更

为支持 `TableTestBase`，添加了以下 test 依赖:

```xml
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-common</artifactId>
    <scope>test</scope>
</dependency>
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-hdfs-client</artifactId>
    <scope>test</scope>
</dependency>
```

## 文件清单

| 文件 | 状态 | 说明 |
|------|------|------|
| `paimon-elasticsearch/pom.xml` | 修改 | 添加 hadoop test 依赖 |
| `LuceneVsESBenchmark.java` | 重写 | 低层 API: GlobalIndexScanner + LuminaReader |
| `ESVectorGlobalIndexTest.java` | 无变更 | 9 个测试全部通过 |
