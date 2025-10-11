# Paimon Chain Table Implementation Plan

## 1. 概述

Chain Table是Paimon的一个新功能，旨在解决数据仓库中周期性存储全量数据的场景问题。它通过将数据分为增量分支(delta)和快照分支(snapshot)来优化存储和计算性能。

### 1.1 动机

在数据仓库系统中，存在一种典型的场景：周期性地存储全量数据（如每天或每小时）。但在连续的时间间隔之间，大部分数据是冗余的，只有少量新变更的数据。传统的处理方式存在以下问题：

1. **全量计算**：合并操作涉及全量计算和shuffle，性能较差
2. **全量存储**：每天存储全量数据，新变更数据通常只占很小比例（如1%）

Chain Table通过以下方式优化：

1. **增量计算**：离线ETL作业只需消费当天的新变更数据，无需全量合并
2. **增量存储**：每天只存储新变更数据，定期异步压缩构建全局链表

## 2. 设计方案

### 2.1 配置项

| Key | 默认值 | 类型 | 描述 |
| --- | --- | --- | --- |
| chain-table.enabled | false | Boolean | 是否启用链表 |
| scan.fallback-snapshot-branch | (none) | String | 当主分支不存在分区时，回退到链表读取的快照分支 |
| scan.fallback-delta-branch | (none) | String | 当主分支不存在分区时，回退到链表读取的增量分支 |

### 2.2 解决方案

在仓库基础上新增两个分支：delta和snapshot，分别描述新变更数据和链表压缩产生的全量数据。

#### 2.2.1 表结构

1. **快照分支(Snapshot Branch)**：存储链表压缩或引导产生的全量数据
2. **增量分支(Delta Branch)**：存储新变更数据

#### 2.2.2 写入策略

根据分支配置将数据写入对应分支，以分区20250722为例：

1. **增量写入**：写入到t$branch_delta
2. **全量写入(链表压缩)**：写入到t$branch_snapshot

#### 2.2.3 读取策略

##### 全量批处理读取

根据快照分支是否存在分区采取相应策略：

1. 查询20250722分区时，如果t$branch_snapshot存在该分区，则直接从t$branch_snapshot读取
2. 查询20250726分区时，如果t$branch_snapshot不存在该分区，则读取t$branch_snapshot中最接近的全量分区和t$branch_delta中截至当前时间的所有增量分区

##### 增量批处理读取

直接从t$branch_delta读取增量分区，例如查询20250722分区时，直接从t$branch_delta读取

##### 流式读取

直接从t$branch_delta读取数据

#### 2.2.4 链表压缩

将当前周期的增量数据与前一周期的全量数据合并生成当天的全量数据。例如，date=20250729的全量数据通过合并t$branch_delta中20250723到20250729的所有增量分区和t$branch_snapshot中20250722的全量数据生成。

## 3. 实现方案

### 3.1 核心类设计

#### 3.1.1 ChainFileStoreTable

继承自FallbackReadFileStoreTable，实现链表的分割和读取功能。

```java
public class ChainFileStoreTable extends FallbackReadFileStoreTable {
    private final FileStoreTable snapshotStoreTable;
    private final FileStoreTable deltaStoreTable;
    
    public ChainFileStoreTable(
            AbstractFileStoreTable snapshotStoreTable, 
            AbstractFileStoreTable deltaStoreTable) {
        super(snapshotStoreTable, deltaStoreTable);
        this.snapshotStoreTable = snapshotStoreTable;
        this.deltaStoreTable = deltaStoreTable;
    }
    
    // 实现链表特定的扫描和读取逻辑
    @Override
    public DataTableScan newScan() {
        return new ChainTableBatchScan(snapshotStoreTable.newScan(), deltaStoreTable.newScan());
    }
    
    @Override
    public InnerTableRead newRead() {
        return new ChainTableRead(snapshotStoreTable.newRead(), deltaStoreTable.newRead());
    }
}
```

#### 3.1.2 ChainTableBatchScan

实现链表的批处理扫描逻辑。

```java
public class ChainTableBatchScan implements DataTableScan {
    private final DataTableScan snapshotScan;
    private final DataTableScan deltaScan;
    
    public ChainTableBatchScan(DataTableScan snapshotScan, DataTableScan deltaScan) {
        this.snapshotScan = snapshotScan;
        this.deltaScan = deltaScan;
    }
    
    @Override
    public TableScan.Plan plan() {
        // 实现链表读取策略
        // 1. 先从快照分支读取存在的分区
        // 2. 对于不存在的分区，从快照分支获取最近的全量数据
        // 3. 从增量分支获取相应时间段的增量数据
        // 4. 合并数据返回
        return new ChainTablePlan(snapshotScan.plan(), deltaScan.plan());
    }
}
```

#### 3.1.3 ChainTableRead

实现链表的读取逻辑。

```java
public class ChainTableRead implements InnerTableRead {
    private final InnerTableRead snapshotRead;
    private final InnerTableRead deltaRead;
    
    public ChainTableRead(InnerTableRead snapshotRead, InnerTableRead deltaRead) {
        this.snapshotRead = snapshotRead;
        this.deltaRead = deltaRead;
    }
    
    @Override
    public RecordReader<InternalRow> createReader(Split split) throws IOException {
        // 根据split类型创建相应的读取器
        if (split instanceof ChainTableSplit) {
            ChainTableSplit chainSplit = (ChainTableSplit) split;
            // 实现链表数据的合并读取
            return new ChainTableRecordReader(chainSplit, snapshotRead, deltaRead);
        }
        // 默认情况，调用父类逻辑
        return snapshotRead.createReader(split);
    }
}
```

### 3.2 配置项实现

在CoreOptions类中添加链表相关配置：

```java
// Chain Table相关配置
public static final ConfigOption<Boolean> CHAIN_TABLE_ENABLED =
    key("chain-table.enabled")
        .booleanType()
        .defaultValue(false)
        .withDescription("Whether to enable chain table");

public static final ConfigOption<String> SCAN_FALLBACK_SNAPSHOT_BRANCH =
    key("scan.fallback-snapshot-branch")
        .stringType()
        .noDefaultValue()
        .withDescription("Snapshot branch when fallback to chain read as partition does not exist in the main branch");

public static final ConfigOption<String> SCAN_FALLBACK_DELTA_BRANCH =
    key("scan.fallback-delta-branch")
        .stringType()
        .noDefaultValue()
        .withDescription("Delta branch when fallback to chain as partition does not exist in the main branch");
```

### 3.3 表工厂改造

修改FileStoreTableFactory以支持链表创建：

```java
public class FileStoreTableFactory {
    public static FileStoreTable create(
            FileIO fileIO,
            Path tablePath,
            TableSchema tableSchema,
            Options dynamicOptions,
            CatalogEnvironment catalogEnvironment) {
        
        // 检查是否启用链表
        CoreOptions coreOptions = new CoreOptions(tableSchema.options());
        if (coreOptions.chainTableEnabled()) {
            // 创建链表实例
            return createChainTable(fileIO, tablePath, tableSchema, dynamicOptions, catalogEnvironment);
        }
        
        // 原有逻辑
        FileStoreTable table =
                createWithoutFallbackBranch(
                        fileIO, tablePath, tableSchema, dynamicOptions, catalogEnvironment);

        Options options = new Options(table.options());
        String fallbackBranch = options.get(CoreOptions.SCAN_FALLBACK_BRANCH);
        if (!StringUtils.isNullOrWhitespaceOnly(fallbackBranch)) {
            // ... 现有回退分支逻辑
        }

        return table;
    }
    
    private static FileStoreTable createChainTable(
            FileIO fileIO,
            Path tablePath,
            TableSchema tableSchema,
            Options dynamicOptions,
            CatalogEnvironment catalogEnvironment) {
        
        CoreOptions coreOptions = new CoreOptions(tableSchema.options());
        String snapshotBranch = coreOptions.scanFallbackSnapshotBranch();
        String deltaBranch = coreOptions.scanFallbackDeltaBranch();
        
        // 创建快照分支表
        TableSchema snapshotSchema = tableSchema.copy(
            Collections.singletonMap(CoreOptions.BRANCH.key(), snapshotBranch));
        FileStoreTable snapshotTable = createWithoutFallbackBranch(
                fileIO, tablePath, snapshotSchema, dynamicOptions, catalogEnvironment);
        
        // 创建增量分支表
        TableSchema deltaSchema = tableSchema.copy(
            Collections.singletonMap(CoreOptions.BRANCH.key(), deltaBranch));
        FileStoreTable deltaTable = createWithoutFallbackBranch(
                fileIO, tablePath, deltaSchema, dynamicOptions, catalogEnvironment);
        
        // 创建链表实例
        return new ChainFileStoreTable(
            (AbstractFileStoreTable) snapshotTable, 
            (AbstractFileStoreTable) deltaTable);
    }
}
```

## 4. 使用示例

### 4.1 创建表

```sql
CREATE TABLE default.t (
  t1 string COMMENT 't1',
  t2 string COMMENT 't2',
  t3 string COMMENT 't3'
) PARTITIONED BY (date string COMMENT 'date')
TBLPROPERTIES (
  'primary_key' = 'date,t1',
  'bucket' = '2',
  'bucket-key' = 't1',
  'partition.timestamp-pattern' = '$date',
  'partition.timestamp-formatter' = 'yyyyMMdd',
  'chain-table.enabled' = 'true',
  'scan.fallback-snapshot-branch' = 'snapshot',
  'scan.fallback-delta-branch' = 'delta'
);
```

### 4.2 创建分支

```sql
CALL sys.create_branch('default.t', 'snapshot');
CALL sys.create_branch('default.t', 'delta');
```

### 4.3 写入数据

```sql
-- 全量写入
INSERT INTO/overwrite `default`.`t$branch_snapshot`
SELECT ...

-- 增量写入
INSERT INTO/overwrite `default`.`t$branch_delta`
SELECT ...
```

### 4.4 读取数据

```sql
-- 全量查询
SELECT * FROM default.t WHERE date = '${date}'

-- 增量查询
SELECT * FROM `default`.`t$branch_delta` WHERE date = '${date}'

-- 混合查询
SELECT * FROM default.t WHERE date = '${date}'
UNION ALL
SELECT * FROM `default`.`t$branch_delta` WHERE date = '${date-1}'
```

## 5. 开发任务分解

### 5.1 第一阶段：核心功能实现

1. **添加配置项** (1天)
   - 在CoreOptions中添加链表相关配置项
   - 添加配置项验证逻辑

2. **创建ChainFileStoreTable类** (2天)
   - 继承FallbackReadFileStoreTable
   - 实现基本的构造函数和核心方法

3. **实现ChainTableBatchScan** (2天)
   - 实现链表扫描逻辑
   - 处理分区回退策略

4. **实现ChainTableRead** (2天)
   - 实现链表读取逻辑
   - 处理数据合并读取

### 5.2 第二阶段：表工厂和集成

1. **修改FileStoreTableFactory** (1天)
   - 添加链表创建逻辑
   - 集成到现有表创建流程

2. **单元测试** (2天)
   - 编写ChainFileStoreTable单元测试
   - 编写ChainTableBatchScan单元测试
   - 编写ChainTableRead单元测试

### 5.3 第三阶段：功能完善和优化

1. **性能优化** (2天)
   - 优化数据合并算法
   - 添加缓存机制

2. **文档完善** (1天)
   - 更新用户文档
   - 添加使用示例

3. **集成测试** (2天)
   - 进行端到端测试
   - 验证各种使用场景

## 6. 测试方案

### 6.1 单元测试

1. **ChainFileStoreTableTest**
   - 测试表的创建和基本功能
   - 测试配置项的正确解析

2. **ChainTableBatchScanTest**
   - 测试扫描逻辑的正确性
   - 测试分区回退策略

3. **ChainTableReadTest**
   - 测试读取逻辑的正确性
   - 测试数据合并功能

### 6.2 集成测试

1. **端到端测试**
   - 创建链表并验证功能
   - 执行写入和读取操作
   - 验证数据一致性

2. **性能测试**
   - 对比链表和普通表的性能差异
   - 验证存储优化效果

## 7. 风险评估和缓解措施

### 7.1 技术风险

1. **数据一致性风险**
   - 风险：链表读取时可能出现数据不一致
   - 缓解：实现严格的事务控制和数据校验机制

2. **性能风险**
   - 风险：数据合并可能影响读取性能
   - 缓解：优化合并算法，添加缓存机制

### 7.2 兼容性风险

1. **向后兼容性**
   - 风险：新功能可能影响现有表的使用
   - 缓解：通过配置项控制功能启用，确保默认行为不变

## 8. 部署和升级方案

### 8.1 部署方案

1. **灰度发布**
   - 先在测试环境部署验证
   - 逐步在生产环境推广

2. **配置管理**
   - 通过配置中心管理链表相关配置
   - 支持动态调整配置

### 8.2 升级方案

1. **平滑升级**
   - 支持现有表无缝升级到链表
   - 提供数据迁移工具

2. **回滚方案**
   - 提供回滚到普通表的功能
   - 确保数据安全

## 9. 监控和运维

### 9.1 监控指标

1. **性能指标**
   - 读取延迟
   - 合并操作耗时
   - 存储空间节省率

2. **健康指标**
   - 分支状态
   - 数据一致性检查结果

### 9.2 运维操作

1. **日常维护**
   - 定期检查分支状态
   - 监控存储空间使用情况

2. **故障处理**
   - 提供故障诊断工具
   - 支持快速恢复机制