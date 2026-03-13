<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
-->

# RESTCatalog FUSE 本地路径配置设计

## 背景

在使用 Paimon RESTCatalog 访问远端对象存储（如 OSS、S3、HDFS）时，数据访问通常通过远端存储 SDK 进行。然而，在远端存储路径通过 FUSE（用户空间文件系统）挂载到本地的场景下，用户可以直接通过本地文件系统路径访问数据，获得更好的性能。

本设计引入配置参数以支持 FUSE 挂载的远端存储路径，允许用户在 Catalog、Database 和 Table 三个层级指定本地路径映射。

## 目标

1. 支持通过本地文件系统访问 FUSE 挂载的远端存储路径
2. 支持分层路径映射：Catalog 根路径 > Database > Table
3. 当 FUSE 本地路径适用时，使用本地 FileIO 进行数据读写
4. 保持与现有 RESTCatalog 行为的向后兼容性

## 配置参数

所有参数定义在 `RESTCatalogOptions.java` 中：

| 参数 | 类型 | 默认值 | 描述 |
|-----|------|--------|------|
| `fuse.local-path.enabled` | Boolean | `false` | 是否启用 FUSE 本地路径映射 |
| `fuse.local-path.root` | String | (无) | FUSE 挂载的本地根路径，如 `/mnt/fuse` |
| `fuse.local-path.database` | Map<String, String> | `{}` | Database 级别的本地路径映射。格式：`db1:/local/path1,db2:/local/path2` |
| `fuse.local-path.table` | Map<String, String> | `{}` | Table 级别的本地路径映射。格式：`db1.table1:/local/path1,db2.table2:/local/path2` |

## 使用示例

### SQL 配置（Flink/Spark）

```sql
CREATE CATALOG paimon_rest_catalog WITH (
    'type' = 'paimon',
    'metastore' = 'rest',
    'uri' = 'http://rest-server:8080',
    'token' = 'xxx',

    -- FUSE 本地路径配置
    'fuse.local-path.enabled' = 'true',
    'fuse.local-path.root' = '/mnt/fuse/warehouse',
    'fuse.local-path.database' = 'db1:/mnt/custom/db1,db2:/mnt/custom/db2',
    'fuse.local-path.table' = 'db1.table1:/mnt/special/t1'
);
```

### 路径解析优先级

解析路径时，系统按以下顺序检查（优先级从高到低）：

1. **Table 级别映射**（`fuse.local-path.table`）
2. **Database 级别映射**（`fuse.local-path.database`）
3. **根路径映射**（`fuse.local-path.root`）

示例：对于表 `db1.table1`：
- 如果 `fuse.local-path.table` 包含 `db1.table1:/mnt/special/t1`，使用 `/mnt/special/t1`
- 否则，如果 `fuse.local-path.database` 包含 `db1:/mnt/custom/db1`，使用 `/mnt/custom/db1`
- 否则，使用 `fuse.local-path.root`（如 `/mnt/fuse/warehouse`）

## 实现方案

### RESTCatalog 修改

修改 `RESTCatalog.java` 中的 `fileIOForData` 方法：

```java
private FileIO fileIOForData(Path path, Identifier identifier) {
    // 如果 FUSE 本地路径启用且路径匹配，使用本地 FileIO
    if (fuseLocalPathEnabled) {
        Path localPath = resolveFUSELocalPath(path, identifier);
        if (localPath != null) {
            // 使用本地文件 IO，无需 token
            return FileIO.get(localPath, CatalogContext.create(new Options(), context.hadoopConf()));
        }
    }
    
    // 原有逻辑：data token 或 ResolvingFileIO
    return dataTokenEnabled
            ? new RESTTokenFileIO(context, api, identifier, path)
            : fileIOFromOptions(path);
}

/**
 * 解析 FUSE 本地路径。优先级：table > database > root。
 * @return 本地路径，如果不适用则返回 null
 */
private Path resolveFUSELocalPath(Path originalPath, Identifier identifier) {
    String pathStr = originalPath.toString();
    
    // 1. 检查 Table 级别映射
    Map<String, String> tableMappings = context.options().get(FUSE_LOCAL_PATH_TABLE);
    String tableKey = identifier.getDatabaseName() + "." + identifier.getTableName();
    if (tableMappings.containsKey(tableKey)) {
        String localRoot = tableMappings.get(tableKey);
        return convertToLocalPath(pathStr, localRoot);
    }
    
    // 2. 检查 Database 级别映射
    Map<String, String> dbMappings = context.options().get(FUSE_LOCAL_PATH_DATABASE);
    if (dbMappings.containsKey(identifier.getDatabaseName())) {
        String localRoot = dbMappings.get(identifier.getDatabaseName());
        return convertToLocalPath(pathStr, localRoot);
    }
    
    // 3. 使用根路径映射
    String fuseRoot = context.options().get(FUSE_LOCAL_PATH_ROOT);
    if (fuseRoot != null) {
        return convertToLocalPath(pathStr, fuseRoot);
    }
    
    return null;
}

private Path convertToLocalPath(String originalPath, String localRoot) {
    // 将远端存储路径转换为本地 FUSE 路径
    // 示例：oss://bucket/warehouse/db1/table1 -> /mnt/fuse/warehouse/db1/table1
    // 具体实现取决于路径结构
}
```

### 行为矩阵

| 配置 | 路径匹配 | 行为 |
|-----|---------|------|
| `fuse.local-path.enabled=true` | 是 | 本地 FileIO 进行数据读写 |
| `fuse.local-path.enabled=true` | 否 | 回退到原有逻辑 |
| `fuse.local-path.enabled=false` | 不适用 | 原有逻辑（data token 或 ResolvingFileIO） |

## 优势

1. **性能提升**：本地文件系统访问通常比基于网络的远端存储访问更快
2. **灵活性**：支持为不同的数据库/表配置不同的本地路径
3. **向后兼容**：默认禁用，现有行为不变

## 安全校验机制

### 问题场景

错误的 FUSE 本地路径配置可能导致严重的数据一致性问题：

| 场景 | 描述 | 后果 |
|-----|------|------|
| **本地路径未挂载** | 用户配置的 `/local/table` 实际没有 FUSE 挂载 | 数据仅写入本地磁盘，未同步到远端存储，导致数据丢失 |
| **远端路径错误** | 本地路径指向了其他库表的远端存储路径 | 数据写入错误的表，导致数据污染 |

### 校验模式配置

新增配置参数控制校验行为：

| 参数 | 类型 | 默认值 | 描述 |
|-----|------|--------|------|
| `fuse.local-path.validation-mode` | String | `strict` | 校验模式：`strict`（严格）、`warn`（警告）、`none`（不校验） |

**校验模式说明**：

| 模式 | 行为 |
|-----|------|
| `strict` | 启用校验，失败时抛出异常，阻止操作 |
| `warn` | 启用校验，失败时输出警告日志，但允许操作继续 |
| `none` | 不进行校验（不推荐，可能导致数据丢失或污染） |

### 校验流程

```
┌─────────────────────────────────────────────────────────────┐
│                     访问表（getTable）                        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│           fuse.local-path.enabled == true ?                 │
└─────────────────────────────────────────────────────────────┘
                    │                    │
                   Yes                   No
                    │                    │
                    ▼                    ▼
        ┌───────────────────┐    ┌───────────────────┐
        │ 解析本地路径       │    │ 使用原有逻辑       │
        │ resolveFUSELocalPath│   │ (RESTTokenFileIO) │
        └───────────────────┘    └───────────────────┘
                    │
                    ▼
        ┌───────────────────────────────────────┐
        │ validation-mode != none ?              │
        └───────────────────────────────────────┘
                    │                    │
                   Yes                   No
                    │                    │
                    ▼                    ▼
        ┌───────────────────┐    ┌───────────────────┐
        │ 校验本地路径存在    │    │ 跳过校验           │
        │ 与远端数据比对      │    │ 直接使用本地路径    │
        └───────────────────┘    └───────────────────┘
                    │
                    ▼
        ┌───────────────────────────────────────┐
        │ 校验通过 ?                             │
        └───────────────────────────────────────┘
           │                    │
          Yes                   No
           │                    │
           ▼                    ▼
    ┌─────────────┐    ┌─────────────────────┐
    │ 使用本地路径 │    │ validation-mode:     │
    │ LocalFileIO │    │  - strict: 抛异常     │
    └─────────────┘    │  - warn: 警告+回退    │
                       └─────────────────────┘
```

### .identifier 文件

每个表目录下都包含一个 `.identifier` 文件用于快速校验：

**文件位置**：`<表路径>/.identifier`

**文件格式**：
```json
{"uuid":"xxx-xxx-xxx-xxx"}
```

**用途**：
- 比对本地和远端路径的表 UUID
- 在昂贵的文件内容比对前进行快速校验
- 创建表时自动生成
- 仅需 UUID（database/table 名称可能因重命名而变化）

### 安全校验实现

使用远端数据校验验证 FUSE 路径正确性：通过现有 FileIO 读取远端存储文件，与本地文件比对。

**完整实现**：

```java
/**
 * RESTCatalog 中 fileIOForData 的完整实现
 * 结合 FUSE 本地路径校验与远端数据校验
 */
private FileIO fileIOForData(Path path, Identifier identifier) {
    // 如果 FUSE 本地路径启用，尝试使用本地路径
    if (fuseLocalPathEnabled) {
        Path localPath = resolveFUSELocalPath(path, identifier);
        if (localPath != null) {
            // 根据校验模式执行校验
            ValidationMode mode = getValidationMode();

            if (mode != ValidationMode.NONE) {
                ValidationResult result = validateFUSEPath(localPath, path, identifier);
                if (!result.isValid()) {
                    handleValidationError(result, mode);
                    // 校验失败，回退到原有逻辑
                    return createDefaultFileIO(path, identifier);
                }
            }

            // 校验通过或跳过校验，使用本地 FileIO
            return createLocalFileIO(localPath);
        }
    }

    // 原有逻辑：data token 或 ResolvingFileIO
    return createDefaultFileIO(path, identifier);
}

/**
 * 校验 FUSE 本地路径
 */
private ValidationResult validateFUSEPath(Path localPath, Path remotePath, Identifier identifier) {
    // 1. 创建 LocalFileIO 用于本地路径操作
    LocalFileIO localFileIO = LocalFileIO.create();

    // 2. 检查本地路径是否存在
    if (!localFileIO.exists(localPath)) {
        return ValidationResult.fail("本地路径不存在: " + localPath);
    }

    // 3. 第一次校验：表标识文件
    ValidationResult identifierResult = validateByIdentifierFile(localFileIO, localPath, remotePath, identifier);
    if (!identifierResult.isSuccess()) {
        return identifierResult;
    }

    // 4. 第二次校验：远端数据校验
    return validateByRemoteData(localFileIO, localPath, remotePath, identifier);
}

/**
 * 第一次校验：检查 .identifier 文件
 * 比对本地和远端的表 UUID 确保路径正确性
 */
private ValidationResult validateByIdentifierFile(
        LocalFileIO localFileIO, Path localPath, Path remotePath, Identifier identifier) {
    try {
        // 1. 获取远端存储 FileIO
        FileIO remoteFileIO = createDefaultFileIO(remotePath, identifier);

        // 2. 读取远端标识文件
        Path remoteIdentifierFile = new Path(remotePath, ".identifier");
        if (!remoteFileIO.exists(remoteIdentifierFile)) {
            // 无标识文件，跳过此次校验
            LOG.debug("未找到表 {} 的 .identifier 文件，跳过标识校验", identifier);
            return ValidationResult.success();
        }

        String remoteIdentifier = readIdentifierFile(remoteFileIO, remoteIdentifierFile);

        // 3. 读取本地标识文件
        Path localIdentifierFile = new Path(localPath, ".identifier");
        if (!localFileIO.exists(localIdentifierFile)) {
            return ValidationResult.fail(
                "本地 .identifier 文件未找到: " + localIdentifierFile +
                "。FUSE 路径可能未正确挂载。");
        }

        String localIdentifier = readIdentifierFile(localFileIO, localIdentifierFile);

        // 4. 比对标识符
        if (!remoteIdentifier.equals(localIdentifier)) {
            return ValidationResult.fail(String.format(
                "表标识不匹配！本地: %s，远端: %s。" +
                "本地路径可能指向了其他表。",
                localIdentifier, remoteIdentifier));
        }

        return ValidationResult.success();

    } catch (Exception e) {
        LOG.warn("标识文件校验失败: {}", identifier, e);
        return ValidationResult.fail("标识文件校验失败: " + e.getMessage());
    }
}

/**
 * 读取 .identifier 文件内容
 * 格式：{"uuid":"xxx-xxx-xxx-xxx"}
 */
private String readIdentifierFile(FileIO fileIO, Path identifierFile) throws IOException {
    try (InputStream in = fileIO.newInputStream(identifierFile)) {
        String json = IOUtils.readUTF8Fully(in);
        JsonNode node = JsonSerdeUtil.fromJson(json, JsonNode.class);
        return node.get("uuid").asText();
    }
}

/**
 * 第二次校验：通过比对远端存储和本地文件验证 FUSE 路径正确性
 * 使用现有 FileIO（RESTTokenFileIO 或 ResolvingFileIO）读取远端存储文件
 */
private ValidationResult validateByRemoteData(
        LocalFileIO localFileIO, Path localPath, Path remotePath, Identifier identifier) {
    try {
        // 1. 获取远端存储 FileIO（使用现有逻辑，可访问远端存储）
        FileIO remoteFileIO = createDefaultFileIO(remotePath, identifier);

        // 2. 使用 SnapshotManager 获取最新 snapshot
        SnapshotManager snapshotManager = new SnapshotManager(remoteFileIO, remotePath);
        Snapshot latestSnapshot = snapshotManager.latestSnapshot();

        Path checksumFile;
        if (latestSnapshot != null) {
            // 有 snapshot，使用 snapshot 文件校验
            checksumFile = snapshotManager.snapshotPath(latestSnapshot.id());
        } else {
            // 无 snapshot（新表），使用 schema 文件校验
            SchemaManager schemaManager = new SchemaManager(remoteFileIO, remotePath);
            Optional<TableSchema> latestSchema = schemaManager.latest();
            if (!latestSchema.isPresent()) {
                // 无 schema（如 format 表、object 表），跳过验证
                LOG.info("未找到表 {} 的 snapshot 或 schema，跳过验证", identifier);
                return ValidationResult.success();
            }
            checksumFile = schemaManager.toSchemaPath(latestSchema.get().id());
        }

        // 3. 读取远端文件内容并计算 hash
        FileStatus remoteStatus = remoteFileIO.getFileStatus(checksumFile);
        String remoteHash = computeFileHash(remoteFileIO, checksumFile);

        // 4. 构建本地文件路径并计算 hash
        Path localChecksumFile = new Path(localPath, remotePath.toUri().getPath());

        if (!localFileIO.exists(localChecksumFile)) {
            return ValidationResult.fail(
                "本地文件未找到: " + localChecksumFile +
                "。FUSE 路径可能未正确挂载。");
        }

        long localSize = localFileIO.getFileSize(localChecksumFile);
        String localHash = computeFileHash(localFileIO, localChecksumFile);

        // 5. 比对文件特征
        if (localSize != remoteStatus.getLen()) {
            return ValidationResult.fail(String.format(
                "文件大小不匹配！本地: %d 字节, 远端: %d 字节。",
                localSize, remoteStatus.getLen()));
        }

        if (!localHash.equalsIgnoreCase(remoteHash)) {
            return ValidationResult.fail(String.format(
                "文件内容哈希不匹配！本地: %s, 远端: %s。",
                localHash, remoteHash));
        }

        return ValidationResult.success();

    } catch (Exception e) {
        LOG.warn("通过远端数据验证 FUSE 路径失败: {}", identifier, e);
        return ValidationResult.fail("远端数据验证失败: " + e.getMessage());
    }
}

/**
 * 使用 FileIO 计算文件内容哈希
 */
private String computeFileHash(FileIO fileIO, Path file) throws IOException {
    MessageDigest md;
    try {
        md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
        throw new IOException("MD5 算法不可用", e);
    }

    try (InputStream is = fileIO.newInputStream(file)) {
        byte[] buffer = new byte[4096];
        int bytesRead;
        while ((bytesRead = is.read(buffer)) != -1) {
            md.update(buffer, 0, bytesRead);
        }
    }
    return Hex.encodeHexString(md.digest());
}

/**
 * 处理校验错误
 */
private void handleValidationError(ValidationResult result, ValidationMode mode) {
    String errorMsg = "FUSE local path validation failed: " + result.getErrorMessage();

    switch (mode) {
        case STRICT:
            throw new IllegalArgumentException(errorMsg);
        case WARN:
            LOG.warn(errorMsg + ". Falling back to default FileIO.");
            break;
        case NONE:
            // 不会执行到这里
            break;
    }
}

/**
 * 使用现有 context 创建本地 FileIO
 */
private FileIO createLocalFileIO(Path localPath) {
    return FileIO.get(localPath, context);
}

/**
 * 创建默认 FileIO（原有逻辑）
 */
private FileIO createDefaultFileIO(Path path, Identifier identifier) {
    return dataTokenEnabled
        ? new RESTTokenFileIO(context, api, identifier, path)
        : fileIOFromOptions(path);
}

// ========== 辅助类 ==========

enum ValidationMode {
    STRICT,  // 严格模式：校验失败抛异常
    WARN,    // 警告模式：校验失败只警告，回退到默认逻辑
    NONE     // 不校验
}

class ValidationResult {
    private final boolean valid;
    private final String errorMessage;

    private ValidationResult(boolean valid, String errorMessage) {
        this.valid = valid;
        this.errorMessage = errorMessage;
    }

    static ValidationResult success() {
        return new ValidationResult(true, null);
    }

    static ValidationResult fail(String errorMessage) {
        return new ValidationResult(false, errorMessage);
    }

    boolean isValid() { return valid; }
    String getErrorMessage() { return errorMessage; }
}
```

**方案优势**：

| 优势 | 说明 |
|------|------|
| **无需扩展 API** | 使用现有 FileIO 和 SnapshotManager/SchemaManager |
| **使用 LATEST snapshot** | 通过 `SnapshotManager.latestSnapshot()` 直接获取，无需遍历 |
| **新表支持** | 无 snapshot 时自动回退到 schema 文件校验 |
| **准确性最高** | 直接验证数据一致性，确保路径正确 |
| **优雅降级** | 校验失败可回退到默认 FileIO |

**校验文件选择逻辑**：

| 场景 | 校验文件 |
|------|----------|
| 有 snapshot | 使用 `SnapshotManager.latestSnapshot()` 获取的最新 snapshot 文件 |
| 无 snapshot（新表）| 使用 `SchemaManager.latest()` 获取的最新 schema 文件 |
| 无 schema（如 format 表、object 表）| 跳过校验 |

**两步校验**：

| 步骤 | 校验方式 | 描述 |
|------|----------|------|
| 1 | `.identifier` 文件 | 比对本地和远端的表 UUID |
| 2 | 远端数据校验 | 比对 snapshot/schema 文件内容 |

**完整校验流程**：

```
┌─────────────────────────────────────────────────────────────┐
│                       校验流程                               │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│           第一步：.identifier 校验                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌───────────────────────────────────────┐
        │ 远端存在 .identifier ?          │
        └───────────────────────────────────────┘
           │                    │
          Yes                   No
           │                    │
           ▼                    ▼
┌─────────────────────┐  ┌─────────────────────────────────────┐
│ 比对 UUID           │  │ 跳过第一步，进入第二步               │
│ 本地 vs 远端        │  │ （远端数据校验）                     │
└─────────────────────┘  └─────────────────────────────────────┘
           │
           ▼
        ┌───────────────────────────────────────┐
        │ UUID 匹配 ?                            │
        └───────────────────────────────────────┘
           │                    │
          Yes                   No → 失败：表标识不匹配
           │
           ▼
┌─────────────────────────────────────────────────────────────┐
│           第二步：远端数据校验                                │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│  1. 获取远端存储 FileIO（RESTTokenFileIO 或 ResolvingFileIO）│
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│     2. 通过 SnapshotManager 获取最新 snapshot               │
│        snapshotManager.latestSnapshot()                     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌───────────────────────────────────────┐
        │ Snapshot 存在 ?                        │
        └───────────────────────────────────────┘
           │                    │
          Yes                   No
           │                    │
           ▼                    ▼
┌─────────────────────┐  ┌─────────────────────────────────────┐
│ 使用 snapshot 文件  │  │ 通过 SchemaManager 获取最新 schema   │
│ 进行校验            │  │ schemaManager.latest()              │
└─────────────────────┘  └─────────────────────────────────────┘
                              │
                              ▼
        ┌───────────────────────────────────────┐
        │ Schema 存在 ?                          │
        └───────────────────────────────────────┘
           │                    │
          Yes                   No → 跳过校验（format/object 表）
           │
           ▼
┌─────────────────────────────────────────────────────────────┐
│     3. 获取远端文件元数据（大小）                            │
│        计算远端文件 hash                                    │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│     4. 读取本地对应文件                                      │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌───────────────────────────────────────┐
        │ 本地文件存在 ?                         │
        └───────────────────────────────────────┘
           │                    │
          Yes                   No → 校验失败（路径错误或未挂载）
           │
           ▼
        ┌───────────────────────────────────────┐
        │ 文件大小匹配 ?                         │
        └───────────────────────────────────────┘
           │                    │
          Yes                   No → 校验失败
           │
           ▼
        ┌───────────────────────────────────────┐
        │ 文件内容 hash 匹配 ?                   │
        └───────────────────────────────────────┘
           │                    │
          Yes                   No → 校验失败（路径指向错误表）
           │
           ▼
    ┌─────────────┐
    │ 校验通过     │
    │ 可安全使用   │
    └─────────────┘
```

### 使用示例（启用安全校验）

```sql
CREATE CATALOG paimon_rest_catalog WITH (
    'type' = 'paimon',
    'metastore' = 'rest',
    'uri' = 'http://rest-server:8080',
    'token' = 'xxx',

    -- FUSE 本地路径配置
    'fuse.local-path.enabled' = 'true',
    'fuse.local-path.root' = '/mnt/fuse/warehouse',

    -- 安全校验配置（可选，默认 strict）
    'fuse.local-path.validation-mode' = 'strict'  -- strict/warn/none
);
```

## 限制

1. FUSE 挂载必须正确配置且可访问
2. 本地路径必须与远端存储路径具有相同的目录结构
3. 写操作需要本地 FUSE 挂载点具有适当的权限
4. Windows 平台 FUSE 支持有限（需第三方工具如 WinFsp）
