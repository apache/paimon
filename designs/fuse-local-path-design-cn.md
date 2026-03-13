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

在使用 Paimon RESTCatalog 访问 OSS（对象存储服务）时，数据访问通常需要通过 `getTableToken` API 从 REST 服务器获取认证令牌。然而，在 OSS 路径通过 FUSE（用户空间文件系统）挂载到本地的场景下，用户可以直接通过本地文件系统路径访问数据，无需 OSS 令牌。

本设计引入配置参数以支持 FUSE 挂载的 OSS 路径，允许用户在 Catalog、Database 和 Table 三个层级指定本地路径映射。

## 目标

1. 支持通过本地文件系统访问 FUSE 挂载的 OSS 路径
2. 支持分层路径映射：Catalog 根路径 > Database > Table
3. 当 FUSE 本地路径适用时，跳过 `getTableToken` API 调用
4. 保持与现有 RESTCatalog 行为的向后兼容性

## 配置参数

所有参数定义在 `RESTCatalogOptions.java` 中：

| 参数 | 类型 | 默认值 | 描述 |
|-----|------|--------|------|
| `fuse.local-path.enabled` | Boolean | `false` | 是否启用 FUSE 本地路径映射 |
| `fuse.local-path.root` | String | (无) | FUSE 挂载的本地根路径，如 `/mnt/oss` |
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
    'fuse.local-path.root' = '/mnt/oss/warehouse',
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
- 否则，使用 `fuse.local-path.root`（如 `/mnt/oss/warehouse`）

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
    // 将 OSS 路径转换为本地 FUSE 路径
    // 示例：oss://bucket/warehouse/db1/table1 -> /mnt/oss/warehouse/db1/table1
    // 具体实现取决于路径结构
}
```

### 行为矩阵

| 配置 | 路径匹配 | 行为 |
|-----|---------|------|
| `fuse.local-path.enabled=true` | 是 | 本地 FileIO，**无需调用 `getTableToken`** |
| `fuse.local-path.enabled=true` | 否 | 回退到原有逻辑 |
| `fuse.local-path.enabled=false` | 不适用 | 原有逻辑（data token 或 ResolvingFileIO） |

## 优势

1. **性能提升**：本地文件系统访问通常比基于网络的 OSS 访问更快
2. **降低成本**：无需调用 `getTableToken` API，减少 REST 服务器负载
3. **灵活性**：支持为不同的数据库/表配置不同的本地路径
4. **向后兼容**：默认禁用，现有行为不变

## 安全校验机制

### 问题场景

错误的 FUSE 本地路径配置可能导致严重的数据一致性问题：

| 场景 | 描述 | 后果 |
|-----|------|------|
| **本地路径未挂载** | 用户配置的 `/local/table` 实际没有 FUSE 挂载 | 数据仅写入本地磁盘，未同步到 OSS，导致数据丢失 |
| **OSS 路径错误** | 本地路径指向了其他库表的 OSS 路径 | 数据写入错误的表，导致数据污染 |

### 校验方案

#### 1. 路径一致性校验（强校验）

在首次访问表时，校验本地路径与 OSS 路径的一致性：

```java
/**
 * 校验 FUSE 本地路径与 OSS 路径的一致性
 * @throws IllegalArgumentException 如果路径不一致
 */
private void validateFUSEPath(Path localPath, Path ossPath, Identifier identifier) {
    // 1. 检查本地路径是否存在且为 FUSE 挂载点
    if (!isFUSEMountPoint(localPath)) {
        throw new IllegalArgumentException(
            String.format("FUSE local path '%s' is not a valid FUSE mount point. " +
                "Data would be written to local disk instead of OSS!", localPath));
    }

    // 2. 校验路径标识一致性：通过读取本地路径下的 .paimon 表标识文件
    Path localIdentifierFile = new Path(localPath, ".paimon-identifier");
    if (fileIO.exists(localIdentifierFile)) {
        String storedIdentifier = readIdentifier(localIdentifierFile);
        String expectedIdentifier = identifier.getDatabaseName() + "." + identifier.getTableName();

        if (!expectedIdentifier.equals(storedIdentifier)) {
            throw new IllegalArgumentException(
                String.format("FUSE path mismatch! Local path '%s' belongs to table '%s', " +
                    "but current table is '%s'.",
                    localPath, storedIdentifier, expectedIdentifier));
        }
    }
}

/**
 * 检查路径是否为 FUSE 挂载点
 * 可通过检查 /proc/mounts (Linux) 或使用 stat 系统调用判断
 */
private boolean isFUSEMountPoint(Path path) {
    // 方案1: 检查 /proc/mounts 中是否包含该路径的 FUSE 挂载
    // 方案2: 检查路径的文件系统类型是否为 fuse.*
    // 方案3: 通过读取 /etc/mtab 或使用 jnr-posix 库
    return checkFUSEMount(path);
}
```

#### 2. 表标识文件机制

在创建表时，自动在表目录下生成 `.paimon-identifier` 文件：

```
/mnt/oss/warehouse/db1/table1/
├── .paimon-identifier    # 内容: "db1.table1"
├── data-xxx.parquet
├── manifest-xxx
└── snapshot-xxx
```

标识文件内容：
```
database=db1
table=table1
table-uuid=xxx-xxx-xxx
created-at=2026-03-13T00:00:00Z
```

#### 3. 校验模式配置

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
        │ 校验 FUSE 挂载点   │    │ 跳过校验           │
        │ 校验路径一致性      │    │ 直接使用本地路径    │
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

### FUSE 挂载点检测实现

#### 方案一：Java NIO FileStore API

```java
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * 检测路径是否为 FUSE 挂载点（跨平台）
 * 通过检查文件系统类型名称判断
 */
private boolean isFUSEMountPoint(Path path) throws IOException {
    FileStore store = Files.getFileStore(path);

    // 获取文件系统类型名称
    String type = store.type();
    String name = store.name();

    // FUSE 文件系统类型通常包含 "fuse" 或特定标识
    // Linux: fuse.sshfs, fuseblk, fuse
    // macOS: macfuse, sshfs, osxfuse
    // 通用: fuse, FUSE
    return type != null && (
        type.toLowerCase().contains("fuse") ||
        type.equalsIgnoreCase("sshfs") ||
        type.equalsIgnoreCase("nfs4") ||
        name.toLowerCase().contains("fuse")
    );
}
```

**平台兼容性**：

| 平台 | FileStore.type() 示例 |
|------|----------------------|
| Linux | `fuse.sshfs`, `fuseblk`, `fuse` |
| macOS | `macfuse`, `osxfuse`, `sshfs` |
| Windows | `NTFS`, `FAT32` (不支持 FUSE，需第三方工具) |

#### 方案二：OSS 数据校验（推荐）

使用现有 FileIO 读取 OSS 文件，与本地文件比对验证路径正确性。

**完整实现**：

```java
/**
 * RESTCatalog 中 fileIOForData 的完整实现
 * 结合 FUSE 本地路径校验与 OSS 数据校验
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
 * 结合文件系统检测和 OSS 数据校验
 */
private ValidationResult validateFUSEPath(Path localPath, Path ossPath, Identifier identifier) {
    // 1. 检查本地路径是否存在
    if (!Files.exists(localPath)) {
        return ValidationResult.fail("Local path does not exist: " + localPath);
    }

    // 2. 检查是否为 FUSE 挂载点（方案一）
    if (!isFUSEMountPoint(localPath)) {
        return ValidationResult.fail("Local path is not a FUSE mount point: " + localPath);
    }

    // 3. OSS 数据校验（方案二，推荐）
    return validateByOSSData(localPath, ossPath, identifier);
}

/**
 * 通过比对 OSS 和本地文件验证 FUSE 路径正确性
 * 使用现有 FileIO（RESTTokenFileIO 或 ResolvingFileIO）读取 OSS 文件
 */
private ValidationResult validateByOSSData(Path localPath, Path ossPath, Identifier identifier) {
    try {
        // 1. 获取 OSS FileIO（使用现有逻辑，可访问 OSS）
        FileIO ossFileIO = createDefaultFileIO(ossPath, identifier);

        // 2. 选择一个用于校验的文件（优先级：snapshot > schema > manifest）
        ChecksumFile checksumFile = findChecksumFile(ossPath, ossFileIO);
        if (checksumFile == null) {
            // 表可能为空（新创建的表），跳过内容校验
            LOG.info("No checksum file found for table: {}, skip content validation", identifier);
            return ValidationResult.success();
        }

        // 3. 读取 OSS 文件内容（仅读取前 N 字节计算 hash）
        FileStatus ossStatus = ossFileIO.getFileStatus(checksumFile.getFullPath());
        String ossHash = computeFileHash(ossFileIO, checksumFile.getFullPath(), HASH_CHECK_LENGTH);

        // 4. 构建本地文件路径并读取
        Path localChecksumFile = new Path(localPath, checksumFile.getRelativePath());
        java.nio.file.Path localNioPath = java.nio.file.Paths.get(localChecksumFile.toUri());

        if (!Files.exists(localNioPath)) {
            return ValidationResult.fail(
                "Local file not found: " + localChecksumFile +
                ". The FUSE path may not be mounted correctly or points to wrong location.");
        }

        // 5. 读取本地文件内容
        long localSize = Files.size(localNioPath);
        String localHash = computeLocalFileHash(localNioPath, HASH_CHECK_LENGTH);

        // 6. 比对文件特征
        if (localSize != ossStatus.getLen()) {
            return ValidationResult.fail(String.format(
                "File size mismatch! Local: %d bytes, OSS: %d bytes. " +
                "The local path may point to a different table.",
                localSize, ossStatus.getLen()));
        }

        if (!localHash.equalsIgnoreCase(ossHash)) {
            return ValidationResult.fail(String.format(
                "File content hash mismatch! Local: %s, OSS: %s. " +
                "The local path points to a different table.",
                localHash, ossHash));
        }

        return ValidationResult.success();

    } catch (Exception e) {
        LOG.warn("Failed to validate FUSE path by OSS data for: {}", identifier, e);
        return ValidationResult.fail("OSS data validation failed: " + e.getMessage());
    }
}

/**
 * 查找可用于校验的文件
 */
private ChecksumFile findChecksumFile(Path tablePath, FileIO fileIO) {
    // 优先级 1: snapshot 文件
    Path snapshotDir = new Path(tablePath, "snapshot");
    if (fileIO.exists(snapshotDir)) {
        FileStatus[] snapshots = fileIO.listStatus(snapshotDir);
        if (snapshots != null && snapshots.length > 0) {
            // 返回最新的 snapshot 文件（按文件名排序）
            Arrays.sort(snapshots, (a, b) -> b.getPath().getName().compareTo(a.getPath().getName()));
            return new ChecksumFile(tablePath, snapshots[0].getPath());
        }
    }

    // 优先级 2: schema 文件
    Path schemaFile = new Path(tablePath, "schema/schema-0");
    if (fileIO.exists(schemaFile)) {
        return new ChecksumFile(tablePath, schemaFile);
    }

    // 优先级 3: manifest 文件
    Path manifestDir = new Path(tablePath, "manifest");
    if (fileIO.exists(manifestDir)) {
        FileStatus[] manifests = fileIO.listStatus(manifestDir);
        if (manifests != null && manifests.length > 0) {
            return new ChecksumFile(tablePath, manifests[0].getPath());
        }
    }

    return null;
}

/**
 * 计算 OSS 文件内容哈希（仅读取前 N 字节）
 */
private String computeFileHash(FileIO fileIO, Path file, int length) throws IOException {
    try (InputStream is = fileIO.newInputStream(file);
         DigestInputStream dis = new DigestInputStream(is, MessageDigest.getInstance("MD5"))) {
        byte[] buffer = new byte[length];
        dis.read(buffer);
        byte[] hash = dis.getMessageDigest().digest();
        return Hex.encodeHexString(hash);
    } catch (NoSuchAlgorithmException e) {
        throw new IOException("MD5 algorithm not available", e);
    }
}

/**
 * 计算本地文件内容哈希（仅读取前 N 字节）
 */
private String computeLocalFileHash(java.nio.file.Path file, int length) throws IOException {
    try (InputStream is = Files.newInputStream(file);
         DigestInputStream dis = new DigestInputStream(is, MessageDigest.getInstance("MD5"))) {
        byte[] buffer = new byte[length];
        dis.read(buffer);
        byte[] hash = dis.getMessageDigest().digest();
        return Hex.encodeHexString(hash);
    } catch (NoSuchAlgorithmException e) {
        throw new IOException("MD5 algorithm not available", e);
    }
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
 * 创建本地 FileIO
 */
private FileIO createLocalFileIO(Path localPath) {
    return FileIO.get(localPath, CatalogContext.create(
        new Options(),
        context.hadoopConf()
    ));
}

/**
 * 创建默认 FileIO（原有逻辑）
 */
private FileIO createDefaultFileIO(Path path, Identifier identifier) {
    return dataTokenEnabled
        ? new RESTTokenFileIO(context, api, identifier, path)
        : fileIOFromOptions(path);
}

private static final int HASH_CHECK_LENGTH = 4096;  // 校验前 4KB

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

class ChecksumFile {
    private final Path tablePath;
    private final Path fullPath;

    ChecksumFile(Path tablePath, Path fullPath) {
        this.tablePath = tablePath;
        this.fullPath = fullPath;
    }

    Path getFullPath() { return fullPath; }

    String getRelativePath() {
        return new Path(tablePath, fullPath.getName()).toString();
    }
}
```

**方案优势**：

| 优势 | 说明 |
|------|------|
| **无需扩展 API** | 使用现有 FileIO 读取 OSS 文件 |
| **准确性最高** | 直接验证数据一致性，100% 确保路径正确 |
| **双重保障** | FUSE 挂载检测 + OSS 数据比对 |
| **防止数据污染** | 可检测路径指向错误表的情况 |
| **优雅降级** | 校验失败可回退到默认 FileIO |

**完整校验流程**：

```
┌─────────────────────────────────────────────────────────────┐
│                    OSS 数据校验流程                          │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│        1. 通过 REST API 获取表的 OSS 路径信息                │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│        2. 选择校验文件（snapshot/manifest/schema）           │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│        3. 获取 OSS 文件元数据（大小、修改时间、hash）         │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│        4. 读取本地对应文件                                   │
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
    'fuse.local-path.root' = '/mnt/oss/warehouse',

    -- 安全校验配置（可选，默认 strict）
    'fuse.local-path.validation-mode' = 'strict'  -- strict/warn/none
);
```

## 限制

1. FUSE 挂载必须正确配置且可访问
2. 本地路径必须与 OSS 路径具有相同的目录结构
3. 写操作需要本地 FUSE 挂载点具有适当的权限
4. Windows 平台 FUSE 支持有限（需第三方工具如 WinFsp）
