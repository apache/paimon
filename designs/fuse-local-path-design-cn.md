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

## 限制

1. FUSE 挂载必须正确配置且可访问
2. 本地路径必须与 OSS 路径具有相同的目录结构
3. 写操作需要本地 FUSE 挂载点具有适当的权限
