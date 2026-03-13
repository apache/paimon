zai<!--
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

# FUSE Local Path Configuration for RESTCatalog

## Background

When using Paimon RESTCatalog with OSS (Object Storage Service), data access typically requires authentication tokens obtained from the REST server via `getTableToken` API. However, in scenarios where OSS paths are mounted locally via FUSE (Filesystem in Userspace), users can access data through local file system paths without needing OSS tokens.

This design introduces configuration parameters to support FUSE-mounted OSS paths, allowing users to specify local path mappings at catalog, database, and table levels.

## Goals

1. Enable local file system access for FUSE-mounted OSS paths
2. Support hierarchical path mapping: catalog root > database > table
3. Skip `getTableToken` API calls when FUSE local path is applicable
4. Maintain backward compatibility with existing RESTCatalog behavior

## Configuration Parameters

All parameters are defined in `RESTCatalogOptions.java`:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `fuse.local-path.enabled` | Boolean | `false` | Whether to enable FUSE local path mapping for OSS paths |
| `fuse.local-path.root` | String | (none) | The root local path for FUSE-mounted OSS, e.g., `/mnt/oss` |
| `fuse.local-path.database` | Map<String, String> | `{}` | Database-level local path mapping. Format: `db1:/local/path1,db2:/local/path2` |
| `fuse.local-path.table` | Map<String, String> | `{}` | Table-level local path mapping. Format: `db1.table1:/local/path1,db2.table2:/local/path2` |

## Usage Example

### SQL Configuration (Flink/Spark)

```sql
CREATE CATALOG paimon_rest_catalog WITH (
    'type' = 'paimon',
    'metastore' = 'rest',
    'uri' = 'http://rest-server:8080',
    'token' = 'xxx',
    
    -- FUSE local path configuration
    'fuse.local-path.enabled' = 'true',
    'fuse.local-path.root' = '/mnt/oss/warehouse',
    'fuse.local-path.database' = 'db1:/mnt/custom/db1,db2:/mnt/custom/db2',
    'fuse.local-path.table' = 'db1.table1:/mnt/special/t1'
);
```

### Path Resolution Priority

When resolving a path, the system checks in the following order (higher priority wins):

1. **Table-level mapping** (`fuse.local-path.table`)
2. **Database-level mapping** (`fuse.local-path.database`)
3. **Root mapping** (`fuse.local-path.root`)

Example: For table `db1.table1`:
- If `fuse.local-path.table` contains `db1.table1:/mnt/special/t1`, use `/mnt/special/t1`
- Else if `fuse.local-path.database` contains `db1:/mnt/custom/db1`, use `/mnt/custom/db1`
- Else use `fuse.local-path.root` (e.g., `/mnt/oss/warehouse`)

## Implementation

### RESTCatalog Modification

The `fileIOForData` method in `RESTCatalog.java` will be modified:

```java
private FileIO fileIOForData(Path path, Identifier identifier) {
    // If FUSE local path is enabled and path matches, use local FileIO
    if (fuseLocalPathEnabled) {
        Path localPath = resolveFUSELocalPath(path, identifier);
        if (localPath != null) {
            // Use local file IO, no token needed
            return FileIO.get(localPath, CatalogContext.create(new Options(), context.hadoopConf()));
        }
    }
    
    // Original logic: data token or ResolvingFileIO
    return dataTokenEnabled
            ? new RESTTokenFileIO(context, api, identifier, path)
            : fileIOFromOptions(path);
}

/**
 * Resolve FUSE local path. Priority: table > database > root.
 * @return Local path, or null if not applicable
 */
private Path resolveFUSELocalPath(Path originalPath, Identifier identifier) {
    String pathStr = originalPath.toString();
    
    // 1. Check table-level mapping
    Map<String, String> tableMappings = context.options().get(FUSE_LOCAL_PATH_TABLE);
    String tableKey = identifier.getDatabaseName() + "." + identifier.getTableName();
    if (tableMappings.containsKey(tableKey)) {
        String localRoot = tableMappings.get(tableKey);
        return convertToLocalPath(pathStr, localRoot);
    }
    
    // 2. Check database-level mapping
    Map<String, String> dbMappings = context.options().get(FUSE_LOCAL_PATH_DATABASE);
    if (dbMappings.containsKey(identifier.getDatabaseName())) {
        String localRoot = dbMappings.get(identifier.getDatabaseName());
        return convertToLocalPath(pathStr, localRoot);
    }
    
    // 3. Use root mapping
    String fuseRoot = context.options().get(FUSE_LOCAL_PATH_ROOT);
    if (fuseRoot != null) {
        return convertToLocalPath(pathStr, fuseRoot);
    }
    
    return null;
}

private Path convertToLocalPath(String originalPath, String localRoot) {
    // Convert OSS path to local FUSE path
    // Example: oss://bucket/warehouse/db1/table1 -> /mnt/oss/warehouse/db1/table1
    // Implementation depends on path structure
}
```

### Behavior Matrix

| Configuration | Path Match | Behavior |
|---------------|------------|----------|
| `fuse.local-path.enabled=true` | Yes | Local FileIO, **no `getTableToken` call** |
| `fuse.local-path.enabled=true` | No | Fallback to original logic |
| `fuse.local-path.enabled=false` | N/A | Original logic (data token or ResolvingFileIO) |

## Benefits

1. **Performance**: Local file system access is typically faster than network-based OSS access
2. **Cost Reduction**: No need to call `getTableToken` API, reducing REST server load
3. **Flexibility**: Supports different local paths for different databases/tables
4. **Backward Compatibility**: Disabled by default, existing behavior unchanged

## Limitations

1. FUSE mount must be properly configured and accessible
2. Local path must have the same directory structure as the OSS path
3. Write operations require proper permissions on the local FUSE mount
