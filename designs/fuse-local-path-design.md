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

## Security Validation Mechanism

### Problem Scenarios

Incorrect FUSE local path configuration can lead to serious data consistency issues:

| Scenario | Description | Consequence |
|----------|-------------|-------------|
| **Local path not mounted** | User's configured `/local/table` is not actually FUSE-mounted | Data is written only to local disk, not synced to OSS, causing data loss |
| **OSS path mismatch** | Local path points to a different table's OSS path | Data is written to the wrong table, causing data pollution |

### Validation Scheme

#### 1. Path Consistency Validation (Strong Validation)

Validate consistency between local path and OSS path when first accessing a table:

```java
/**
 * Validate consistency between FUSE local path and OSS path
 * @throws IllegalArgumentException if paths are inconsistent
 */
private void validateFUSEPath(Path localPath, Path ossPath, Identifier identifier) {
    // 1. Check if local path exists and is a FUSE mount point
    if (!isFUSEMountPoint(localPath)) {
        throw new IllegalArgumentException(
            String.format("FUSE local path '%s' is not a valid FUSE mount point. " +
                "Data would be written to local disk instead of OSS!", localPath));
    }

    // 2. Validate path identifier consistency: read .paimon table identifier file
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
 * Check if path is a FUSE mount point
 * Can be determined by checking /proc/mounts (Linux) or using stat system call
 */
private boolean isFUSEMountPoint(Path path) {
    // Option 1: Check /proc/mounts for FUSE mount of this path
    // Option 2: Check if filesystem type is fuse.*
    // Option 3: Read /etc/mtab or use jnr-posix library
    return checkFUSEMount(path);
}
```

#### 2. Table Identifier File Mechanism

When creating a table, automatically generate a `.paimon-identifier` file in the table directory:

```
/mnt/oss/warehouse/db1/table1/
├── .paimon-identifier    # Content: "db1.table1"
├── data-xxx.parquet
├── manifest-xxx
└── snapshot-xxx
```

Identifier file content:
```
database=db1
table=table1
table-uuid=xxx-xxx-xxx
created-at=2026-03-13T00:00:00Z
```

#### 3. Validation Mode Configuration

New configuration parameter to control validation behavior:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `fuse.local-path.validation-mode` | String | `strict` | Validation mode: `strict`, `warn`, or `none` |

**Validation Mode Description**:

| Mode | Behavior |
|------|----------|
| `strict` | Enable validation, throw exception on failure, block the operation |
| `warn` | Enable validation, log warning on failure, but allow operation to proceed |
| `none` | No validation (not recommended, may cause data loss or pollution) |

### Validation Flow

```
┌─────────────────────────────────────────────────────────────┐
│                     Access Table (getTable)                  │
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
        │ Resolve local path│    │ Use original logic│
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
        │ Validate FUSE     │    │ Skip validation   │
        │ mount point       │    │ Use local path    │
        │ Validate path     │    │ directly          │
        │ consistency       │    │                   │
        └───────────────────┘    └───────────────────┘
                    │
                    ▼
        ┌───────────────────────────────────────┐
        │ Validation passed ?                    │
        └───────────────────────────────────────┘
           │                    │
          Yes                   No
           │                    │
           ▼                    ▼
    ┌─────────────┐    ┌─────────────────────┐
    │ Use local   │    │ validation-mode:     │
    │ path        │    │  - strict: throw     │
    │ LocalFileIO │    │  - warn: warn+fallback│
    └─────────────┘    └─────────────────────┘
```

### Security Validation Implementation

Use OSS data validation to verify FUSE path correctness: read OSS files via existing FileIO and compare with local files.

**Complete Implementation**:

```java
/**
 * Complete implementation of fileIOForData in RESTCatalog
 * Combining FUSE local path validation with OSS data validation
 */
private FileIO fileIOForData(Path path, Identifier identifier) {
    // If FUSE local path is enabled, try using local path
    if (fuseLocalPathEnabled) {
        Path localPath = resolveFUSELocalPath(path, identifier);
        if (localPath != null) {
            // Execute validation based on validation mode
            ValidationMode mode = getValidationMode();

            if (mode != ValidationMode.NONE) {
                ValidationResult result = validateFUSEPath(localPath, path, identifier);
                if (!result.isValid()) {
                    handleValidationError(result, mode);
                    // Validation failed, fallback to default logic
                    return createDefaultFileIO(path, identifier);
                }
            }

            // Validation passed or skipped, use local FileIO
            return createLocalFileIO(localPath);
        }
    }

    // Original logic: data token or ResolvingFileIO
    return createDefaultFileIO(path, identifier);
}

/**
 * Validate FUSE local path
 */
private ValidationResult validateFUSEPath(Path localPath, Path ossPath, Identifier identifier) {
    // 1. Create LocalFileIO for local path
    LocalFileIO localFileIO = LocalFileIO.create();

    // 2. Check if local path exists
    if (!localFileIO.exists(localPath)) {
        return ValidationResult.fail("Local path does not exist: " + localPath);
    }

    // 3. OSS data validation
    return validateByOSSData(localFileIO, localPath, ossPath, identifier);
}

/**
 * Validate FUSE path correctness by comparing OSS and local file data
 * Uses existing FileIO (RESTTokenFileIO or ResolvingFileIO) to read OSS files
 */
private ValidationResult validateByOSSData(
        LocalFileIO localFileIO, Path localPath, Path ossPath, Identifier identifier) {
    try {
        // 1. Get OSS FileIO (using existing logic, can access OSS)
        FileIO ossFileIO = createDefaultFileIO(ossPath, identifier);

        // 2. Get latest snapshot via SnapshotManager
        SnapshotManager snapshotManager = new SnapshotManager(ossFileIO, ossPath);
        Snapshot latestSnapshot = snapshotManager.latestSnapshot();

        Path checksumFile;
        if (latestSnapshot != null) {
            // Has snapshot, use snapshot file for validation
            checksumFile = snapshotManager.snapshotPath(latestSnapshot.id());
        } else {
            // No snapshot (new table), use schema file for validation
            SchemaManager schemaManager = new SchemaManager(ossFileIO, ossPath);
            Optional<TableSchema> latestSchema = schemaManager.latest();
            if (!latestSchema.isPresent()) {
                // Table is completely empty (no schema, shouldn't happen theoretically)
                LOG.info("No snapshot or schema found for table: {}, skip validation", identifier);
                return ValidationResult.success();
            }
            checksumFile = schemaManager.toSchemaPath(latestSchema.get().id());
        }

        // 3. Read OSS file content and compute hash
        FileStatus ossStatus = ossFileIO.getFileStatus(checksumFile);
        String ossHash = computeFileHash(ossFileIO, checksumFile);

        // 4. Build local file path and compute hash
        Path localChecksumFile = new Path(localPath, ossPath.toUri().getPath());

        if (!localFileIO.exists(localChecksumFile)) {
            return ValidationResult.fail(
                "Local file not found: " + localChecksumFile +
                ". The FUSE path may not be mounted correctly.");
        }

        long localSize = localFileIO.getFileSize(localChecksumFile);
        String localHash = computeFileHash(localFileIO, localChecksumFile);

        // 5. Compare file features
        if (localSize != ossStatus.getLen()) {
            return ValidationResult.fail(String.format(
                "File size mismatch! Local: %d bytes, OSS: %d bytes.",
                localSize, ossStatus.getLen()));
        }

        if (!localHash.equalsIgnoreCase(ossHash)) {
            return ValidationResult.fail(String.format(
                "File content hash mismatch! Local: %s, OSS: %s.",
                localHash, ossHash));
        }

        return ValidationResult.success();

    } catch (Exception e) {
        LOG.warn("Failed to validate FUSE path by OSS data for: {}", identifier, e);
        return ValidationResult.fail("OSS data validation failed: " + e.getMessage());
    }
}

/**
 * Compute file content hash using FileIO
 */
private String computeFileHash(FileIO fileIO, Path file) throws IOException {
    MessageDigest md;
    try {
        md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
        throw new IOException("MD5 algorithm not available", e);
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
 * Handle validation error
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
            // Won't reach here
            break;
    }
}

/**
 * Create local FileIO
 */
private FileIO createLocalFileIO(Path localPath) {
    return FileIO.get(localPath, CatalogContext.create(
        new Options(),
        context.hadoopConf()
    ));
}

/**
 * Create default FileIO (original logic)
 */
private FileIO createDefaultFileIO(Path path, Identifier identifier) {
    return dataTokenEnabled
        ? new RESTTokenFileIO(context, api, identifier, path)
        : fileIOFromOptions(path);
}

// ========== Helper Classes ==========

enum ValidationMode {
    STRICT,  // Strict mode: throw exception on validation failure
    WARN,    // Warn mode: log warning on failure, fallback to default logic
    NONE     // No validation
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

**Advantages**:

| Advantage | Description |
|-----------|-------------|
| **No API Extension Needed** | Uses existing FileIO and SnapshotManager/SchemaManager |
| **Uses LATEST snapshot** | Gets via `SnapshotManager.latestSnapshot()`, no traversal needed |
| **New Table Support** | Falls back to schema file validation when no snapshot |
| **Most Accurate** | Directly validates data consistency, ensures path correctness |
| **Graceful Degradation** | Validation failure falls back to default FileIO |

**Validation File Selection Logic**:

| Scenario | Validation File |
|----------|-----------------|
| Has snapshot | Latest snapshot file via `SnapshotManager.latestSnapshot()` |
| No snapshot (new table) | Latest schema file via `SchemaManager.latest()` |
| No schema (theoretically impossible) | Skip validation |

**Complete Validation Flow**:

```
┌─────────────────────────────────────────────────────────────┐
│                  OSS Data Validation Flow                   │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│     1. Get OSS FileIO (RESTTokenFileIO or ResolvingFileIO)  │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│     2. Get latest snapshot via SnapshotManager              │
│        snapshotManager.latestSnapshot()                     │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌───────────────────────────────────────┐
        │ Snapshot exists ?                     │
        └───────────────────────────────────────┘
           │                    │
          Yes                   No
           │                    │
           ▼                    ▼
┌─────────────────────┐  ┌─────────────────────────────────────┐
│ Use snapshot file   │  │ Get latest schema via SchemaManager │
│ for validation      │  │ schemaManager.latest()              │
└─────────────────────┘  └─────────────────────────────────────┘
                              │
                              ▼
        ┌───────────────────────────────────────┐
        │ Schema exists ?                       │
        └───────────────────────────────────────┘
           │                    │
          Yes                   No → Skip validation (empty table)
           │
           ▼
┌─────────────────────────────────────────────────────────────┐
│     3. Get OSS file metadata (size)                         │
│        Compute OSS file hash                                │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────┐
│     4. Read local corresponding file                        │
└─────────────────────────────────────────────────────────────┘
                              │
                              ▼
        ┌───────────────────────────────────────┐
        │ Local file exists ?                   │
        └───────────────────────────────────────┘
           │                    │
          Yes                   No → Validation failed (wrong path or not mounted)
           │
           ▼
        ┌───────────────────────────────────────┐
        │ File size matches ?                   │
        └───────────────────────────────────────┘
           │                    │
          Yes                   No → Validation failed
           │
           ▼
        ┌───────────────────────────────────────┐
        │ File content hash matches ?           │
        └───────────────────────────────────────┘
           │                    │
          Yes                   No → Validation failed (path points to wrong table)
           │
           ▼
    ┌─────────────┐
    │ Validation  │
    │ passed      │
    │ Safe to use │
    └─────────────┘
```

### Usage Example (with Security Validation)

```sql
CREATE CATALOG paimon_rest_catalog WITH (
    'type' = 'paimon',
    'metastore' = 'rest',
    'uri' = 'http://rest-server:8080',
    'token' = 'xxx',

    -- FUSE local path configuration
    'fuse.local-path.enabled' = 'true',
    'fuse.local-path.root' = '/mnt/oss/warehouse',

    -- Security validation configuration (optional, default: strict)
    'fuse.local-path.validation-mode' = 'strict'  -- strict/warn/none
);
```

## Limitations

1. FUSE mount must be properly configured and accessible
2. Local path must have the same directory structure as the OSS path
3. Write operations require proper permissions on the local FUSE mount
4. Windows platform has limited FUSE support (requires third-party tools like WinFsp)

