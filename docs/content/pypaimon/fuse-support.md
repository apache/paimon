---
title: "FUSE Support"
weight: 7
type: docs
aliases:
  - /pypaimon/fuse-support.html
---

# FUSE Support

When using PyPaimon REST Catalog to access remote object storage (such as OSS, S3, or HDFS), data access typically goes through remote storage SDKs. However, in scenarios where remote storage paths are mounted locally via FUSE (Filesystem in Userspace), users can access data directly through local filesystem paths for better performance.

This feature enables PyPaimon to use local file access when FUSE mount is available, bypassing remote storage SDKs.

## Configuration

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `fuse.local-path.enabled` | Boolean | `false` | Whether to enable FUSE local path mapping |
| `fuse.local-path.root` | String | (none) | FUSE mounted local root path, e.g., `/mnt/fuse/warehouse` |
| `fuse.local-path.mode` | String | `pvfs` | FUSE path mode: `pvfs` uses database/table logical names, `raw` uses URI path segments directly |
| `fuse.local-path.validation-mode` | String | `strict` | Validation mode: `strict`, `warn`, or `none` |

## Usage

```python
from pypaimon import CatalogFactory

# PVFS mode (default): uses database/table logical names
catalog_options = {
    'metastore': 'rest',
    'uri': 'http://rest-server:8080',
    'warehouse': 'oss://my-catalog/',
    'token.provider': 'xxx',

    # FUSE local path configuration
    'fuse.local-path.enabled': 'true',
    'fuse.local-path.root': '/mnt/fuse/warehouse',
    'fuse.local-path.mode': 'pvfs',
    'fuse.local-path.validation-mode': 'strict'
}

catalog = CatalogFactory.create(catalog_options)
```

## Path Modes

The `fuse.local-path.mode` option controls how remote storage paths are mapped to local FUSE paths:

| Mode | Path Mapping | Use Case |
|------|-------------|----------|
| `pvfs` | Uses database/table logical names from Identifier: `<root>/<db-name>/<table-name>` | PVFS catalog-level FUSE mount, where remote paths use UUIDs (e.g., `oss://<catalog-id>/<db-id>/<table-id>`) but FUSE exposes logical names |
| `raw` | Uses URI path segments directly: `<root>/<db-id>/<table-id>` | Standard FUSE mount where local directory structure mirrors remote storage layout |

## Validation Modes

Validation is performed on first data access to verify FUSE mount correctness. The `validation-mode` controls behavior when the local path does not exist:

| Mode | Behavior | Use Case |
|------|----------|----------|
| `strict` | Throw exception, block operation | Production, safety first |
| `warn` | Log warning, fallback to default FileIO | Testing, compatibility first |
| `none` | Skip validation, use directly | Trusted environment, performance first |

**Note**: Configuration errors (e.g., `fuse.local-path.enabled=true` but `fuse.local-path.root` not configured) will throw exceptions directly, regardless of validation mode.

## How It Works

1. When `fuse.local-path.enabled=true`, PyPaimon attempts to use local file access
2. On first data access, validation is triggered (unless mode is `none`)
3. Validation fetches the `default` database location and converts it to local path (always using raw URI parsing)
4. If local path exists, subsequent data access uses `FuseLocalFileIO`
5. Path translation depends on `fuse.local-path.mode`:
   - `pvfs`: remote path `oss://<catalog-id>/<db-id>/<table-id>` → local path `<root>/<db-name>/<table-name>`
   - `raw`: remote path `oss://<catalog-id>/<db-id>/<table-id>` → local path `<root>/<db-id>/<table-id>`
6. If validation fails, behavior depends on `validation-mode`

## Example Scenario

### PVFS Mode

Assume you have:
- Remote storage paths use UUIDs: `oss://clg-paimon-xxx/db-xxx/tbl-xxx`
- FUSE mount: `/mnt/fuse/warehouse` (mounted to `pvfs://demo_catalog`)
- FUSE exposes logical names: `/mnt/fuse/warehouse/my_db/my_table`

```python
from pypaimon import CatalogFactory

catalog = CatalogFactory.create({
    'metastore': 'rest',
    'uri': 'http://rest-server:8080',
    'warehouse': 'oss://my-catalog/',
    'fuse.local-path.enabled': 'true',
    'fuse.local-path.root': '/mnt/fuse/warehouse',
    'fuse.local-path.mode': 'pvfs',
    'fuse.local-path.validation-mode': 'none'
})

# When reading table 'my_db.my_table', PyPaimon will:
# 1. Convert "oss://clg-paimon-xxx/db-xxx/tbl-xxx" to "/mnt/fuse/warehouse/my_db/my_table"
# 2. Use FuseLocalFileIO to read from local path
table = catalog.get_table('my_db.my_table')
reader = table.new_read_builder().new_read()
```

### Raw Mode

Assume you have:
- Remote storage: `oss://my-catalog/`
- FUSE mount: `/mnt/fuse/warehouse` (mounted to `oss://my-catalog/`)
- Local directory structure mirrors remote: `/mnt/fuse/warehouse/db/table`

```python
from pypaimon import CatalogFactory

catalog = CatalogFactory.create({
    'metastore': 'rest',
    'uri': 'http://rest-server:8080',
    'warehouse': 'oss://my-catalog/',
    'fuse.local-path.enabled': 'true',
    'fuse.local-path.root': '/mnt/fuse/warehouse',
    'fuse.local-path.mode': 'raw'
})

# When reading table data, PyPaimon will:
# 1. Convert "oss://my-catalog/db/table" to "/mnt/fuse/warehouse/db/table"
# 2. Use FuseLocalFileIO to read from local path
table = catalog.get_table('db.table')
reader = table.new_read_builder().new_read()
```

## Limitations

- Only catalog-level FUSE mount is supported (single `fuse.local-path.root` configuration)
- Validation only checks if local path exists, not data consistency
- If FUSE mount becomes unavailable after validation, file operations may fail
