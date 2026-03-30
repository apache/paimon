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
| `fuse.enabled` | Boolean | `false` | Whether to enable FUSE local path mapping |
| `fuse.root` | String | (none) | FUSE mounted local root path, e.g., `/mnt/fuse/warehouse` |
| `fuse.validation-mode` | String | `strict` | Validation mode: `strict`, `warn`, or `none` |

## Usage

```python
from pypaimon import CatalogFactory

catalog_options = {
    'metastore': 'rest',
    'uri': 'http://rest-server:8080',
    'warehouse': 'oss://my-catalog/',
    'token.provider': 'xxx',

    # FUSE local path configuration
    'fuse.enabled': 'true',
    'fuse.root': '/mnt/fuse/warehouse',
    'fuse.validation-mode': 'strict'
}

catalog = CatalogFactory.create(catalog_options)
```

## Validation Modes

Validation is performed on first data access to verify FUSE mount correctness. The `validation-mode` controls behavior when the local path does not exist:

| Mode | Behavior | Use Case |
|------|----------|----------|
| `strict` | Throw exception, block operation | Production, safety first |
| `warn` | Log warning, fallback to default FileIO | Testing, compatibility first |
| `none` | Skip validation, use directly | Trusted environment, performance first |

**Note**: Configuration errors (e.g., `fuse.enabled=true` but `fuse.root` not configured) will throw exceptions directly, regardless of validation mode.

## How It Works

1. When `fuse.enabled=true`, PyPaimon attempts to use local file access
2. On first data access, validation is triggered (unless mode is `none`)
3. Validation fetches the `default` database location and converts it to local path
4. If local path exists, subsequent data access uses `FuseLocalFileIO`
5. Path translation uses database/table logical names: remote path `oss://<catalog-id>/<db-id>/<table-id>` → local path `<root>/<db-name>/<table-name>`
6. If validation fails, behavior depends on `validation-mode`

## Example Scenario

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
    'fuse.enabled': 'true',
    'fuse.root': '/mnt/fuse/warehouse',
    'fuse.validation-mode': 'none'
})

# When reading table 'my_db.my_table', PyPaimon will:
# 1. Convert "oss://clg-paimon-xxx/db-xxx/tbl-xxx" to "/mnt/fuse/warehouse/my_db/my_table"
# 2. Use FuseLocalFileIO to read from local path
table = catalog.get_table('my_db.my_table')
reader = table.new_read_builder().new_read()
```

## Limitations

- Only catalog-level FUSE mount is supported (single `fuse.root` configuration)
- Validation only checks if local path exists, not data consistency
- If FUSE mount becomes unavailable after validation, file operations may fail
