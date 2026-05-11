---
title: "Local Cache"
weight: 8
type: docs
aliases:
- /program-api/file-cache.html
- /pypaimon/file-cache.html
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Local Cache

When reading files from remote storage (S3, OSS, HDFS, etc.), each seek+read goes over the network. Paimon provides a block-level local cache that transparently caches file reads, significantly reducing remote I/O for repeated access patterns.

The cache supports two modes:
- **Disk cache**: when `local-cache.dir` is configured, blocks are cached on local disk.
- **Memory cache**: when `local-cache.dir` is not configured, blocks are cached in memory.

## Cached File Types

The cache classifies files by type. By default, only `meta` and `global-index` types are cached. You can customize this via the `local-cache.whitelist` option.

| File Type | Config Name | Examples | Default Cached |
|-----------|-------------|----------|----------------|
| META | meta | snapshot, schema, manifest, statistics, tag | Yes |
| GLOBAL_INDEX | global-index | BTree, Lumina, Tantivy index files | Yes |
| BUCKET_INDEX | bucket-index | Hash, deletion vector index files | No |
| DATA | data | Data files (ORC, Parquet, etc.) | No |
| FILE_INDEX | file-index | Data-file level bloom filter, bitmap | No |

All file types can be added to the whitelist. The default whitelist is `meta,global-index`.

## Enable Cache

This is a catalog-level option. Configure it when creating the catalog:

{{< tabs "enable-cache" >}}

{{< tab "Java" >}}

```java
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.Options;

Options options = new Options();
options.set("warehouse", "s3://my-bucket/warehouse");
options.set("local-cache.enabled", "true");
// optional: use disk cache by specifying a directory
options.set("local-cache.dir", "/tmp/paimon-cache");
// optional: customize limits
options.set("local-cache.max-size", "2gb");
options.set("local-cache.block-size", "1mb");

CatalogContext context = CatalogContext.create(options);
Catalog catalog = CatalogFactory.createCatalog(context);

// All tables from this catalog will use the cache
Table table = catalog.getTable(Identifier.create("my_db", "my_table"));
```

{{< /tab >}}

{{< tab "Python" >}}

```python
import pypaimon

options = {
    "warehouse": "s3://my-bucket/warehouse",
    "local-cache.enabled": "true",
    # optional: use disk cache by specifying a directory
    "local-cache.dir": "/tmp/paimon-cache",
    # optional: customize limits
    "local-cache.max-size": "2gb",
    "local-cache.block-size": "1mb",
}

catalog = pypaimon.create_catalog(options)

# All tables from this catalog will use the cache
table = catalog.get_table("db.my_table")
```

{{< /tab >}}

{{< /tabs >}}

## Cache Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `local-cache.enabled` | Boolean | false | Whether to enable local block cache for file reads. |
| `local-cache.dir` | String | (none) | Directory for storing cached blocks on disk. If not configured, memory cache is used. |
| `local-cache.max-size` | MemorySize | unlimited | Maximum total size of the cache. When exceeded, the least recently used blocks are evicted. |
| `local-cache.block-size` | MemorySize | 1 mb | Block size for caching. Files are logically divided into fixed-size blocks and cached independently. |
| `local-cache.whitelist` | String | meta,global-index | Comma-separated list of file types to cache. Supported values: `meta`, `global-index`, `bucket-index`, `data`, `file-index`. |

## How It Works

- Files are logically divided into fixed-size blocks (default 1 MB).
- On the first read, blocks are downloaded from remote storage and cached locally (on disk or in memory).
- Subsequent reads of the same block are served from the local cache, skipping remote I/O.
- When using disk cache, cache files are keyed by remote file path and block offset, so they persist across process restarts and can be reused.
- When the cache exceeds `max-size`, the least recently used blocks are evicted automatically.

## Cache Lifecycle

The cache is created and managed by the Catalog. All tables obtained from the same catalog share a single cache instance. The cache lives as long as the Catalog object is reachable — no explicit close is needed.

In distributed computing frameworks (Flink, Spark), the `FileIO` is serialized and shipped to task managers. After deserialization, the cache is **not** recreated — reads fall through directly to the remote storage. This is by design: the cache lifecycle is bound to the Catalog that created it, and a deserialized `FileIO` is no longer managed by any Catalog.

If you need caching on task managers, create a new Catalog with cache options enabled on each worker node.
