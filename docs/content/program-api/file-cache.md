---
title: "Local Disk Cache"
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

# Local Disk Cache

When reading files from remote storage (S3, OSS, HDFS, etc.), each seek+read goes over the network. Paimon provides a block-level local disk cache that transparently caches file reads on local disk, significantly reducing remote I/O for repeated access patterns.

## Cached File Types

The cache classifies files by type. By default, only `meta` and `global-index` types are cached. You can customize this via the `file-cache.whitelist` option.

| File Type | Config Name | Examples | Default Cached |
|-----------|-------------|----------|----------------|
| META | meta | snapshot, schema, manifest, statistics, tag | Yes |
| GLOBAL_INDEX | global-index | BTree, Lumina, Tantivy index files | Yes |
| BUCKET_INDEX | bucket-index | Hash, deletion vector index files | No |
| DATA | data | Data files (ORC, Parquet, etc.) | No |
| FILE_INDEX | file-index | Data-file level bloom filter, bitmap | No |

All file types can be added to the whitelist. The default whitelist is `meta,global-index`.

## Enable Cache

Use `table.copy()` to pass cache options as dynamic parameters:

{{< tabs "enable-cache" >}}

{{< tab "Java" >}}

```java
import org.apache.paimon.table.Table;

import java.util.HashMap;
import java.util.Map;

Table table = catalog.getTable(Identifier.create("my_db", "my_table"));

Map<String, String> options = new HashMap<>();
options.put("file-cache.enabled", "true");
// optional: customize cache directory and limits
options.put("file-cache.dir", "/tmp/paimon-file-cache");
options.put("file-cache.max-size", "2gb");
options.put("file-cache.block-size", "1mb");

// All subsequent reads on this table instance will use the cache
table = table.copy(options);
```

{{< /tab >}}

{{< tab "Python" >}}

```python
table = catalog.get_table("db.my_table")

# Enable cache with dynamic options
table = table.copy({
    "file-cache.enabled": "true",
    # optional: customize cache directory and limits
    "file-cache.dir": "/tmp/paimon-file-cache",
    "file-cache.max-size": "2gb",
    "file-cache.block-size": "1mb",
})

# All subsequent reads on this table instance will use the cache
```

{{< /tab >}}

{{< /tabs >}}

## Cache Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `file-cache.enabled` | Boolean | false | Whether to enable local disk block cache. |
| `file-cache.dir` | String | `<tmpdir>/paimon-file-cache` | Directory for storing cached blocks. |
| `file-cache.max-size` | MemorySize | unlimited | Maximum total size of the cache. When exceeded, the least recently used blocks are evicted. |
| `file-cache.block-size` | MemorySize | 1 mb | Block size for caching. Files are logically divided into fixed-size blocks and cached independently. |
| `file-cache.whitelist` | String | meta,global-index | Comma-separated list of file types to cache. Supported values: `meta`, `global-index`, `bucket-index`, `data`, `file-index`. |

## How It Works

- Files are logically divided into fixed-size blocks (default 1 MB).
- On the first read, blocks are downloaded from remote storage and saved to local disk.
- Subsequent reads of the same block are served from local disk, skipping remote I/O.
- Cache files are keyed by remote file path and block offset, so they persist across process restarts and can be reused.
- When the cache exceeds `max-size`, the least recently used blocks are evicted automatically.
