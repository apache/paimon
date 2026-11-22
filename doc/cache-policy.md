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

# Cache policy

Cache policy routes object-storage operations through a cache endpoint or directly
to the origin endpoint. Java and PyPaimon use the same operation, file-type and
path selection rules. It is independent of the local block cache and does not
change table storage or read semantics.

REST Catalog can provide credentials, endpoints and cache settings in a table
token. Applications can override endpoint and cache settings in their catalog
configuration.

## Configuration

| Option | Default | Description |
| --- | --- | --- |
| `fs.oss.endpoint` | Supplied by the storage configuration or REST token | Endpoint used for operations selected by the cache policy. |
| `dlf.oss-endpoint` | Unset | REST Catalog client override for `fs.oss.endpoint`. A nonblank value has the highest endpoint priority. |
| `io-cache.origin.endpoint` | The effective `fs.oss.endpoint` | Origin endpoint for operations that bypass the cache. An unset, empty or whitespace-only value uses `fs.oss.endpoint`. |
| `io-cache.policy` | Unset; no operations use the cache | Comma-separated operation labels: `meta`, `read`, `write`, or `none`. |
| `io-cache.whitelist` | `*` | Comma-separated file types allowed to use the cache. |
| `io-cache.whitelist-path` | `*` | Comma-separated URI path substrings allowed to use the cache. |

### Client overrides

For REST token configuration, endpoint priority is:

1. A nonblank client `dlf.oss-endpoint`.
2. An explicitly configured client `fs.oss.endpoint`.
3. The token's `fs.oss.endpoint`.
4. The underlying storage configuration when no endpoint is provided above.

Every explicitly configured client `io-cache.*` option takes precedence over the
same option in the token, including `io-cache.origin.endpoint`, policy and both
whitelists. Unspecified options retain the token's values. Explicit empty values
are preserved: an empty policy or whitelist does not fall back to the token's
value.

These overrides apply to both FileIO operations and routing settings exposed to
native readers. They continue to apply after token refresh. Object-storage
credentials are supplied by the REST token.

Cache and origin settings have separate roles. Overriding `fs.oss.endpoint`
selects the endpoint for cache-eligible operations; it does not replace an
explicit `io-cache.origin.endpoint`.

After configuration is merged, if cache and origin use the same endpoint, Paimon
initializes a regular FileIO directly. No cache-policy wrapper or cache
connectivity probe is needed. This also applies when the origin endpoint is
omitted and therefore inherits the cache endpoint.

### Operation labels

| Label | Operations |
| --- | --- |
| `read` | File-content reads. |
| `write` | File-content writes supported by the cache endpoint. |
| `meta` | File attributes, existence, length and directory-status queries, including corresponding batch operations. |
| `none` | Disable all cache operations, even when combined with other labels. |

Use lowercase labels separated by commas. For example, `meta,read` enables
attribute queries and content reads. An unset or empty policy sends all operations
to origin. Enabling an operation does not bypass file-type, path or immutability
requirements.

The `meta` operation label and `meta` file type serve different purposes. With
`io-cache.policy=meta` and `io-cache.whitelist=data`, data-file attribute queries
can use the cache, while data-file content reads use origin.

### File types and paths

| File-type value | Files |
| --- | --- |
| `meta` | Immutable snapshot, schema, manifest, statistics and other table metadata files. |
| `global-index` | Table-level global index files. |
| `bucket-index` | Bucket-level index files. |
| `file-index` | Index files associated with individual data files. |
| `data` | Data files and files not assigned to another category. |
| `*` | All file types, subject to the path and immutability rules. |

PyPaimon also supports `structured-data` for its structured data-file category,
including Parquet, ORC, Avro, ROW and Mosaic. Use `data` for a shared
Java/Python configuration; it includes this category in PyPaimon.

Whitelist rules:

- Entries within one whitelist are alternatives. The file-type and path
  whitelists must both match.
- Path matching uses the URI path, excluding its scheme, bucket authority, query
  and fragment. For `oss://bucket/db/table/bucket-0/data.orc`, the matched path is
  `/db/table/bucket-0/data.orc`.
- Path entries are case-sensitive substrings, not glob patterns or regular
  expressions. A standalone `*` matches all paths; `bucket-*` is a literal
  substring.
- Leading and trailing whitespace around entries is ignored. An explicit empty
  whitelist, or a list containing only commas and whitespace, matches nothing.
  Unknown file types are ignored with a warning.

## Routing behavior

An operation uses the cache only when all of the following are true:

- Its operation label is enabled and the policy does not contain `none`.
- The file is immutable.
- Its file type matches `io-cache.whitelist`.
- Its URI path matches `io-cache.whitelist-path`.

Mutable files always use origin. Examples include snapshot pointers such as
`LATEST` and `EARLIEST`, success markers, consumer/service/tag state, Paimon
temporary files, and mutable Iceberg-compatible metadata such as
`version-hint.text` and `*.metadata.json`.

The following operations always use origin:

| Operation | Routing |
| --- | --- |
| Directory listing and recursive listing | Origin |
| Directory creation, deletion and rename | Origin |
| Atomic writes and two-phase or multipart upload/commit operations | Origin |
| Presigned URLs and archive/restore operations | Origin |

Cache and origin endpoints must refer to the same object-storage namespace and
accept the credentials used by the application. The cache endpoint must support
the operations enabled by its policy.

### Connection failures

When a selected cache operation first initializes its backend, Paimon checks
connectivity before returning a stream. A missing object is a valid probe result.

A recognized connection failure, such as connection refusal, DNS failure or
connection timeout, causes the operation to retry against origin. Paimon emits a
`WARN` log containing `CACHE_POLICY_FALLBACK`, the endpoints, path and full
exception. Subsequent operations on that FileIO instance use origin; a new
instance may try the cache again.

Missing files, existing files, permission or authentication failures, cancellation,
and errors that cannot be identified as connection failures do not trigger this
fallback.

Already-open streams are not transparently replayed. A write that fails after
consuming input or persisting partial data may still fail; applications must
handle such failures. When that failure is identified as a connection problem,
subsequent operations on the same FileIO bypass the cache.

## Examples

### Cache metadata reads

A REST token or explicit cache-policy configuration can select immutable metadata
reads and attribute queries:

```properties
fs.oss.endpoint=cache.example.com
io-cache.origin.endpoint=oss-region.example.com
io-cache.policy=meta,read
io-cache.whitelist=meta
```

Data-file reads and all writes use origin. Metadata reads use the cache; mutable
metadata and directory listings still use origin.

### Restrict caching to selected data paths

```properties
fs.oss.endpoint=cache.example.com
io-cache.origin.endpoint=oss-region.example.com
io-cache.policy=read
io-cache.whitelist=data,file-index
io-cache.whitelist-path=/db/table/bucket-0/,/db/table/bucket-1/
```

With table path `oss://bucket/db/table`, this caches data and file-index content
reads under the two listed buckets. Reads in other paths, attribute queries and
writes use origin.

### Override a server policy from the client

These catalog options override the corresponding token settings:

```properties
io-cache.policy=meta,read
io-cache.whitelist=meta
io-cache.whitelist-path=*
```

To disable cache routing, set this client catalog option:

```properties
io-cache.policy=none
```

Operations then use the effective origin endpoint.

To force every operation to a particular endpoint, set both endpoint roles to
the same value:

```properties
dlf.oss-endpoint=oss-region.example.com
io-cache.origin.endpoint=oss-region.example.com
```

The client endpoint overrides the token endpoint, and Paimon creates a regular
FileIO because the effective cache and origin endpoints are identical.

### Explicit FileIO construction

REST Catalog automatically applies cache policy when it constructs a
token-backed FileIO. Applications that create FileIO instances directly can use
`CachePolicyFileIO.create` in Java or PyPaimon and supply `fs.oss.endpoint`,
`io-cache.origin.endpoint` and the policy options above. A plain `FileIO.get`
call does not automatically enable endpoint routing.

## Legacy JindoCache configuration

Java's deprecated `io-cache.enabled` option controls the legacy JindoCache
integration only. It is not required to enable cache-policy endpoint routing.
Use `io-cache.policy=none` to disable endpoint routing.
