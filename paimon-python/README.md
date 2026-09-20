![Paimon](https://github.com/apache/paimon/blob/master/docs/static/paimon-simple.png)

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

# PyPaimon

This PyPi package contains the Python APIs for using Paimon.

# Version

Pypaimon requires Python 3.6+.

# Dependencies

The core dependencies are listed in `dev/requirements.txt`.
The development dependencies are listed in `dev/requirements-dev.txt`.

# OSS metadata commits

Install `pypaimon[oss]` (legacy PyArrow data access) or `pypaimon[jindo]`
(Jindo data access). Both include `oss2` for atomic metadata writes.
Configure `fs.oss.accessKeyId`, `fs.oss.accessKeySecret` and `fs.oss.endpoint`,
plus `fs.oss.securityToken` when using STS. An endpoint without a scheme uses
HTTPS for metadata writes. Credentials supplied only through an underlying
filesystem's credential provider must also be supplied through these options.

Metadata requests always use OSS Signature V4 for both AK and STS credentials,
independently of the Jindo data-access signer setting. Set `fs.oss.region` to the
bucket's region ID, such as `cn-hangzhou`. If unset, it is inferred from standard
`oss-<region>.aliyuncs.com` or `oss-<region>-internal.aliyuncs.com` endpoints.
Other endpoints, including acceleration endpoints, require an explicit region.

Atomic metadata PUTs also forward the OSS server-side encryption options, using
the same resolution as Java `OSSFileIO`:

| Option | Behavior |
| --- | --- |
| `fs.oss.server-side-encryption` | `AES256`, `KMS` or `SM4` (case-insensitive) |
| `fs.oss.server-side-encryption-key-id` | KMS key ID; implies `KMS` if the method is unset |
| `fs.oss.server-side-data-encryption` | `SM4` with `KMS`; implies `KMS` if the method is unset |
| `fs.oss.server-side-encryption-algorithm` | Legacy method fallback, used only if all three options above are unset |

The first three options reject blank values and invalid combinations before any
request is sent. If no encryption options are set, no encryption headers are
added and OSS applies the bucket's default policy. These settings cover the
atomic metadata PUT path; ordinary data writes still use the configured
PyArrow/Jindo filesystem and its encryption capabilities.

For `oss://` paths, `FileIO.get` selects `OssFileIO`, a thin `PyArrowFileIO`
subclass that overrides atomic creation. Filesystem initialization, path handling,
and ordinary PyArrow/Jindo file operations are inherited unchanged.
Use `FileIO.get(path, options)` or construct `OssFileIO` explicitly for OSS atomic
writes. REST token refresh and `ResolvingFileIO` also route atomic writes through
this implementation.

When the bucket is confirmed unversioned, `OssFileIO.try_to_write_atomic` uses a single OSS PUT
with `x-oss-forbid-overwrite=true`. Exactly one writer can create a given object;
`FileAlreadyExists` returns `False` so snapshot commits can retry. Other SDK errors
are raised as `OSError`, retaining their cause for diagnostics. A lost PUT response
is not retried by the SDK; the snapshot
commit loop checks the commit user and identifier before retrying.

The vendor SDK is an optional backend dependency, imported only for OSS atomic
writes. The common FileIO API and Paimon table format remain independent of it.
This implementation uses OSS-specific conditional creation; it does not provide
the same atomic-write capability for every object store.

Conditional creation requires a bucket that has **never enabled versioning**.
The first atomic write on each `OssFileIO` instance checks `GetBucketVersioning`
and caches the result, including the query-denied fallback. Concurrent first
writes may repeat the check and warning. Query errors other than `403 AccessDenied`
are not cached.
If versioning is Enabled/Suspended,
the state is unrecognized, or the query returns `403 AccessDenied`, the operation
logs a warning when caching the fallback and uses the inherited PyArrow/Jindo
temporary-file-and-rename path.
This preserves legacy writes without making version-query permission mandatory,
but the fallback does **not** guarantee safe concurrent commits. It also retains
the existing backend's encryption behavior rather than applying the conditional
PUT's OSS SSE headers. Invalid credentials, expired tokens, missing buckets, and
other query failures still propagate as errors.

Grant `oss:GetBucketVersioning` and keep versioning disabled to use conditional
creation. Keep bucket versioning and version-query permissions unchanged for the
instance's lifetime; recreate the FileIO after changing them. A configuration
change is not guaranteed to produce an error and can invalidate the conditional-write guarantee.

All concurrent writers must use conditional creation. Older Python clients or
other clients that overwrite snapshot objects can still overwrite a successful
commit. This change does not add conditional writes for other object stores.

# Build

You can build the source package by executing the following command:

```commandline
python3 setup.py sdist
```

The package is under `dist/`. Then you can install the package by executing the following command:

```commandline
pip3 install dist/*.tar.gz
```

The command will install the package and core dependencies to your local Python environment.

# Native scan planning

PyPaimon can plan splits with the optional `pypaimon-rust` package while retaining
the Python reader:

```python
native_table = table.copy({"scan.native-plan.enabled": "true"})
builder = native_table.new_read_builder()
plan = builder.new_scan().plan()
rows = builder.new_read().to_arrow(plan.splits())
explanation = builder.explain()
print(explanation.native_planned)
```

The adapter checks the installed binding's capabilities and falls back to the
Python planner for unsupported scans. New bindings preserve `plan.snapshot_id`
even when pruning removes every split. Native explain output includes snapshot
and split metadata; native pruning counters are not exposed.

To run both split planning and data-file reading in Rust, enable the independent
native-read option:

```python
native_table = table.copy({"read.native.enabled": "true"})
builder = native_table.new_read_builder().with_projection(["id", "name"])
plan = builder.new_scan().plan()
rows = builder.new_read().to_arrow(plan.splits())
```

Native reads return PyArrow batches through the Arrow C Data interface. They
currently require untouched splits produced by the native planner and top-level
projection. Query authorization, nested projection, row-kind output, and
explicit blob parallelism retain the Python reader. For materialized
`to_arrow()` reads, the effective split parallelism (the method argument,
`read.parallelism`, or the automatic default) runs independent Rust readers
over contiguous split groups. Streaming `to_arrow_batch_reader()` keeps one
lazy Rust reader. A missing reader capability, unsupported route, or
native-reader construction failure falls back to Python; I/O and data errors
raised after streaming starts surface to the caller.

With Rust main's `Table.from_resolved_schema()` binding, filesystem and JDBC catalog
tables preserve the Python table's resolved schema and complete effective
options. Stale table objects, historical schemas, and `copy()` overrides or
option removals no longer require catalog reloading or Python planning.
Tables opened with `FileStoreTable.from_path(path, file_io_options=None)` use
the same path with standard local, PyArrow or resolving FileIO. Storage options
configure FileIO; use `copy()` for table read options.
JDBC planning uses the resolved table location and storage properties without
opening another database connection.
REST tables use `Table.copy_with_resolved_schema()` to preserve the same schema
and option semantics, including branches whose schemas are catalog-managed.
The native table retains REST credentials, token refresh and catalog snapshot
resolution. Database and table names containing dots are passed as separate
identifier components. REST snapshot results (including empty results) take precedence over
filesystem snapshots. REST errors, including HTTP 501, are propagated as in Java.
Custom catalog/FileIO contexts still fall back when they cannot be reproduced.

Explicit row ranges on data-evolution tables require `ReadBuilder.with_row_ranges()`.
Watermark time travel requires Rust 0.4 or newer. Branch reads require the
branch-aware binding exposing `Table.branch()`, and the resolved branch is
checked before planning. Deletion-vector scans require `pypaimon-rust>=0.4.0`,
which includes schema-aware decoding of Python-written index manifests and
legacy bucket-index path compatibility. The reader honors explicit paths, then
bucket paths, and can read older Python files placed in `table/index`.
Bucket paths use the partition field types and `partition.legacy-name` to match
Java formatting, including timestamp precision and different JVM float spellings.
New Python writes honor `index-file-in-data-file-dir` and retain explicit paths
when Python and Java partition-directory formatting differs. Older releases
and prereleases before 0.4.0 use the Python planner for deletion vectors.
When using an unreleased 0.4.0 development wheel, rebuild it with these fixes;
package version checks cannot distinguish local builds with identical versions.

Append scans support `with_shard()` and `with_slice()` with Rust 0.4 or newer,
which preserves the file order needed for positional selection; primary-key scans support
bucket-based `with_shard()`. Data-evolution position selection requires the
binding's `TableScan.with_row_position_slice()` and `with_row_position_shard()`.
Selection occurs before reader filtering and deletion vectors, so surviving row
counts can differ between shards. Limits are applied after shard/slice selection.

Timestamp incremental scans require `ReadBuilder.new_incremental_scan()` and
stream-aware splits exposing `Split.is_streaming()`. Python resolves
`(start_timestamp, end_timestamp]` to snapshot IDs; Rust packs the selected APPEND
deltas into one plan. Like Java, readers retain physical change events, including
repeated primary keys and retracts across commits. They do not merge the window
into a final table state or apply endpoint deletion vectors or global indexes.
Other commit kinds are excluded; the ending snapshot still supplies plan metadata.
Rebuild development wheels from Rust main to obtain this contract.

`scan.version` supports tags, snapshot IDs and `watermark-<value>`, resolving tags
first and using the historical schema. Ordinary postpone-bucket batch scans can
use native planning and exclude pending files in negative buckets.

Dynamic and cross-partition primary-key buckets support native planning, including
bucket sharding. Cross-partition key migration is maintained by the writer's index.
Batch first-row scans follow Java and exclude un-compacted level-0 files; they can
use native planning. With deletion vectors, batch scans exclude level 0 unless
`deletion-vectors.merge-on-read=true`, in which case overlapping key ranges stay
together when they include L0 and require reader-side merging. Fully materialized
DV files across levels use raw splits, including first-row clustering tables.
First-row L0 runs can use native planning, including plans with materialized files
in separate raw splits. Plans that require merging clustered materialized files
still fall back to Python. Readers preserve physical row positions until deletion
vectors are applied, then evaluate residual predicates after merging.
Write scans and incremental scans retain level 0.

Append and data-evolution chunk shuffle use Rust file and deletion-vector planning.
Python retains live-row chunk sizing, seeded shuffle order and balanced worker
assignment, so the same seed selects the same chunks with either planner.
Projection does not remove aligned column files before chunk construction.
Chunk shuffle supports partition predicates, deletion vectors and timestamp
incremental scans; its existing restrictions on limits, slices, row ranges and
global-index results still apply.

Scored global-index results on data-evolution append tables use native row-range
planning; Python attaches scores to the selected ranges and reads the data.
Primary-key sorted indexes refine native batch splits through Python's existing
index reader, preserving merge-required splits and the selected snapshot.

Query authorization, first-row plans mixing L0 with merge-required materialized files,
and precomputed primary-key global-index results still use the Python planner.
Continuous streaming and write planning also retain their Python entrypoints.
Native planning remains optional and is disabled by default.

# Coalesced BLOB reads

FileIO merges nearby BLOB ranges before reading. Set
`file-io.read-coalesce.max-gap` and `file-io.read-coalesce.max-block` in the
catalog or connection options to tune the 1 MiB and 8 MiB defaults:

```python
import pypaimon.multimodal as pmm

connection = pmm.connect(options={
    "warehouse": "/tmp/warehouse",
    "file-io.read-coalesce.max-gap": "64 kb",
    "file-io.read-coalesce.max-block": "16 mb",
})
```

`max-block` constrains coalescing, but does not split an individual BLOB range.
A single read can therefore exceed this value.

# Load LeRobot Dataset v3

Install the optional dependency, then import a local directory, FileIO URI, or
Hugging Face repository:

```commandline
pip install 'pypaimon[lerobot]'
```

```python
import pypaimon.multimodal as pmm

connection = pmm.connect(options={"warehouse": "/tmp/warehouse"})
connection.load_from_lerobot(
    "robot_data",
    "/data/lerobot_dataset",
)
```

The source dataset must be non-empty. Its schema comes from `meta/info.json`.
Each frame becomes one row; media uses BLOB columns. The import creates frame,
Episode, task, info, and optional stats/subtask tables. Info and stats use
`key STRING, value STRING` rows, with each value JSON-encoded to preserve
nested metadata. Decode values with `json.loads`.

Before training, pause writes and create a shared tag:

```python
connection.create_lerobot_tag("robot_data", "train-2026-09-07")
frames = connection.get_table("robot_data").scan(
    tag_name="train-2026-09-07").to_arrow()
```

Read every metadata component with the same tag. Use the tag only after creation
succeeds; cross-table tagging is not atomic. Alternatively, pass `tag_name` to
`load_from_lerobot` to tag the imported snapshots immediately.

# HDF5 to multimodal tables

HDF5 loading requires Python 3.8 or newer. Install the optional dependency and
create the target multimodal table before loading local or remote HDF5 files as
one or more Arrow batches:

```commandline
pip install 'pypaimon[hdf5,vortex]'
```

```python
import pyarrow as pa
import pypaimon.multimodal as pmm

EMBEDDING_VECTOR_TYPE = pa.list_(pa.float32(), 3)
IMAGE_BLOB_TYPE = pa.large_binary()

schema = pa.schema([
    pa.field("episode_id", pa.string(), nullable=False),
    pa.field("frame_index", pa.int32(), nullable=False),
    # Arrow fixed-size lists map to Paimon VECTOR columns.
    pa.field("embedding", EMBEDDING_VECTOR_TYPE, nullable=False),
    # Arrow binary and large-binary values map to Paimon BLOB columns.
    pa.field("image", IMAGE_BLOB_TYPE),
])


def transform(h5, source):
    episode_id = source.stem
    for begin in range(0, len(h5["embedding"]), 128):
        end = min(begin + 128, len(h5["embedding"]))
        yield pa.RecordBatch.from_pydict({
            "episode_id": [episode_id] * (end - begin),
            "frame_index": list(range(begin, end)),
            "embedding": h5["embedding"][begin:end].tolist(),
            "image": [bytes(value) for value in h5["image"][begin:end]],
        }, schema=schema)

connection = pmm.connect(options={"warehouse": "/tmp/warehouse"})
frames = connection.create_table(
    "frames",
    schema=schema,
)
result = connection.load_from_hdf5(
    "frames", "/data/episodes", transform=transform)
print(result.file_count, result.batch_count, result.row_count, result.snapshot_id)
```

`load_from_hdf5` accepts one `.h5`/`.hdf5` file, an iterable of paths, or
directories that are searched recursively. Paths are resolved, duplicate
files within the call are removed, and the remaining files are processed in
sorted order. Every yielded batch must have exactly the target columns and be
safely convertible to the table schema; missing or extra columns, nulls for
non-nullable fields, incompatible types, and invalid fixed-size vector lengths
fail the call.

Remote `hdfs://`, `viewfs://`, `oss://`, `s3://`, and `gs://` sources use
PyPaimon's FileIO abstraction. Pass source-only credentials and endpoints via
`source_options={"fs.oss.endpoint": "...", ...}`; target warehouse FileIO
settings are deliberately not reused. h5py reads the seekable FileIO stream
directly without a local temporary download. Legacy OSS with PyArrow before 16
supports explicit files but requires Jindo or a newer PyArrow for recursive
directory discovery. In transforms, `source.local_path` returns a decoded
`Path` for local sources (including spaces and Unicode) and `None` for remote
sources.

An empty path iterable or an existing directory without HDF5 files returns
zero counts and `snapshot_id=None` without creating a writer or snapshot.
Nonexistent paths, unsupported file suffixes, and discovered files whose
transform produces no rows remain errors.

All files in one call use one writer and one commit, so success creates one
snapshot. The API is append-only: it does not add provenance columns, keep a
source ledger, skip files, or detect drift. Repeating the same call appends the
rows again. It is not retry-safe because an exception from the commit can have
an unknown result; inspect table state before deciding whether to retry.

# ROSBag to multimodal tables

ROSBag loading requires Python 3.10 or newer:

```commandline
pip install 'pypaimon[rosbag]'
```

Create the target table, then map ROS messages with a user transform:

```python
import pyarrow as pa


schema = pa.schema([
    pa.field("source", pa.string(), nullable=False),
    pa.field("timestamp", pa.int64(), nullable=False),
    pa.field("value", pa.string(), nullable=False),
])
connection.create_table("messages", schema=schema)


def transform(reader, source):
    rows = []
    for connection, timestamp, rawdata in reader.messages():
        message = reader.deserialize(rawdata, connection.msgtype)
        rows.append({
            "source": source.name,
            "timestamp": timestamp,
            "value": message.data,
        })
    return pa.Table.from_pylist(rows)

result = connection.load_from_rosbag(
    "messages",
    "s3://robot-data/recordings",
    transform=transform,
    source_options={"fs.s3.endpoint": "https://s3.example.com"},
)
```

ROS1 `.bag`, ROS2 SQLite3/MCAP directories, and standalone ROS2 `.mcap`
files are supported. OSS, S3, HDFS, ViewFS, and GCS URI sources use FileIO
and are copied in bounded chunks to a local temporary directory because
`rosbags` requires local paths. Standalone `.db3` files are rejected by
default; `allow_storage_fragment=True` imports the one SQLite fragment without
claiming that the complete recording is present.

Every source is scanned to EOF before its transform runs. Transform output is
strictly checked against the target Arrow schema and stored in a temporary
Arrow IPC file. Paimon writers are created only after every source passes, so
source, transform, and schema errors do not create Paimon data files. This
front-loaded validation reads each recording twice and requires temporary disk
space. A successful call commits all sources in one snapshot.

Ray uses the same validation contract. Install both extras and call
`pypaimon.ray.load_from_rosbag`; transformed output is fully materialized in
Ray before `write_paimon` starts:

```commandline
pip install 'pypaimon[ray,rosbag]'
```

# HDFS without a local Hadoop install

`pypaimon` supports HDFS through a pure-protocol client based on
[`hdfs-native`](https://github.com/Kimahriman/hdfs-native) (Rust + PyO3).
Use it when you want HDFS access **without** installing Hadoop, a JDK,
`libhdfs`, or wrestling with `CLASSPATH` / `LD_LIBRARY_PATH`.

Install with the optional extra:

```commandline
pip install 'pypaimon[hdfs]'
```

The native backend requires **Python 3.10+** (and is unavailable on Windows).
On older interpreters the extra is skipped, so `pypaimon` still installs — keep
using the legacy `pyarrow` (`libhdfs`/JVM) backend there via
`hdfs.client.impl=pyarrow`.

For `hdfs://` and `viewfs://` URIs this backend is now the default.
Switch back to the legacy `libhdfs` (JNI) path with:

```python
catalog = CatalogFactory.create({
    "warehouse": "hdfs://ns1/warehouse",
    "hdfs.client.impl": "pyarrow",   # default: "native"
})
```

## Sourcing the cluster wiring

The client still needs to know about NameNode addresses, HA failover
groups, and `viewfs` mount tables. Three options:

1. **Local xml** — set `HADOOP_CONF_DIR` (or the `hdfs.conf-dir` option)
   to a directory containing `core-site.xml` / `hdfs-site.xml`. Only the
   xml is required; no Hadoop binaries or JDK.

2. **Catalog options (REST-friendly)** — pass the original Hadoop
   key/values directly in catalog options. Keys with prefixes `dfs.`,
   `fs.`, `hadoop.`, `ipc.`, `io.` are forwarded as-is. A REST catalog
   can deliver these in its response, giving a fully zero-file client
   experience:

   ```python
   CatalogFactory.create({
       "warehouse": "viewfs://cluster/warehouse",
       "dfs.nameservices": "ns1",
       "dfs.ha.namenodes.ns1": "nn1,nn2",
       "dfs.namenode.rpc-address.ns1.nn1": "host-1:8020",
       "dfs.namenode.rpc-address.ns1.nn2": "host-2:8020",
       "fs.viewfs.mounttable.cluster.link./prod": "hdfs://ns1/prod",
   })
   ```

3. **Namespaced overrides** — use `hdfs.config.<key>` to forward any
   other Hadoop key not covered by the prefix whitelist.

The three sources can be combined; catalog options take precedence over
xml.

## Kerberos

A secured cluster still needs the GSSAPI system library
(`libgssapi-krb5-2` on Debian/Ubuntu, `krb5` via Homebrew on macOS,
`krb5-libs` on RHEL) plus a `krb5.conf`. Provide credentials by either:

- Running `kinit` yourself and pointing `KRB5CCNAME` at the cache, or
- Setting `security.kerberos.login.principal` and
  `security.kerberos.login.keytab` in catalog options — `pypaimon` will
  run `kinit` for you.

## Fallback behaviour

If the native backend fails to initialise (e.g. wheel missing on an
unsupported platform such as Windows), `pypaimon` automatically falls
back to the `pyarrow` (`libhdfs`/JVM) path and logs a warning. Disable
the fallback with `hdfs.client.fallback-to-pyarrow=false` if you want
hard failures instead.


# Vector index range reads

Native vector indexes (`ivf-flat`, `ivf-pq`, `ivf-sq`, `ivf-rq`, and `diskann`)
read multiple file ranges concurrently when the input stream supports
thread-safe positional reads. Set the table option `vindex.read.parallelism`
to a positive integer to control the maximum number of concurrent reads per
index reader, including reads from concurrent native query callbacks.

The default is **4** for remote index paths and **1** for local paths (including
`file://`). Setting it to **1** disables range-level concurrency. Streams that
only support `seek` and `read` remain serialized. Workers are created lazily
and released when the index reader closes; separate readers have separate
budgets. This option controls index I/O, not shard search or native compute
threads.


# Native vector index training

The native vector index writer submits training vectors in bounded batches.
`<index-type>.train.sample-ratio` (or its field-level override) still selects
the same evenly spaced non-null vectors in the same order. Native training
receives the final corpus size for automatic IVF sizing. This bounds Python
training buffers; native training and index construction have their own
memory requirements.


# Vector fallback scoring and refinement

Raw vector fallback and refinement score regular FLOAT vectors in bounded
blocks using NumPy. List, large-list and fixed-size-list Arrow arrays are
supported, including slices and multiple chunks. Null or unsupported blocks
use the scalar path. Candidate filters are applied before scoring.

L2 and cosine retain scalar accumulation order. Inner product retains Python
`sum` semantics, including its behavior on newer Python versions. Existing
Top-K tie-breaking rules are preserved. The same scoring path is used for raw and
refined primary-key vector results.
