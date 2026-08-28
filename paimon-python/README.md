![Paimon](https://github.com/apache/paimon/blob/master/docs/static/paimon-simple.png)

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

# PyPaimon

This PyPi package contains the Python APIs for using Paimon.

# Version

Pypaimon requires Python 3.6+.

# Dependencies

The core dependencies are listed in `dev/requirements.txt`.
The development dependencies are listed in `dev/requirements-dev.txt`.

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

# LeRobot Dataset v3 to multimodal tables

Install the optional dependency, then import a local, FileIO URI, or Hugging
Face v3 dataset once. The target table is created from `meta/info.json` when
absent; later calls append.

```commandline
pip install 'pypaimon[lerobot]'
```

```python
import pypaimon.multimodal as pmm

connection = pmm.connect(options={"warehouse": "/tmp/warehouse"})
result = connection.load_from_lerobot(
    "robot_data",
    "/data/lerobot_dataset",
)
print(result.row_count, result.snapshot_id)
```

Each LeRobot frame becomes one Paimon row. Numeric vectors remain vectors,
images use their existing compressed bytes, and video frames are decoded once
and stored as per-frame PNG BLOBs; the source MP4 is never copied into every
row.

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
