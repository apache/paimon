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

