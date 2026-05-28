# HDFS End-to-End Tests

Verifies the native HDFS FileIO backend (`HdfsNativeFileIO`) against a live HDFS
cluster. No local Hadoop install or JVM required on the client side.

## Quick start (Docker)

```sh
# 1. Bring up a single-NameNode + single-DataNode cluster.
docker compose -f pypaimon/tests/e2e/hdfs/docker-compose.yml up -d

# Wait ~30s for the cluster to become healthy; check with:
docker compose -f pypaimon/tests/e2e/hdfs/docker-compose.yml ps

# 2. Install the package with the hdfs extra.
pip install -e '.[hdfs]'

# 3. Run the tests.
PYPAIMON_HDFS_E2E_URL=hdfs://localhost:8020 \
  python -m pytest pypaimon/tests/e2e/hdfs/ -v

# 4. Teardown.
docker compose -f pypaimon/tests/e2e/hdfs/docker-compose.yml down -v
```

## REST-catalog config delivery mode (no local xml)

The native backend accepts Hadoop key/values directly via catalog options.
Skip the `core-site.xml` / `hdfs-site.xml` dance entirely by configuring the
cluster wiring as options — exactly what a REST catalog would push to the
client in its response. Example:

```python
catalog = CatalogFactory.create({
    "warehouse": "viewfs://cluster/warehouse",
    "hdfs.client.impl": "native",
    # Forwarded as-is to the underlying client:
    "dfs.nameservices": "ns1,ns2",
    "dfs.ha.namenodes.ns1": "nn1,nn2",
    "dfs.namenode.rpc-address.ns1.nn1": "host-1:8020",
    "dfs.namenode.rpc-address.ns1.nn2": "host-2:8020",
    "fs.viewfs.mounttable.cluster.link./prod": "hdfs://ns1/prod",
})
```

Keys matching the prefixes `dfs.` / `fs.` / `hadoop.` / `ipc.` / `io.` are
forwarded automatically. Use the `hdfs.config.<key>` namespace for any other
key you want passed through.

## Kerberos

The cluster in `docker-compose.yml` runs without security to keep the
smoke test simple. For a Kerberized e2e: provision a krb5 + HDFS compose
separately, install `libgssapi-krb5-2` (or platform equivalent) on the
client, set `KRB5_CONFIG` and `KRB5CCNAME`, then either run `kinit`
yourself or pass `security.kerberos.login.principal` + `.keytab` as
catalog options (pypaimon will run `kinit` for you).
