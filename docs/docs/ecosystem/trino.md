---
title: "Trino"
sidebar_position: 5
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

# Trino

Use the separately distributed Paimon connector to query tables from Trino. The examples on this
page describe the **Trino 440 connector**. For other versions, follow the matching revision of
[Apache Paimon Trino](https://github.com/apache/paimon-trino).

## Version

Match the Trino server version, connector artifact, and Java runtime. The connector repository
has its own release cycle and Paimon dependency; its default branch can target a different Trino
and Java version from the examples here.

The write examples below require a connector with write support and a supported table layout.
They do not imply that every historical Trino connector supports these operations.

## Installation {#preparing-paimon-jar-file}

### Obtain the Plugin

Use the [download page](../project/download#engine-jars) to locate the Trino 440 plugin archive.
For other versions, use the [connector repository](https://github.com/apache/paimon-trino).
The plugin is a distribution containing its dependencies, rather than a single jar to place on
the general Trino classpath.

To build from source, check out a connector revision matching the Trino server and use the Java
version required by that revision's `pom.xml`. Run its documented build; for revisions using the
Maven build, the command is:

```bash
mvn clean install -DskipTests
```

Locate the plugin archive in that revision's build output. Artifact names and module paths
vary across revisions. Use the connector artifact's actual version rather than substituting
the Paimon version shown by this documentation.

### Install Paimon Connector

For a Trino 440 distribution named
`paimon-trino-440-<connector-version>-plugin.tar.gz`, extract it under the Trino plugin directory
on the coordinator and every worker:

```bash
tar -zxf paimon-trino-440-<connector-version>-plugin.tar.gz -C "${TRINO_HOME}/plugin"
```

For the Trino 440 connector running on JDK 21, include these entries in `etc/jvm.config`:

```text
--add-opens=java.base/sun.nio.ch=ALL-UNNAMED
--add-opens=java.base/java.nio=ALL-UNNAMED
```

Restart Trino after installing the plugin and completing the catalog configuration.

## Configure Paimon Catalog

Create `etc/catalog/paimon.properties` on the Trino nodes. The file name registers the SQL
catalog as `paimon`:

```properties
connector.name=paimon
warehouse=hdfs://namenode:8020/warehouse/paimon
```

This example uses a filesystem catalog and assumes HDFS access is configured. For a local
single-node experiment, `warehouse=file:/tmp/warehouse` is sufficient. Use shared storage for
a distributed deployment.

### Filesystem

The Trino 440 connector uses Trino's filesystem integration. Configure storage access before
querying a table. For HDFS, provide `HADOOP_HOME`, `HADOOP_CONF_DIR`, or the connector's
`hadoop-conf-dir` property as appropriate for the deployment.

For Hadoop-backed object storage, supply the filesystem configuration through:

```properties
hive.config.resources=/path/to/core-site.xml
```

Make the configuration file and required filesystem libraries available on all nodes that access
the warehouse. Follow the filesystem instructions for your **Trino and connector versions**;
properties from newer Trino releases may differ.

Connector revisions using `io.trino.hadoop:hadoop-apache` can override that dependency when a
specific Hadoop distribution is required. For example, if supported by the selected revision:

```bash
mvn clean install -DskipTests -Dhadoop.apache.version=3.3.5-1
```

### Kerberos

For the Trino 440 connector's Kerberos login, set these catalog properties:

```properties
security.kerberos.login.principal=hadoop-user@EXAMPLE.COM
security.kerberos.login.keytab=/etc/trino/hdfs.keytab
```

Distribute the keytab to every Trino node that needs it and configure access for the service user.

### Temporary Directory {#tmp-dir}

Paimon extracts jars for code generation into the JVM temporary directory. Choose a writable
location that is not removed by periodic cleanup while Trino is running. Set this JVM option
in `etc/jvm.config` on each node:

```text
-Djava.io.tmpdir=/path/to/trino-tmp
```

## Create Schema

The following examples form one sequence in the `paimon` catalog:

```sql
CREATE SCHEMA paimon.test_db;
```

## Create Table

Create a primary-key table with a fixed bucket count. Include the partition column in the
primary key:

```sql
CREATE TABLE paimon.test_db.orders (
    order_key bigint,
    order_status varchar,
    total_price decimal(18,4),
    order_date date
)
WITH (
    file_format = 'ORC',
    primary_key = ARRAY['order_key', 'order_date'],
    partitioned_by = ARRAY['order_date'],
    bucket = '2',
    bucket_key = 'order_key',
    changelog_producer = 'input'
);
```

## Insert

The Trino 440 connector supports inserts into primary-key tables with fixed buckets and
non-primary-key tables with `bucket = -1`.

```sql
INSERT INTO paimon.test_db.orders
    (order_key, order_status, total_price, order_date)
VALUES (1, 'NEW', DECIMAL '19.9900', DATE '2024-01-01');
```

## Query

```sql
SELECT * FROM paimon.test_db.orders;
```

## Add Column

Continue with the existing table; there is no need to create it again:

```sql
ALTER TABLE paimon.test_db.orders ADD COLUMN shipping_address varchar;
```

## Query with Time Traveling

Use a retained snapshot or tag. The timestamp and identifiers below are examples; replace them
with values from the table's history.

```sql
-- Select the snapshot at the specified timestamp.
SELECT * FROM paimon.test_db.orders
FOR TIMESTAMP AS OF TIMESTAMP '2024-01-01 00:00:00 Asia/Shanghai';

-- Select snapshot 1 if it is still retained.
SELECT * FROM paimon.test_db.orders FOR VERSION AS OF 1;

-- Select an existing tag.
SELECT * FROM paimon.test_db.orders FOR VERSION AS OF 'my-tag';
```

:::warning Numeric tag names

A numeric tag name takes precedence over a matching snapshot ID when supplied as a string.
If tag `'1'` points to snapshot 2, `FOR VERSION AS OF '1'` reads snapshot 2. Prefer descriptive
tag names to avoid ambiguity.

:::

See [Snapshot Management](../maintenance/manage-snapshots) and [Tags](../maintenance/manage-tags)
for retention and tag creation.

## Type Mapping {#trino-to-paimon-type-mapping}

Common read mappings for the connector are summarized below using SQL type names. Check the
matching connector revision for precision limits and unsupported types.

| Paimon type | Trino type |
| --- | --- |
| `BOOLEAN` | `BOOLEAN` |
| `TINYINT`, `SMALLINT`, `INT`, `BIGINT` | `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT` |
| `FLOAT`, `DOUBLE` | `REAL`, `DOUBLE` |
| `DECIMAL(p, s)` | `DECIMAL(p, s)` |
| `CHAR(n)` | `CHAR(n)` |
| `VARCHAR(n)`, `STRING` | `VARCHAR(n)`, `VARCHAR` |
| `VARBINARY` | `VARBINARY` |
| `DATE` | `DATE` |
| `TIMESTAMP` | `TIMESTAMP` |
| `TIMESTAMP WITH LOCAL TIME ZONE` | `TIMESTAMP WITH TIME ZONE` |
| `ARRAY`, `MAP`, `ROW` | `ARRAY`, `MAP`, `ROW` |

## Next Steps

Use [Connecting Engines](./connecting-engines#troubleshooting) for catalog and storage checks.
Report connector-specific problems to [Apache Paimon Trino](https://github.com/apache/paimon-trino/issues),
including the Trino, Java, and connector versions and the table options.
