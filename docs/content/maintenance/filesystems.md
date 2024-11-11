---
title: "Filesystems"
weight: 1
type: docs
aliases:
- /maintenance/filesystems.html
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

# Filesystems

Apache Paimon utilizes the same pluggable file systems as Apache Flink. Users can follow the
[standard plugin mechanism](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/filesystems/plugins/)
to configure the plugin structure if using Flink as compute engine. However, for other engines like Spark
or Hive, the provided opt jars (by Flink) may get conflicts and cannot be used directly. It is not convenient
for users to fix class conflicts, thus Paimon provides the self-contained and engine-unified
FileSystem pluggable jars for user to query tables from Spark/Hive side.

## Supported FileSystems

| FileSystem        | URI Scheme | Pluggable | Description                                                            |
|:------------------|:-----------|-----------|:-----------------------------------------------------------------------|
| Local File System | file://    | N         | Built-in Support                                                       |
| HDFS              | hdfs://    | N         | Built-in Support, ensure that the cluster is in the hadoop environment |
| Aliyun OSS        | oss://     | Y         |                                                                        |
| S3                | s3://      | Y         |                                                                        |

## Dependency

We recommend you to download the jar directly: [Download Link]({{< ref "project/download#filesystem-jars" >}}).

You can also manually build bundled jar from the source code.

To build from source code, [clone the git repository]({{< github_repo >}}).

Build shaded jar with the following command.

```bash
mvn clean install -DskipTests
```

You can find the shaded jars under
`./paimon-filesystems/paimon-${fs}/target/paimon-${fs}-{{< version >}}.jar`.

## HDFS

You don't need any additional dependencies to access HDFS because you have already taken care of the Hadoop dependencies.

### HDFS Configuration

For HDFS, the most important thing is to be able to read your HDFS configuration.

{{< tabs "hdfs conf" >}}

{{< tab "Flink" >}}

You may not have to do anything, if you are in a hadoop environment. Otherwise pick one of the following ways to
configure your HDFS:

1. Set environment variable `HADOOP_HOME` or `HADOOP_CONF_DIR`.
2. Configure `'hadoop-conf-dir'` in the paimon catalog.
3. Configure Hadoop options through prefix `'hadoop.'` in the paimon catalog.

The first approach is recommended.

If you do not want to include the value of the environment variable, you can configure `hadoop-conf-loader` to `option`.

{{< /tab >}}

{{< tab "Hive/Spark" >}}

HDFS Configuration is available directly through the computation cluster, see cluster configuration of Hive and Spark for details.

{{< /tab >}}

{{< /tabs >}}

### Hadoop-compatible file systems (HCFS)

All Hadoop file systems are automatically available when the Hadoop libraries are on the classpath.

This way, Paimon seamlessly supports all of Hadoop file systems implementing the `org.apache.hadoop.fs.FileSystem`
interface, and all Hadoop-compatible file systems (HCFS).

- HDFS
- Alluxio (see configuration specifics below)
- XtreemFS
- …

The Hadoop configuration has to have an entry for the required file system implementation in the `core-site.xml` file.

For Alluxio support add the following entry into the core-site.xml file:

```shell
<property>
  <name>fs.alluxio.impl</name>
  <value>alluxio.hadoop.FileSystem</value>
</property>
```

### Kerberos

{{< tabs "Kerberos" >}}

{{< tab "Flink" >}}

It is recommended to use [Flink Kerberos Keytab](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/security/security-kerberos/).

{{< /tab >}}

{{< tab "Spark" >}}

It is recommended to use [Spark Kerberos Keytab](https://spark.apache.org/docs/latest/security.html#using-a-keytab).

{{< /tab >}}

{{< tab "Hive" >}}

An intuitive approach is to configure Hive's kerberos authentication.

{{< /tab >}}

{{< tab "Trino/JavaAPI" >}}

Configure the following three options in your catalog configuration:

- security.kerberos.login.keytab: Absolute path to a Kerberos keytab file that contains the user credentials.
  Please make sure it is copied to each machine.
- security.kerberos.login.principal: Kerberos principal name associated with the keytab.
- security.kerberos.login.use-ticket-cache: True or false, indicates whether to read from your Kerberos ticket cache.

For JavaAPI:
```
SecurityContext.install(catalogOptions);
```

{{< /tab >}}

{{< /tabs >}}

### HDFS HA

Ensure that `hdfs-site.xml` and `core-site.xml` contain the necessary [HA configuration](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HDFSHighAvailabilityWithNFS.html).

### HDFS ViewFS

Ensure that `hdfs-site.xml` and `core-site.xml` contain the necessary [ViewFs configuration](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/ViewFs.html).

## OSS

{{< stable >}}

Download [paimon-oss-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-oss/{{< version >}}/paimon-oss-{{< version >}}.jar).

{{< /stable >}}

{{< unstable >}}

Download [paimon-oss-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-oss/{{< version >}}/).

{{< /unstable >}}

{{< tabs "oss" >}}

{{< tab "Flink" >}}

{{< hint info >}}
If you have already configured [oss access through Flink](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/filesystems/oss/) (Via Flink FileSystem),
here you can skip the following configuration.
{{< /hint >}}

Put `paimon-oss-{{< version >}}.jar` into `lib` directory of your Flink home, and create catalog:

```sql
CREATE CATALOG my_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'oss://<bucket>/<path>',
    'fs.oss.endpoint' = 'oss-cn-hangzhou.aliyuncs.com',
    'fs.oss.accessKeyId' = 'xxx',
    'fs.oss.accessKeySecret' = 'yyy'
);
```

{{< /tab >}}

{{< tab "Spark" >}}

{{< hint info >}}
If you have already configured oss access through Spark (Via Hadoop FileSystem), here you can skip the following configuration.
{{< /hint >}}

Place `paimon-oss-{{< version >}}.jar` together with `paimon-spark-{{< version >}}.jar` under Spark's jars directory, and start like

```shell
spark-sql \ 
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.warehouse=oss://<bucket>/<path> \
  --conf spark.sql.catalog.paimon.fs.oss.endpoint=oss-cn-hangzhou.aliyuncs.com \
  --conf spark.sql.catalog.paimon.fs.oss.accessKeyId=xxx \
  --conf spark.sql.catalog.paimon.fs.oss.accessKeySecret=yyy
```

{{< /tab >}}

{{< tab "Hive" >}}

{{< hint info >}}
If you have already configured oss access through Hive (Via Hadoop FileSystem), here you can skip the following configuration.
{{< /hint >}}

NOTE: You need to ensure that Hive metastore can access `oss`.

Place `paimon-oss-{{< version >}}.jar` together with `paimon-hive-connector-{{< version >}}.jar` under Hive's auxlib directory, and start like

```sql
SET paimon.fs.oss.endpoint=oss-cn-hangzhou.aliyuncs.com;
SET paimon.fs.oss.accessKeyId=xxx;
SET paimon.fs.oss.accessKeySecret=yyy;
```

And read table from hive metastore, table can be created by Flink or Spark, see [Catalog with Hive Metastore]({{< ref "flink/sql-ddl" >}})
```sql
SELECT * FROM test_table;
SELECT COUNT(1) FROM test_table;
```

{{< /tab >}}
{{< tab "Trino" >}}

From version 0.8, paimon-trino uses trino filesystem as basic file read and write system. We strongly recommend you to use jindo-sdk in trino.

You can find [How to config jindo sdk on trino](https://github.com/aliyun/alibabacloud-jindodata/blob/master/docs/user/4.x/4.6.x/4.6.12/oss/presto/jindosdk_on_presto.md) here.
Please note that:
* Use paimon to replace hive-hadoop2 when you decompress the plugin jar and find location to put in.
* You can specify the `core-site.xml` in `paimon.properties` on configuration [hive.config.resources](https://trino.io/docs/current/connector/hive.html#hdfs-configuration).
* Presto and Jindo use the same configuration method.

{{< /tab >}}
{{< /tabs >}}

## S3

{{< stable >}}

Download [paimon-s3-{{< version >}}.jar](https://repo.maven.apache.org/maven2/org/apache/paimon/paimon-s3/{{< version >}}/paimon-s3-{{< version >}}.jar).

{{< /stable >}}

{{< unstable >}}

Download [paimon-s3-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-s3/{{< version >}}/).

{{< /unstable >}}

{{< tabs "s3" >}}

{{< tab "Flink" >}}

{{< hint info >}}
If you have already configured [s3 access through Flink](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/filesystems/s3/) (Via Flink FileSystem),
here you can skip the following configuration.
{{< /hint >}}

Put `paimon-s3-{{< version >}}.jar` into `lib` directory of your Flink home, and create catalog:

```sql
CREATE CATALOG my_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 's3://<bucket>/<path>',
    's3.endpoint' = 'your-endpoint-hostname',
    's3.access-key' = 'xxx',
    's3.secret-key' = 'yyy'
);
```

{{< /tab >}}

{{< tab "Spark" >}}

{{< hint info >}}
If you have already configured s3 access through Spark (Via Hadoop FileSystem), here you can skip the following configuration.
{{< /hint >}}

Place `paimon-s3-{{< version >}}.jar` together with `paimon-spark-{{< version >}}.jar` under Spark's jars directory, and start like

```shell
spark-sql \ 
  --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
  --conf spark.sql.catalog.paimon.warehouse=s3://<bucket>/<path> \
  --conf spark.sql.catalog.paimon.s3.endpoint=your-endpoint-hostname \
  --conf spark.sql.catalog.paimon.s3.access-key=xxx \
  --conf spark.sql.catalog.paimon.s3.secret-key=yyy
```

{{< /tab >}}

{{< tab "Hive" >}}

{{< hint info >}}
If you have already configured s3 access through Hive ((Via Hadoop FileSystem)), here you can skip the following configuration.
{{< /hint >}}

NOTE: You need to ensure that Hive metastore can access `s3`.

Place `paimon-s3-{{< version >}}.jar` together with `paimon-hive-connector-{{< version >}}.jar` under Hive's auxlib directory, and start like

```sql
SET paimon.s3.endpoint=your-endpoint-hostname;
SET paimon.s3.access-key=xxx;
SET paimon.s3.secret-key=yyy;
```

And read table from hive metastore, table can be created by Flink or Spark, see [Catalog with Hive Metastore]({{< ref "flink/sql-ddl" >}})
```sql
SELECT * FROM test_table;
SELECT COUNT(1) FROM test_table;
```

{{< /tab >}}

{{< tab "Trino" >}}

Paimon use shared trino filesystem as basic read and write system.

Please refer to [Trino S3](https://trino.io/docs/current/object-storage/file-system-s3.html) to config s3 filesystem in trino.

{{< /tab >}}

{{< /tabs >}}

### S3 Complaint Object Stores

The S3 Filesystem also support using S3 compliant object stores such as MinIO, Tencent's COS and IBM’s Cloud Object
Storage. Just configure your endpoint to the provider of the object store service.

```yaml
s3.endpoint: your-endpoint-hostname
```

### Configure Path Style Access

Some S3 compliant object stores might not have virtual host style addressing enabled by default, for example when using Standalone MinIO for testing purpose.
In such cases, you will have to provide the property to enable path style access.

```yaml
s3.path.style.access: true
```

### S3A Performance

[Tune Performance](https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/performance.html) for `S3AFileSystem`.

If you encounter the following exception:
```shell
Caused by: org.apache.http.conn.ConnectionPoolTimeoutException: Timeout waiting for connection from pool.
```
Try to configure this in catalog options: `fs.s3a.connection.maximum=1000`.
