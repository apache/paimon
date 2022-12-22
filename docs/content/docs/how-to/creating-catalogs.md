---
title: "Creating Catalogs"
weight: 1
type: docs
aliases:
- /how-to/creating-catalogs.html
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

# Creating Catalogs

Table Store catalogs currently support two types of metastores:

* `filesystem` metastore (default), which stores both metadata and table files in filesystems.
* `hive` metastore, which additionally stores metadata in Hive metastore. Users can directly access the tables from Hive.

## Creating a Catalog with Filesystem Metastore

{{< tabs "filesystem-metastore-example" >}}

{{< tab "Flink" >}}

The following Flink SQL registers and uses a Table Store catalog named `my_catalog`. Metadata and table files are stored under `hdfs://path/to/warehouse`.

```sql
CREATE CATALOG my_catalog WITH (
    'type' = 'table-store',
    'warehouse' = 'hdfs://path/to/warehouse'
);

USE CATALOG my_catalog;
```

{{< /tab >}}

{{< tab "Spark3" >}}

The following shell command registers a Table Store catalog named `tablestore`. Metadata and table files are stored under `hdfs://path/to/warehouse`.

```bash
spark-sql ... \
    --conf spark.sql.catalog.tablestore=org.apache.flink.table.store.spark.SparkCatalog \
    --conf spark.sql.catalog.tablestore.warehouse=hdfs://path/to/warehouse
```

After `spark-sql` is started, you can switch to the `default` database of the `tablestore` catalog with the following SQL.

```sql
USE tablestore.default;
```

{{< /tab >}}

{{< /tabs >}}

## Creating a Catalog with Hive Metastore

By using Table Store Hive catalog, changes to the catalog will directly affect the corresponding Hive metastore. Tables created in such catalog can also be accessed directly from Hive.

### Preparing Table Store Hive Catalog Jar File

{{< stable >}}

Download the jar file with corresponding version.

| Version | Jar |
|---|---|
| Hive 3.1 | [flink-table-store-hive-catalog-{{< version >}}_3.1.jar](https://www.apache.org/dyn/closer.lua/flink/flink-table-store-{{< version >}}/flink-table-store-hive-catalog-{{< version >}}_3.1.jar) |
| Hive 2.3 | [flink-table-store-hive-catalog-{{< version >}}_2.3.jar](https://www.apache.org/dyn/closer.lua/flink/flink-table-store-{{< version >}}/flink-table-store-hive-catalog-{{< version >}}_2.3.jar) |
| Hive 2.2 | [flink-table-store-hive-catalog-{{< version >}}_2.2.jar](https://www.apache.org/dyn/closer.lua/flink/flink-table-store-{{< version >}}/flink-table-store-hive-catalog-{{< version >}}_2.2.jar) |
| Hive 2.1 | [flink-table-store-hive-catalog-{{< version >}}_2.1.jar](https://www.apache.org/dyn/closer.lua/flink/flink-table-store-{{< version >}}/flink-table-store-hive-catalog-{{< version >}}_2.1.jar) |

You can also manually build bundled jar from the source code.

{{< /stable >}}

{{< unstable >}}

You are using an unreleased version of Table Store so you need to manually build bundled jar from the source code.

{{< /unstable >}}

To build from source code, either [download the source of a release](https://flink.apache.org/downloads.html) or [clone the git repository]({{< github_repo >}}).

Build bundled jar with the following command.

| Version | Command |
|---|---|
| Hive 3.1 | `mvn clean install -Dmaven.test.skip=true -Phive-3.1` |
| Hive 2.3 | `mvn clean install -Dmaven.test.skip=true` |
| Hive 2.2 | `mvn clean install -Dmaven.test.skip=true -Phive-2.2` |
| Hive 2.1 | `mvn clean install -Dmaven.test.skip=true -Phive-2.1` |
| Hive 2.1 CDH 6.3 | `mvn clean install -Dmaven.test.skip=true -Phive-2.1-cdh-6.3` |

You can find Hive catalog jar in `./flink-table-store-hive/flink-table-store-hive-catalog/target/flink-table-store-hive-catalog-{{< version >}}.jar`.

### Registering Hive Catalog

{{< tabs "hive-metastore-example" >}}

{{< tab "Flink" >}}

To enable Table Store Hive catalog support in Flink, you can pick one of the following two methods.

* Copy Table Store Hive catalog jar file into the `lib` directory of your Flink installation directory. Note that this must be done before starting your Flink cluster.
* If you're using Flink's SQL client, append `--jar /path/to/flink-table-store-hive-catalog-{{< version >}}.jar` to the starting command of SQL client.

The following Flink SQL registers and uses a Table Store Hive catalog named `my_hive`. Metadata and table files are stored under `hdfs://path/to/warehouse`. In addition, metadata is also stored in Hive metastore.

```sql
CREATE CATALOG my_hive WITH (
    'type' = 'table-store',
    'metastore' = 'hive',
    'uri' = 'thrift://<hive-metastore-host-name>:<port>',
    'warehouse' = 'hdfs://path/to/warehouse'
);

USE CATALOG my_hive;
```

{{< /tab >}}

{{< tab "Spark3" >}}

To enable Table Store Hive catalog support in Spark3, append the path of Table Store Hive catalog jar file to `--jars` argument when starting spark.

The following shell command registers a Table tore Hive catalog named `tablestore`. Metadata and table files are stored under `hdfs://path/to/warehouse`. In addition, metadata is also stored in Hive metastore.

```bash
spark-sql ... \
    --conf spark.sql.catalog.tablestore=org.apache.flink.table.store.spark.SparkCatalog \
    --conf spark.sql.catalog.tablestore.warehouse=hdfs://path/to/warehouse \
    --conf spark.sql.catalog.tablestore.metastore=hive \
    --conf spark.sql.catalog.tablestore.uri=thrift://<hive-metastore-host-name>:<port>
```

After `spark-sql` is started, you can switch to the `default` database of the `tablestore` catalog with the following SQL.

```sql
USE tablestore.default;
```

{{< /tab >}}

{{< /tabs >}}