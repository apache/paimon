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

Paimon catalogs currently support two types of metastores:

* `filesystem` metastore (default), which stores both metadata and table files in filesystems.
* `hive` metastore, which additionally stores metadata in Hive metastore. Users can directly access the tables from Hive.

See [CatalogOptions]({{< ref "maintenance/configurations#catalogoptions" >}}) for detailed options when creating a catalog.

## Creating a Catalog with Filesystem Metastore

{{< tabs "filesystem-metastore-example" >}}

{{< tab "Flink" >}}

The following Flink SQL registers and uses a Paimon catalog named `my_catalog`. Metadata and table files are stored under `hdfs://path/to/warehouse`.

```sql
CREATE CATALOG my_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'hdfs://path/to/warehouse'
);

USE CATALOG my_catalog;
```

{{< /tab >}}

{{< tab "Spark3" >}}

The following shell command registers a paimon catalog named `paimon`. Metadata and table files are stored under `hdfs://path/to/warehouse`.

```bash
spark-sql ... \
    --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
    --conf spark.sql.catalog.paimon.warehouse=hdfs://path/to/warehouse
```

After `spark-sql` is started, you can switch to the `default` database of the `paimon` catalog with the following SQL.

```sql
USE paimon.default;
```

{{< /tab >}}

{{< /tabs >}}

## Creating a Catalog with Hive Metastore

By using Paimon Hive catalog, changes to the catalog will directly affect the corresponding Hive metastore. Tables created in such catalog can also be accessed directly from Hive.

### Preparing Paimon Hive Catalog Jar File

Download the jar file with corresponding version.

{{< stable >}}

| Version    | Jar                                                                                                                                                                                    |
|------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Hive 2 & 3 | [paimon-hive-catalog-{{< version >}}.jar](https://www.apache.org/dyn/closer.lua/flink/paimon-{{< version >}}/paimon-hive-catalog-{{< version >}}.jar) |

{{< /stable >}}

{{< unstable >}}

| Version    | Jar                                                                                                                                                                                    |
|------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Hive 2 & 3 | [paimon-hive-catalog-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-hive-catalog/{{< version >}}/) |

{{< /unstable >}}

You can also manually build bundled jar from the source code.

To build from source code, [clone the git repository]({{< github_repo >}}).

Build bundled jar with the following command.
`mvn clean install -Dmaven.test.skip=true`

You can find Hive catalog jar in `./paimon-hive/paimon-hive-catalog/target/paimon-hive-catalog-{{< version >}}.jar`.

### Registering Hive Catalog

{{< tabs "hive-metastore-example" >}}

{{< tab "Flink" >}}

To enable Paimon Hive catalog support in Flink, you can pick one of the following two methods.

* Copy Paimon Hive catalog jar file into the `lib` directory of your Flink installation directory. Note that this must be done before starting your Flink cluster.
* If you're using Flink's SQL client, append `--jar /path/to/paimon-hive-catalog-{{< version >}}.jar` to the starting command of SQL client.

The following Flink SQL registers and uses a Paimon Hive catalog named `my_hive`. Metadata and table files are stored under `hdfs://path/to/warehouse`. In addition, metadata is also stored in Hive metastore.

```sql
CREATE CATALOG my_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    'uri' = 'thrift://<hive-metastore-host-name>:<port>',
    'warehouse' = 'hdfs://path/to/warehouse'
);

USE CATALOG my_hive;
```

{{< /tab >}}

{{< tab "Spark3" >}}

To enable Paimon Hive catalog support in Spark3, append the path of Paimon Hive catalog jar file to `--jars` argument when starting spark.

The following shell command registers a Table tore Hive catalog named `paimon`. Metadata and table files are stored under `hdfs://path/to/warehouse`. In addition, metadata is also stored in Hive metastore.

```bash
spark-sql ... \
    --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
    --conf spark.sql.catalog.paimon.warehouse=hdfs://path/to/warehouse \
    --conf spark.sql.catalog.paimon.metastore=hive \
    --conf spark.sql.catalog.paimon.uri=thrift://<hive-metastore-host-name>:<port>
```

After `spark-sql` is started, you can switch to the `default` database of the `paimon` catalog with the following SQL.

```sql
USE paimon.default;
```

{{< /tab >}}

{{< /tabs >}}