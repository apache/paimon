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

The following Flink SQL registers and uses a Paimon catalog named `my_catalog`. Metadata and table files are stored under `hdfs:///path/to/warehouse`.

```sql
CREATE CATALOG my_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'hdfs:///path/to/warehouse'
);

USE CATALOG my_catalog;
```

You can define any default table options with the prefix `table-default.` for tables created in the catalog.

{{< /tab >}}

{{< tab "Spark3" >}}

The following shell command registers a paimon catalog named `paimon`. Metadata and table files are stored under `hdfs:///path/to/warehouse`.

```bash
spark-sql ... \
    --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
    --conf spark.sql.catalog.paimon.warehouse=hdfs:///path/to/warehouse
```

You can define any default table options with the prefix `spark.sql.catalog.paimon.table-default.` for tables created in the catalog.

After `spark-sql` is started, you can switch to the `default` database of the `paimon` catalog with the following SQL.

```sql
USE paimon.default;
```

{{< /tab >}}

{{< /tabs >}}

## Creating a Catalog with Hive Metastore

By using Paimon Hive catalog, changes to the catalog will directly affect the corresponding Hive metastore. Tables created in such catalog can also be accessed directly from Hive.

To use Hive catalog, Database name, Table name and Field names should be **lower** case.

{{< tabs "hive-metastore-example" >}}

{{< tab "Flink" >}}

Paimon Hive catalog in Flink relies on Flink Hive connector bundled jar. You should first download Hive connector bundled jar and add it to classpath.

| Metastore version |  Bundle Name  | SQL Client JAR                                                                                                                                                                                                                                                                                                   |
|:------------------|:--------------|:-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| 2.3.0 - 3.1.3     | Flink Bundle  | [Download](https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/table/hive/overview/#using-bundled-hive-jar) |
| 1.2.0 - x.x.x     | Presto Bundle | [Download](https://repo.maven.apache.org/maven2/com/facebook/presto/hive/hive-apache/1.2.2-2/hive-apache-1.2.2-2.jar) |

The following Flink SQL registers and uses a Paimon Hive catalog named `my_hive`. Metadata and table files are stored under `hdfs:///path/to/warehouse`. In addition, metadata is also stored in Hive metastore.

If your Hive requires security authentication such as Kerberos, LDAP, Ranger or you want the paimon table to be managed
by Apache Atlas(Setting 'hive.metastore.event.listeners' in hive-site.xml). You can specify the hive-conf-dir and
hadoop-conf-dir parameter to the hive-site.xml file path. 

```sql
CREATE CATALOG my_hive WITH (
    'type' = 'paimon',
    'metastore' = 'hive',
    -- 'uri' = 'thrift://<hive-metastore-host-name>:<port>', default use 'hive.metastore.uris' in HiveConf
    -- 'hive-conf-dir' = '...', this is recommended in the kerberos environment
    -- 'hadoop-conf-dir' = '...', this is recommended in the kerberos environment
    -- 'warehouse' = 'hdfs:///path/to/warehouse', default use 'hive.metastore.warehouse.dir' in HiveConf
);

USE CATALOG my_hive;
```

You can define any default table options with the prefix `table-default.` for tables created in the catalog.

Also, you can create [FlinkGenericCatalog]({{< ref "engines/flink" >}}).

{{< /tab >}}

{{< tab "Spark3" >}}

Your Spark installation should be able to detect, or already contains Hive dependencies. See [here](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html) for more information.

The following shell command registers a Paimon Hive catalog named `paimon`. Metadata and table files are stored under `hdfs:///path/to/warehouse`. In addition, metadata is also stored in Hive metastore.

```bash
spark-sql ... \
    --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
    --conf spark.sql.catalog.paimon.warehouse=hdfs:///path/to/warehouse \
    --conf spark.sql.catalog.paimon.metastore=hive \
    --conf spark.sql.catalog.paimon.uri=thrift://<hive-metastore-host-name>:<port>
```

You can define any default table options with the prefix `spark.sql.catalog.paimon.table-default.` for tables created in the catalog.

After `spark-sql` is started, you can switch to the `default` database of the `paimon` catalog with the following SQL.

```sql
USE paimon.default;
```

Also, you can create [SparkGenericCatalog]({{< ref "engines/spark3" >}}).

{{< /tab >}}

{{< /tabs >}}

> When using hive catalog to change incompatible column types through alter table, you need to configure `hive.metastore.disallow.incompatible.col.type.changes=false`. see [HIVE-17832](https://issues.apache.org/jira/browse/HIVE-17832).

> If you are using Hive3, please disable Hive ACID:
>
> ```shell
> hive.strict.managed.tables=false
> hive.create.as.insert.only=false
> metastore.create.as.acid=false
> ```

### Setting Location in Properties

If you are using an object storage , and you don't want that the location of paimon table/database is accessed by the filesystem of hive,
which may lead to the error such as "No FileSystem for scheme: s3a".
You can set location in the properties of table/database by the config of `location-in-properties`. See
[setting the location of table/database in properties ]({{< ref "maintenance/configurations#HiveCatalogOptions" >}})

### Synchronizing Partitions into Hive Metastore

By default, Paimon does not synchronize newly created partitions into Hive metastore. Users will see an unpartitioned table in Hive. Partition push-down will be carried out by filter push-down instead.

If you want to see a partitioned table in Hive and also synchronize newly created partitions into Hive metastore, please set the table property `metastore.partitioned-table` to true. Also see [CoreOptions]({{< ref "maintenance/configurations#CoreOptions" >}}).

### Adding Parameters to a Hive Table

Using the table option facilitates the convenient definition of Hive table parameters. 
Parameters prefixed with `hive.` will be automatically defined in the `TBLPROPERTIES` of the Hive table. 
For instance, using the option `hive.table.owner=Jon` will automatically add the parameter `table.owner=Jon` to the table properties during the creation process.

