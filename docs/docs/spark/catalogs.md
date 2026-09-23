---
title: "Catalogs"
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

# Catalogs

Configure the connector JAR and `PaimonSparkSessionExtensions` as shown in
[Quick Start](./quick-start#setup) before using the snippets below. The `...` in each shell
command stands for those common startup arguments.

## Create Catalog

Choose a metastore for your Paimon catalog:

* `filesystem` metastore (default), which stores both metadata and table files in filesystems.
* `hive` metastore, which additionally stores metadata in Hive metastore. Users can directly access the tables from Hive.
* `jdbc` metastore, which additionally stores metadata in a relational database.
* `rest` metastore, which accesses a remote catalog service over HTTP.

See [CatalogOptions](../maintenance/configurations#catalogoptions) for detailed options when creating a catalog.

### Create Filesystem Catalog

The following shell command registers a Paimon catalog named `paimon`. Metadata and table files are stored under `hdfs:///path/to/warehouse`.

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

### Creating Hive Catalog

By using Paimon Hive catalog, changes to the catalog will directly affect the corresponding Hive metastore. Tables created in such catalog can also be accessed directly from Hive.

To use Hive catalog, database, table, and field names must be lowercase.

Your Spark installation should be able to detect, or already contains Hive dependencies. See [Spark Hive Tables](https://spark.apache.org/docs/latest/sql-data-sources-hive-tables.html) for more information.

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

To share Spark's built-in catalog with non-Paimon tables, see [SparkGenericCatalog](#sparkgenericcatalog).

**Synchronizing Partitions into Hive Metastore**

By default, Paimon does not synchronize newly created partitions into Hive metastore. Users will see an unpartitioned table in Hive. Partition push-down will be carried out by filter push-down instead.

If you want to see a partitioned table in Hive and also synchronize newly created partitions into Hive metastore, please set the table property `metastore.partitioned-table` to true. Also see [CoreOptions](../maintenance/configurations#coreoptions).

### Creating JDBC Catalog

The JDBC catalog stores catalog metadata in a relational database such as SQLite, MySQL, or PostgreSQL.

Currently, lock configuration is only supported for MySQL and SQLite. If you are using a different type of database for catalog storage, please do not configure `lock.enabled`.

Add the JDBC driver for your database to Spark's classpath along with the Paimon connector.

| database type | Bundle Name          | SQL Client JAR                                                             |
|:--------------|:---------------------|:---------------------------------------------------------------------------|
| mysql         | mysql-connector-java | [Download](https://mvnrepository.com/artifact/mysql/mysql-connector-java)  |
| postgres      | postgresql           | [Download](https://mvnrepository.com/artifact/org.postgresql/postgresql)   |

JDBC catalog supports persistent Paimon views. View metadata is stored in the automatically created
`paimon_views` table in the catalog database.

Within a single database, a name cannot be used by both a table and a view; all writers
(`createTable`, `renameTable`, `createView`, `renameView`) reject conflicting identifiers. See
[Views](../concepts/views#jdbc-catalog-notes) for upgrade-time permissions, single-process locking
semantics, and database visibility details.

```bash
spark-sql ... \
    --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
    --conf spark.sql.catalog.paimon.warehouse=hdfs:///path/to/warehouse \
    --conf spark.sql.catalog.paimon.metastore=jdbc \
    --conf spark.sql.catalog.paimon.uri=jdbc:mysql://<host>:<port>/<databaseName> \
    --conf spark.sql.catalog.paimon.jdbc.user=... \
    --conf spark.sql.catalog.paimon.jdbc.password=...

```

```sql
USE paimon.default;
```
### Creating REST Catalog

The REST catalog sends catalog operations to a remote service. Choose the authentication
method configured by that service. The provider key is spelled `bear` for bearer tokens.

#### Bearer Token
```bash
spark-sql ... \
    --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
    --conf spark.sql.catalog.paimon.metastore=rest \
    --conf spark.sql.catalog.paimon.uri=<catalog server url> \
    --conf spark.sql.catalog.paimon.token.provider=bear \
    --conf spark.sql.catalog.paimon.token=<token>

```

#### DLF Access Key
```bash
spark-sql ... \
    --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
    --conf spark.sql.catalog.paimon.metastore=rest \
    --conf spark.sql.catalog.paimon.uri=<catalog server url> \
    --conf spark.sql.catalog.paimon.token.provider=dlf \
    --conf spark.sql.catalog.paimon.dlf.access-key-id=<access-key-id> \
    --conf spark.sql.catalog.paimon.dlf.access-key-secret=<access-key-secret>

```

#### DLF STS Token
```bash
spark-sql ... \
    --conf spark.sql.catalog.paimon=org.apache.paimon.spark.SparkCatalog \
    --conf spark.sql.catalog.paimon.metastore=rest \
    --conf spark.sql.catalog.paimon.uri=<catalog server url> \
    --conf spark.sql.catalog.paimon.token.provider=dlf \
    --conf spark.sql.catalog.paimon.dlf.access-key-id=<access-key-id> \
    --conf spark.sql.catalog.paimon.dlf.access-key-secret=<access-key-secret> \
    --conf spark.sql.catalog.paimon.dlf.security-token=<security-token>

```

```sql
USE paimon.default;
```

## SparkGenericCatalog

When starting `spark-sql`, use the following command to register Paimon's Spark Generic catalog to replace Spark
default catalog `spark_catalog`. (default warehouse is Spark `spark.sql.warehouse.dir`)

Currently, it is only recommended to use `SparkGenericCatalog` in the case of Hive metastore, Paimon will infer
Hive conf from Spark session, you just need to configure Spark's Hive conf.

```bash
spark-sql ... \
    --conf spark.sql.catalog.spark_catalog=org.apache.paimon.spark.SparkGenericCatalog \
    --conf spark.sql.extensions=org.apache.paimon.spark.extensions.PaimonSparkSessionExtensions
```

Using `SparkGenericCatalog`, you can use Paimon tables in this Catalog or non-Paimon tables such as Spark's csv,
parquet, Hive tables, etc.

Use `USING paimon` when creating a Paimon table in `SparkGenericCatalog`:

```sql
CREATE TABLE my_table (k INT, v STRING)
USING paimon
TBLPROPERTIES ('primary-key' = 'k');
```

## Next Steps

See [Configuration](./configuration) for option scopes, and [Create Tables, Views, and Tags](./sql-ddl)
for SQL DDL.
