---
title: "SQL DDL"
sidebar_position: 2
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

# SQL DDL

Create a catalog first, then define the table's columns, keys, partitions, and options.
Use [SQL Alter](./sql-alter) to evolve an existing table and [SQL Write](./sql-write) to populate it.

## Create Catalog

| Catalog | Metadata location and use case | Guide |
| --- | --- | --- |
| Filesystem (default) | Store metadata and data in the warehouse filesystem. | [Filesystem Catalog](#create-filesystem-catalog) |
| Hive | Register tables with Hive Metastore for shared access. | [Hive Catalog](#creating-hive-catalog) |
| JDBC | Store catalog metadata in a relational database. | [JDBC Catalog](#creating-jdbc-catalog) |
| REST | Access a catalog service through the REST API. | [REST Catalog](../concepts/rest/) |
| Generic | Use Hive Metastore to manage Paimon, Hive, and other Flink connector tables together. | [Generic Catalog](#creating-generic-catalog) |

See [CatalogOptions](../maintenance/configurations#catalogoptions) for detailed options when creating a catalog.

### Create Filesystem Catalog

The following Flink SQL registers and uses a Paimon catalog named `my_catalog`. Metadata and table files are stored under `hdfs:///path/to/warehouse`.

```sql
CREATE CATALOG my_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'hdfs:///path/to/warehouse'
);

USE CATALOG my_catalog;
```

You can define any default table options with the prefix `table-default.` for tables created in the catalog.

### Creating Hive Catalog

By using Paimon Hive catalog, changes to the catalog will directly affect the corresponding Hive metastore. Tables created in such catalog can also be accessed directly from Hive.

To use Hive catalog, Database name, Table name and Field names should be **lower** case.

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

Also, you can create [FlinkGenericCatalog](./quick-start).

> When using hive catalog to change incompatible column types through alter table, you need to configure `hive.metastore.disallow.incompatible.col.type.changes=false` on the **Hive Metastore server** (in its `hive-site.xml`, then restart HMS). Setting this on the Paimon catalog or via Flink SQL `SET` only configures the client-side HiveConf and is not propagated to the remote HMS over Thrift. See [HIVE-17832](https://issues.apache.org/jira/browse/HIVE-17832).

> If you are using Hive3, please disable Hive ACID:
>
> ```shell
> hive.strict.managed.tables=false
> hive.create.as.insert.only=false
> metastore.create.as.acid=false
> ```

#### Synchronizing Partitions into Hive Metastore

By default, Paimon does not synchronize newly created partitions into Hive metastore. Users will see an unpartitioned table in Hive. Partition push-down will be carried out by filter push-down instead.

If you want to see a partitioned table in Hive and also synchronize newly created partitions into Hive metastore, please set the table property `metastore.partitioned-table` to true. Also see [CoreOptions](../maintenance/configurations#coreoptions).

#### Adding Parameters to a Hive Table

Using the table option facilitates the convenient definition of Hive table parameters.
Parameters prefixed with `hive.` will be automatically defined in the `TBLPROPERTIES` of the Hive table.
For instance, using the option `hive.table.owner=Jon` will automatically add the parameter `table.owner=Jon` to the table properties during the creation process.

#### Setting Location in Properties

If you are using an object storage , and you don't want that the location of paimon table/database is accessed by the filesystem of hive,
which may lead to the error such as "No FileSystem for scheme: s3a".
You can set location in the properties of table/database by the config of `location-in-properties`. See
[setting the location of table/database in properties ](../maintenance/configurations#hivecatalogoptions)

### Creating JDBC Catalog

By using the Paimon JDBC catalog, changes to the catalog will be directly stored in relational databases such as SQLite, MySQL, postgres, etc.

Currently, lock configuration is only supported for MySQL and SQLite. If you are using a different type of database for catalog storage, please do not configure `lock.enabled`.

Paimon JDBC Catalog in Flink needs to correctly add the corresponding jar package for connecting to the database. You should first download JDBC  connector bundled jar and add it to classpath. such as MySQL, postgres

| database type | Bundle Name          | SQL Client JAR                                                             |
|:--------------|:---------------------|:---------------------------------------------------------------------------|
| mysql         | mysql-connector-java | [Download](https://mvnrepository.com/artifact/mysql/mysql-connector-java)  |
| postgres      | postgresql           | [Download](https://mvnrepository.com/artifact/org.postgresql/postgresql)   |

```sql
CREATE CATALOG my_jdbc WITH (
    'type' = 'paimon',
    'metastore' = 'jdbc',
    'uri' = 'jdbc:mysql://<host>:<port>/<databaseName>',
    'jdbc.user' = '...',
    'jdbc.password' = '...',
    'catalog-key'='jdbc',
    'warehouse' = 'hdfs:///path/to/warehouse'
);

USE CATALOG my_jdbc;
```
You can configure any connection parameters that have been declared by JDBC through "jdbc.", the connection parameters may be different between different databases, please configure according to the actual situation.

You can also perform logical isolation for databases under multiple catalogs by specifying "catalog-key".

Additionally, when creating a JdbcCatalog, you can specify the maximum length for the lock key by configuring "lock-key-max-length," which defaults to 255. Since this value is a combination of {catalog-key}.{database-name}.{table-name}, please adjust accordingly.

JDBC catalog supports persistent Paimon views. View metadata is stored in the automatically created
`paimon_views` table in the catalog database. The view SQL is stored as catalog metadata and is not
resolved when the view is created.

Within a single database, a name cannot be used by both a table and a view; all writers
(`createTable`, `renameTable`, `createView`, `renameView`) reject conflicting identifiers. See
[Views](../concepts/views#jdbc-catalog-notes) for upgrade-time permissions, single-process locking
semantics, and database visibility details.

```sql
CREATE VIEW sales_view AS SELECT name, amount FROM sales WHERE amount > 100;

SHOW VIEWS;

DROP VIEW sales_view;
```

You can define any default table options with the prefix `table-default.` for tables created in the catalog.

### Creating Generic Catalog

A generic catalog uses Hive Metastore and can contain Paimon, Hive, and Flink connector tables
(such as Kafka tables). Install the [Hive dependencies](#creating-hive-catalog) first.

Specify `'connector' = 'paimon'` when creating a Paimon table in this catalog.

:::info

Paimon will use `hive.metastore.warehouse.dir` in your `hive-site.xml`, please use path with scheme.
For example, `hdfs://...`. Otherwise, Paimon will use the local path.

:::

```sql
CREATE CATALOG my_catalog WITH (
    'type'='paimon-generic',
    'hive-conf-dir'='...',
    'hadoop-conf-dir'='...'
);

USE CATALOG my_catalog;

-- create a word count table
CREATE TABLE word_count (
    word STRING PRIMARY KEY NOT ENFORCED,
    cnt BIGINT
) WITH (
    'connector'='paimon'
);
```

## Create Table

The following examples create managed Paimon tables in the selected catalog. Dropping these
tables also deletes their table files. Choose [append-table](../append-table/) or
[primary-key](../primary-key-table/) semantics before defining keys and partitions.

The following SQL assumes that you have registered and are using a Paimon catalog. It creates a managed table named
`my_table` with five columns in the catalog's `default` database, where `dt`, `hh` and `user_id` are the primary keys.

```sql
CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING,
    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED
);
```

To partition the same row layout by day and hour, create a separate table:

```sql
CREATE TABLE partitioned_events (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING,
    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED
) PARTITIONED BY (dt, hh);
```

:::info

If you need cross partition upsert (primary keys not contain all partition fields), see [Cross partition Upsert](../primary-key-table/data-distribution#cross-partitions-upsert) mode.

:::

:::info

By configuring [partition.expiration-time](../maintenance/manage-partitions), expired partitions can be automatically deleted.

:::

### Specify Statistics Mode

Paimon will automatically collect the statistics of the data file for speeding up the query process. There are four modes supported:

- `full`: collect the full metrics: `null_count, min, max` .
- `truncate(length)`: length can be any positive number, the default mode is `truncate(16)`, which means collect the null count, min/max value with truncated length of 16.
  This is mainly to avoid too big column which will enlarge the manifest file.
- `counts`: only collect the null count.
- `none`: disable the metadata stats collection.

The statistics collector mode can be configured by `'metadata.stats-mode'`, by default is `'truncate(16)'`.
You can configure the field level by setting `'fields.{field_name}.stats-mode'`.

For the stats mode of `none`, by default `metadata.stats-dense-store` is `true`, which will significantly reduce the
storage size of the manifest. But the Paimon sdk in reading engine requires at least version 0.9.1 or 1.0.0 or higher.

### Field Default Value

For the default-value procedure and write semantics, see [Default Value](./default-value).
You can also set a field default through the table property `'fields.item_id.default-value'`,
note that partition fields and primary key fields can not be specified.

## Create Table As Select

`CREATE TABLE AS SELECT` (CTAS) derives columns from a query and writes its result into the
new table. Choose batch mode for a bounded copy, or enable checkpointing for streaming CTAS.

The following examples use one source schema and create independent destination tables:

```sql
SET 'execution.runtime-mode' = 'batch';

CREATE TABLE source_events (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING
);

INSERT INTO source_events VALUES (1, 10, 'view', '2026-09-01', '10');

-- Copy the query result using default table options.
CREATE TABLE copied_events AS SELECT * FROM source_events;

-- Select a partition field for the destination.
CREATE TABLE daily_events WITH ('partition' = 'dt')
AS SELECT * FROM source_events;

-- Choose a file format.
CREATE TABLE parquet_events WITH ('file.format' = 'parquet')
AS SELECT * FROM source_events;

-- Define a primary key for upsert semantics.
CREATE TABLE keyed_events WITH ('primary-key' = 'dt,hh,user_id')
AS SELECT * FROM source_events;

-- Combine primary keys and partitions.
CREATE TABLE partitioned_keyed_events WITH (
    'primary-key' = 'dt,hh,user_id',
    'partition' = 'dt'
) AS SELECT * FROM source_events;
```

## Create Table Like

Use `CREATE TABLE LIKE` to copy a schema and partition definition. This example uses
`EXCLUDING OPTIONS`, so it does **not** inherit the source table's options. Configure the target
options separately instead of inadvertently reusing source-specific settings such as a path.

```sql
-- Reuse the partitioned_events definition from Create Table above.
CREATE TABLE events_like LIKE partitioned_events (EXCLUDING OPTIONS);
```

`LIKE` copies the definition without copying data. Use CTAS when the new table should also
contain the result of a query.

## Work with Flink Temporary Tables

A temporary table's definition belongs to the current SQL session. Dropping it or closing the
session removes that definition without deleting the external data. Use temporary tables to
read another connector alongside tables in the Paimon catalog.

This example assumes a Paimon catalog is selected and that the CSV file exists at the configured
path. Both sides of the join explicitly use the same key column:

```sql
SET 'execution.runtime-mode' = 'batch';

CREATE TABLE product_names (
    product_id BIGINT PRIMARY KEY NOT ENFORCED,
    name STRING
);

CREATE TEMPORARY TABLE product_prices (
    product_id BIGINT,
    price DECIMAL(10, 2)
) WITH (
    'connector' = 'filesystem',
    'path' = 'file:/tmp/product-prices.csv',
    'format' = 'csv'
);

SELECT n.product_id, n.name, p.price
FROM product_names AS n
JOIN product_prices AS p ON n.product_id = p.product_id;
```

For a distributed job, use a path accessible to all tasks and install the required filesystem
dependencies; see [Installation](./installation).

## Catalog-managed Format Table Partitions

:::info

For an internal Format Table in a REST catalog, `metastore.partitioned-table = true` makes the
catalog the source of truth for partitions, which requires an internal table in a catalog that supports it (currently the REST
catalog) and cannot be combined with `format-table.implementation = engine`. On a Paimon table
the same option keeps its existing meaning (synchronize partitions into the metastore); on a
Format Table it only takes effect in a REST catalog, and elsewhere partitions are still discovered
from the filesystem.

**A Flink job reads only the partitions the catalog knows.** Directories written before the option
was enabled, by an older writer, or by anything that does not register what it wrote are invisible,
and a table whose catalog holds no partitions reads as empty. Flink has no SQL command to register
them: use Spark's `MSCK REPAIR TABLE` or the catalog's partition API. Flink writes on a current
version do register the partitions they produce.

Flink SQL cannot set a custom partition `LOCATION`. Upgrade Flink readers before registering custom
locations through the catalog API.

In a REST catalog, asking for catalog-managed partitions on a table that cannot have them — an
external table, or `format-table.implementation = engine` — fails. In any other catalog the option
keeps the meaning it has always had on a Format Table — none — and partitions come from the
filesystem. Removing the option needs a catalog that can alter the table; Flink's Format Table path
cannot, so use another engine.

:::
