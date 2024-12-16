---
title: "SQL DDL"
weight: 2
type: docs
aliases:
- /flink/sql-ddl.html
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

## Create Catalog

Paimon catalogs currently support three types of metastores:

* `filesystem` metastore (default), which stores both metadata and table files in filesystems.
* `hive` metastore, which additionally stores metadata in Hive metastore. Users can directly access the tables from Hive.
* `jdbc` metastore, which additionally stores metadata in relational databases such as MySQL, Postgres, etc.

See [CatalogOptions]({{< ref "maintenance/configurations#catalogoptions" >}}) for detailed options when creating a catalog.

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

Also, you can create [FlinkGenericCatalog]({{< ref "flink/quick-start" >}}).

> When using hive catalog to change incompatible column types through alter table, you need to configure `hive.metastore.disallow.incompatible.col.type.changes=false`. see [HIVE-17832](https://issues.apache.org/jira/browse/HIVE-17832).

> If you are using Hive3, please disable Hive ACID:
>
> ```shell
> hive.strict.managed.tables=false
> hive.create.as.insert.only=false
> metastore.create.as.acid=false
> ```

#### Synchronizing Partitions into Hive Metastore

By default, Paimon does not synchronize newly created partitions into Hive metastore. Users will see an unpartitioned table in Hive. Partition push-down will be carried out by filter push-down instead.

If you want to see a partitioned table in Hive and also synchronize newly created partitions into Hive metastore, please set the table property `metastore.partitioned-table` to true. Also see [CoreOptions]({{< ref "maintenance/configurations#coreoptions" >}}).

#### Adding Parameters to a Hive Table

Using the table option facilitates the convenient definition of Hive table parameters. 
Parameters prefixed with `hive.` will be automatically defined in the `TBLPROPERTIES` of the Hive table. 
For instance, using the option `hive.table.owner=Jon` will automatically add the parameter `table.owner=Jon` to the table properties during the creation process.

#### Setting Location in Properties

If you are using an object storage , and you don't want that the location of paimon table/database is accessed by the filesystem of hive,
which may lead to the error such as "No FileSystem for scheme: s3a".
You can set location in the properties of table/database by the config of `location-in-properties`. See
[setting the location of table/database in properties ]({{< ref "maintenance/configurations#hivecatalogoptions" >}})

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

You can define any default table options with the prefix `table-default.` for tables created in the catalog.

## Create Table

After use Paimon catalog, you can create and drop tables. Tables created in Paimon Catalogs are managed by the catalog.
When the table is dropped from catalog, its table files will also be deleted.

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

You can create partitioned table:

```sql
CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING,
    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED
) PARTITIONED BY (dt, hh);
```

{{< hint info >}}
If you need cross partition upsert (primary keys not contain all partition fields), see [Cross partition Upsert]({{< ref "primary-key-table/data-distribution#cross-partitions-upsert-dynamic-bucket-mode">}}) mode.
{{< /hint >}}

{{< hint info >}}
By configuring [partition.expiration-time]({{< ref "flink/expire-partition" >}}), expired partitions can be automatically deleted.
{{< /hint >}}

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

Paimon table currently supports setting default values for fields in table properties by `'fields.item_id.default-value'`,
note that partition fields and primary key fields can not be specified.

## Create Table As Select

Table can be created and populated by the results of a query, for example, we have a sql like this: `CREATE TABLE table_b AS SELECT id, name FORM table_a`,
The resulting table `table_b` will be equivalent to create the table and insert the data with the following statement:
`CREATE TABLE table_b (id INT, name STRING); INSERT INTO table_b SELECT id, name FROM table_a;`

We can specify the primary key or partition when use `CREATE TABLE AS SELECT`, for syntax, please refer to the following sql.

```sql
/* For streaming mode, you need to enable the checkpoint. */

CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT
);
CREATE TABLE my_table_as AS SELECT * FROM my_table;

/* partitioned table */
CREATE TABLE my_table_partition (
     user_id BIGINT,
     item_id BIGINT,
     behavior STRING,
     dt STRING,
     hh STRING
) PARTITIONED BY (dt, hh);
CREATE TABLE my_table_partition_as WITH ('partition' = 'dt') AS SELECT * FROM my_table_partition;
    
/* change options */
CREATE TABLE my_table_options (
       user_id BIGINT,
       item_id BIGINT
) WITH ('file.format' = 'orc');
CREATE TABLE my_table_options_as WITH ('file.format' = 'parquet') AS SELECT * FROM my_table_options;

/* primary key */
CREATE TABLE my_table_pk (
      user_id BIGINT,
      item_id BIGINT,
      behavior STRING,
      dt STRING,
      hh STRING,
      PRIMARY KEY (dt, hh, user_id) NOT ENFORCED
);
CREATE TABLE my_table_pk_as WITH ('primary-key' = 'dt,hh') AS SELECT * FROM my_table_pk;


/* primary key + partition */
CREATE TABLE my_table_all (
      user_id BIGINT,
      item_id BIGINT,
      behavior STRING,
      dt STRING,
      hh STRING,
      PRIMARY KEY (dt, hh, user_id) NOT ENFORCED 
) PARTITIONED BY (dt, hh);
CREATE TABLE my_table_all_as WITH ('primary-key' = 'dt,hh', 'partition' = 'dt') AS SELECT * FROM my_table_all;
```

## Create Table Like

To create a table with the same schema, partition, and table properties as another table, use CREATE TABLE LIKE.

```sql
CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING,
    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED
);

CREATE TABLE my_table_like LIKE my_table (EXCLUDING OPTIONS);
```

## Work with Flink Temporary Tables

Flink Temporary tables are just recorded but not managed by the current Flink SQL session. If the temporary table is
dropped, its resources will not be deleted. Temporary tables are also dropped when Flink SQL session is closed.

If you want to use Paimon catalog along with other tables but do not want to store them in other catalogs, you can
create a temporary table. The following Flink SQL creates a Paimon catalog and a temporary table and also illustrates
how to use both tables together.

```sql
CREATE CATALOG my_catalog WITH (
    'type' = 'paimon',
    'warehouse' = 'hdfs:///path/to/warehouse'
);

USE CATALOG my_catalog;

-- Assume that there is already a table named my_table in my_catalog

CREATE TEMPORARY TABLE temp_table (
    k INT,
    v STRING
) WITH (
    'connector' = 'filesystem',
    'path' = 'hdfs:///path/to/temp_table.csv',
    'format' = 'csv'
);

SELECT my_table.k, my_table.v, temp_table.v FROM my_table JOIN temp_table ON my_table.k = temp_table.k;
```
