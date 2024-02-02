---
title: "Creating Tables"
weight: 2
type: docs
aliases:
- /how-to/creating-tables.html
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

# Creating Tables

## Creating Catalog Managed Tables

Tables created in Paimon [catalogs]({{< ref "how-to/creating-catalogs" >}}) are managed by the catalog. When the table is dropped from catalog, its table files will also be deleted.

The following SQL assumes that you have registered and are using a Paimon catalog. It creates a managed table named `MyTable` with five columns in the catalog's `default` database, where `dt`, `hh` and `user_id` are the primary keys.

{{< tabs "primary-keys-example" >}}

{{< tab "Flink" >}}

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

{{< /tab >}}

{{< tab "Spark3" >}}

```sql
CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING
) TBLPROPERTIES (
    'primary-key' = 'dt,hh,user_id'
);
```

{{< /tab >}}

{{< tab "Hive" >}}

```sql
SET hive.metastore.warehouse.dir=warehouse_path;

CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING
)
STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'
TBLPROPERTIES (
    'primary-key' = 'dt,hh,user_id'
);
```

{{< /tab >}}

{{< tab "Trino" >}}

```sql
CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT,
    behavior VARCHAR,
    dt VARCHAR,
    hh VARCHAR
) WITH (
    primary_key = ARRAY['dt', 'hh', 'user_id']
);
```

{{< /tab >}}

{{< tab "Presto" >}}

```sql
CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT,
    behavior VARCHAR,
    dt VARCHAR,
    hh VARCHAR
) WITH (
    primary_key = ARRAY['dt', 'hh', 'user_id']
);
```

{{< /tab >}}

{{< /tabs >}}

{{< hint info >}}
Inserting jobs on the table should be stopped prior to dropping tables, or table files couldn't be deleted completely.
{{< /hint >}}

### Partitioned Tables

The following SQL creates a table named `MyTable` with five columns partitioned by `dt` and `hh`, where `dt`, `hh` and `user_id` are the primary keys.

{{< tabs "partitions-example" >}}

{{< tab "Flink" >}}

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

{{< /tab >}}

{{< tab "Spark3" >}}

```sql
CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING
) PARTITIONED BY (dt, hh) TBLPROPERTIES (
    'primary-key' = 'dt,hh,user_id'
);
```

{{< /tab >}}

{{< tab "Hive" >}}

```sql
SET hive.metastore.warehouse.dir=warehouse_path;

CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING
) PARTITIONED BY ( 
    dt STRING,
    hh STRING
)
STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'
TBLPROPERTIES (
    'primary-key' = 'dt,hh,user_id'
);
```

{{< /tab >}}

{{< tab "Trino" >}}

```sql
CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT,
    behavior VARCHAR,
    dt VARCHAR,
    hh VARCHAR
) WITH (
    primary_key = ARRAY['dt', 'hh', 'user_id'],
    partitioned_by = ARRAY['dt', 'hh']
);
```

{{< /tab >}}

{{< tab "Presto" >}}

```sql
CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT,
    behavior VARCHAR,
    dt VARCHAR,
    hh VARCHAR
) WITH (
    primary_key = ARRAY['dt', 'hh', 'user_id'],
    partitioned_by = ARRAY['dt', 'hh']
);
```

{{< /tab >}}

{{< /tabs >}}

{{< hint info >}}
By configuring [partition.expiration-time]({{< ref "maintenance/manage-partition" >}}), expired partitions can be automatically deleted.
{{< /hint >}}

#### Pick Partition Fields

{{< hint info >}}
If you need cross partition upsert (primary keys not contain all partition fields), see [Cross partition Upsert]({{< ref "concepts/primary-key-table/data-distribution#cross-partitions-upsert-dynamic-bucket-mode">}}) mode.
{{< /hint >}}

The following three types of fields may be defined as partition fields in the warehouse:
- Creation Time (Recommended): The creation time is generally immutable, so you can confidently treat it as a partition field
  and add it to the primary key.
- Event Time: Event time is a field in the original table. For CDC data, such as tables synchronized from MySQL
  CDC or Changelogs generated by Paimon, they are all complete CDC data, including `UPDATE_BEFORE` records, even
  if you declare the primary key containing partition field, you can achieve the unique effect (require `'changelog-producer'='input'`).
- CDC op_ts: It cannot be defined as a partition field, unable to know previous record timestamp.

### Specify Statistics Mode

Paimon will automatically collect the statistics of the data file for speeding up the query process. There are four modes supported:

- `full`: collect the full metrics: `null_count, min, max` .
- `truncate(length)`: length can be any positive number, the default mode is `truncate(16)`, which means collect the null count, min/max value with truncated length of 16.
  This is mainly to avoid too big column which will enlarge the manifest file.
- `counts`: only collect the null count.
- `none`: disable the metadata stats collection.

The statistics collector mode can be configured by `'metadata.stats-mode'`, by default is `'truncate(16)'`.
You can configure the field level by setting `'fields.{field_name}.stats-mode'`.


### Field Default Value

Paimon table currently supports setting default values for fields in table properties,
note that partition fields and primary key fields can not be specified.
{{< tabs "default-value-example" >}}

{{< tab "Flink" >}}

```sql
CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING,
    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED
) PARTITIONED BY (dt, hh)
with(
    'fields.item_id.default-value'='0'
);
```

{{< /tab >}}

{{< tab "Spark3" >}}

```sql
CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING
) PARTITIONED BY (dt, hh) TBLPROPERTIES (
    'primary-key' = 'dt,hh,user_id',
    'fields.item_id.default-value'='0'
);
```

{{< /tab >}}

{{< tab "Hive" >}}

```sql
SET hive.metastore.warehouse.dir=warehouse_path;

CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING
)
STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'
TBLPROPERTIES (
    'primary-key' = 'dt,hh,user_id',
    'partition'='dt,hh',
    'fields.item_id.default-value'='0'
);
```

{{< /tab >}}

{{< /tabs >}}


## Create Table As

Table can be created and populated by the results of a query, for example, we have a sql like this: `CREATE TABLE table_b AS SELECT id, name FORM table_a`,
The resulting table `table_b` will be equivalent to create the table and insert the data with the following statement:
`CREATE TABLE table_b (id INT, name STRING); INSERT INTO table_b SELECT id, name FROM table_a;`

We can specify the primary key or partition when use `CREATE TABLE AS SELECT`, for syntax, please refer to the following sql.

{{< tabs "create-table-as" >}}

{{< tab "Flink" >}}

```sql

/* For streaming mode, you need to enable the checkpoint. */

CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT
);
CREATE TABLE MyTableAs AS SELECT * FROM MyTable;

/* partitioned table */
CREATE TABLE my_table_partition (
     user_id BIGINT,
     item_id BIGINT,
     behavior STRING,
     dt STRING,
     hh STRING
) PARTITIONED BY (dt, hh);
CREATE TABLE my_table_partition As WITH ('partition' = 'dt') AS SELECT * FROM my_table_partition;
    
/* change options */
CREATE TABLE my_table_options (
       user_id BIGINT,
       item_id BIGINT
) WITH ('file.format' = 'orc');
CREATE TABLE my_table_options As WITH ('file.format' = 'parquet') AS SELECT * FROM my_table_options;

/* primary key */
CREATE TABLE my_table_pk (
      user_id BIGINT,
      item_id BIGINT,
      behavior STRING,
      dt STRING,
      hh STRING,
      PRIMARY KEY (dt, hh, user_id) NOT ENFORCED
);
CREATE TABLE my_table_pk As WITH ('primary-key' = 'dt,hh') AS SELECT * FROM my_table_pk;


/* primary key + partition */
CREATE TABLE my_table_all (
      user_id BIGINT,
      item_id BIGINT,
      behavior STRING,
      dt STRING,
      hh STRING,
      PRIMARY KEY (dt, hh, user_id) NOT ENFORCED 
) PARTITIONED BY (dt, hh);
CREATE TABLE my_table_all As WITH ('primary-key' = 'dt,hh', 'partition' = 'dt') AS SELECT * FROM my_table_all;
```

{{< /tab >}}

{{< tab "Spark3" >}}

```sql
CREATE TABLE my_table (
     user_id BIGINT,
     item_id BIGINT
);
CREATE TABLE my_table As AS SELECT * FROM my_table;

/* partitioned table*/
CREATE TABLE my_table_partition (
      user_id BIGINT,
      item_id BIGINT,
      behavior STRING,
      dt STRING,
      hh STRING
) PARTITIONED BY (dt, hh);
CREATE TABLE my_table_partition As PARTITIONED BY (dt) AS SELECT * FROM my_table_partition;

/* change TBLPROPERTIES */
CREATE TABLE my_table_options (
       user_id BIGINT,
       item_id BIGINT
) TBLPROPERTIES ('file.format' = 'orc');
CREATE TABLE my_table_options As TBLPROPERTIES ('file.format' = 'parquet') AS SELECT * FROM my_table_options;


/* primary key */
CREATE TABLE my_table_pk (
     user_id BIGINT,
     item_id BIGINT,
     behavior STRING,
     dt STRING,
     hh STRING
) TBLPROPERTIES (
    'primary-key' = 'dt,hh,user_id'
);
CREATE TABLE my_table_pk As TBLPROPERTIES ('primary-key' = 'dt') AS SELECT * FROM my_table_pk;

/* primary key + partition */
CREATE TABLE my_table_all (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING
) PARTITIONED BY (dt, hh) TBLPROPERTIES (
    'primary-key' = 'dt,hh,user_id'
);
CREATE TABLE my_table_all As PARTITIONED BY (dt) TBLPROPERTIES ('primary-key' = 'dt,hh') AS SELECT * FROM my_table_all;
```

{{< /tab >}}

{{< /tabs >}}


### Create Table Like

{{< tabs "create-table-like" >}}

{{< tab "Flink" >}}

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

CREATE TABLE my_table_like LIKE my_table;

-- Create Paimon Table like other connector table
CREATE TABLE my_table_like WITH ('connector' = 'paimon') LIKE my_table;
```

{{< /tab >}}

{{< /tabs >}}

### Table Properties

Users can specify table properties to enable features or improve performance of Paimon. For a complete list of such properties, see [configurations]({{< ref "maintenance/configurations" >}}).

The following SQL creates a table named `MyTable` with five columns partitioned by `dt` and `hh`, where `dt`, `hh` and `user_id` are the primary keys. This table has two properties: `'bucket' = '2'` and `'bucket-key' = 'user_id'`.

{{< tabs "table-properties-example" >}}

{{< tab "Flink" >}}

```sql
CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING,
    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED
) PARTITIONED BY (dt, hh) WITH (
    'bucket' = '2',
    'bucket-key' = 'user_id'
);
```

{{< /tab >}}

{{< tab "Spark3" >}}

```sql
CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING
) PARTITIONED BY (dt, hh) TBLPROPERTIES (
    'primary-key' = 'dt,hh,user_id',
    'bucket' = '2',
    'bucket-key' = 'user_id'
);
```

{{< /tab >}}

{{< tab "Hive" >}}

```sql
CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING
)
STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'
TBLPROPERTIES (
    'primary-key' = 'dt,hh,user_id',
    'partition'='dt,hh',
    'bucket' = '2',
    'bucket-key' = 'user_id'
);
```

{{< /tab >}}

{{< tab "Trino" >}}

```sql
CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT,
    behavior VARCHAR,
    dt VARCHAR,
    hh VARCHAR
) WITH (
    primary_key = ARRAY['dt', 'hh', 'user_id'],
    partitioned_by = ARRAY['dt', 'hh'],
    bucket = '2',
    bucket_key = 'user_id'
);
```

{{< /tab >}}

{{< tab "Presto" >}}

```sql
CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT,
    behavior VARCHAR,
    dt VARCHAR,
    hh VARCHAR
) WITH (
    primary_key = ARRAY['dt', 'hh', 'user_id'],
    partitioned_by = ARRAY['dt', 'hh'],
    bucket = '2',
    bucket_key = 'user_id'
);
```

{{< /tab >}}

{{< /tabs >}}

## Creating External Tables

{{< hint info >}}
If the table already exists, options will not be updated into the table's metadata, just as dynamic options.
{{< /hint >}}

External tables are recorded but not managed by catalogs. If an external table is dropped, its table files will not be deleted.

Paimon external tables can be used in any catalog. If you do not want to create a Paimon catalog and just want to read / write a table, you can consider external tables.

{{< tabs "external-table-example" >}}

{{< tab "Flink (Deprecated)" >}}

{{< hint info >}}
Please **DO NOT** use this mode. We recommend using the Paimon Catalog way. The current reservation is only for compatibility.
{{< /hint >}}

Flink SQL supports reading and writing an external table. External Paimon tables are created by specifying the `connector` and `path` table properties. The following SQL creates an external table named `MyTable` with five columns, where the base path of table files is `hdfs:///path/to/table`.

```sql
CREATE TABLE my_table (
    user_id BIGINT,
    item_id BIGINT,
    behavior STRING,
    dt STRING,
    hh STRING,
    PRIMARY KEY (dt, hh, user_id) NOT ENFORCED
) WITH (
    'connector' = 'paimon',
    'path' = 'hdfs:///path/to/table',
    'auto-create' = 'true' -- this table property creates table files for an empty table if table path does not exist
                           -- currently only supported by Flink
);
```

(Flink SQL must declare all fields, which is difficult to use. And if the table already exists, type nullable should be checked.)

{{< /tab >}}

{{< tab "Spark3" >}}

Spark3 only supports creating external tables through Scala API. The following Scala code loads the table located at `hdfs:///path/to/table` into a `DataSet`.

```scala
val dataset = spark.read.format("paimon").load("hdfs:///path/to/table")
```

{{< /tab >}}

{{< tab "Spark2" >}}

Spark2 only supports creating external tables through Scala API. The following Scala code loads the table located at `hdfs:///path/to/table` into a `DataSet`.

```scala
val dataset = spark.read.format("paimon").load("hdfs:///path/to/table")
```

{{< /tab >}}

{{< tab "Hive" >}}

To access existing paimon table, you can also register them as external tables in Hive. The following SQL creates an
external table named `my_table`, where the base path of table files is `hdfs:///path/to/table`. As schemas are stored
in table files, users do not need to write column definitions.

```sql
CREATE EXTERNAL TABLE my_table
STORED BY 'org.apache.paimon.hive.PaimonStorageHandler'
LOCATION 'hdfs:///path/to/table';
```

{{< /tab >}}

{{< /tabs >}}

## Creating Temporary Tables

{{< tabs "temporary-table-example" >}}

{{< tab "Flink" >}}

Temporary tables are only supported by Flink. Like external tables, temporary tables are just recorded but not managed by the current Flink SQL session. If the temporary table is dropped, its resources will not be deleted. Temporary tables are also dropped when Flink SQL session is closed.

If you want to use Paimon catalog along with other tables but do not want to store them in other catalogs, you can create a temporary table. The following Flink SQL creates a Paimon catalog and a temporary table and also illustrates how to use both tables together.

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

{{< /tab >}}

{{< /tabs >}}
