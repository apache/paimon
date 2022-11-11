---
title: "Overview"
weight: 1
type: docs
aliases:
- /filesystem/overview.html
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

# Overview

# File Systems for Unified Engine

Apache Flink Table Store utilizes the same pluggable file systems as Apache Flink. Users can follow the [standard plugin mechanism](https://nightlies.apache.org/flink/flink-docs-stable/docs/deployment/filesystems/plugins/) to configure the
plugin structure if using Flink as compute engine. However, for other engines like Spark or Hive, the provided opt jars (by Flink) may get conflicts and cannot be used directly.
It is not convenient for users to fix class conflicts, thus Flink Table Store provides the self-contained and engine-unified FileSystem pluggable jars for user
to query tables from Spark/Hive side.

## Supported FileSystem

| FileSystem        | URI Scheme       | Pluggable | Description                     |
|:------------------|:-----------------|-----------|:--------------------------------|
| Local File System | file://          | N         | Built-in Support                |
| Aliyun OSS        | oss://           | Y         | Tested on Spark3.3 and Hive 3.1 |

## Build
After [Build Flink Table Store]({{< ref "docs/engines/build" >}}) from the source code, you can find the shaded jars under
`./flink-table-store-filesystem/flink-table-store-filesystem-${filesystem}/target/flink-table-store-filesystem-${filesystem}-{{< version >}}.jar`.


## Common Configurations
After building, users need pick the required file system jar, and configure the required file system parameters by adding a command/configuration prefix `tablestore`.

For example, if users want set up a Flink job and use OSS as the underlay file system, and want to read from Spark/Hive side.

- On Flink side, configure `flink-conf.yaml` like
    ```yaml
    fs.oss.endpoint: oss-cn-hangzhou.aliyun.cs.com
    fs.oss.accessKey: xxx
    fs.oss.accessSecret: yyy
    ```

- On Spark side, place `flink-table-store-filesystem-oss-{{< version >}}.jar` together with `flink-table-store-spark-{{< version >}}.jar` under Spark's jars directory, and start like
  - Spark Shell
    ```shell
    spark-shell \ 
      --conf spark.datasource.tablestore.fs.oss.endpoint=oss-cn-hangzhou.aliyun.cs.com \
      --conf spark.datasource.tablestore.fs.oss.accessKey=xxx \
      --conf spark.datasource.tablestore.fs.oss.accessSecret=yyy
    ```
  - Spark SQL
  
    ```shell
    spark-sql \ 
      --conf spark.sql.catalog.tablestore=org.apache.flink.table.store.spark.SparkCatalog \
      --conf spark.sql.catalog.tablestore.warehouse=oss://<bucket-name>/ \
      --conf spark.sql.catalog.tablestore.fs.oss.endpoint=oss-cn-hangzhou.aliyun.cs.com \
      --conf spark.sql.catalog.tablestore.fs.oss.accessKey=xxx \
      --conf spark.sql.catalog.tablestore.fs.oss.accessSecret=yyy
    ```
- On Hive side, place `flink-table-store-filesystem-oss-{{< version >}}.jar` together with `flink-table-store-hive-connector-{{< version >}}.jar` under Hive's auxlib directory, and start like
  - Hive Catalog
    ```sql
    SET tablestore.fs.oss.endpoint=oss-cn-hangzhou.aliyun.cs.com;
    SET tablestore.fs.oss.accessKey=xxx;
    SET tablestore.fs.oss.accessSecret=yyy;

    CREATE EXTERNAL TABLE external_test_table
    STORED BY 'org.apache.flink.table.store.hive.TableStoreHiveStorageHandler'
    LOCATION 'oss://<bucket-name>/<db-name>.db/<table-name>';
    ```



