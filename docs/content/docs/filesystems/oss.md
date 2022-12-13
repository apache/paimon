---
title: "OSS"
weight: 2
type: docs
aliases:
- /filesystems/oss.html
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

# OSS

## Build

[Build Flink Table Store]({{< ref "docs/engines/build" >}}), you can find the shaded jars under
`./flink-table-store-filesystems/flink-table-store-oss/target/flink-table-store-oss-{{< version >}}.jar`.

## Use

- On Flink side, configure `flink-conf.yaml` like
    ```yaml
    fs.oss.endpoint: oss-cn-hangzhou.aliyun.cs.com
    fs.oss.accessKey: xxx
    fs.oss.accessSecret: yyy
    ```

- On Spark side, place `flink-table-store-oss-{{< version >}}.jar` together with `flink-table-store-spark-{{< version >}}.jar` under Spark's jars directory, and start like
  - Spark SQL
    ```shell
    spark-sql \ 
      --conf spark.sql.catalog.tablestore=org.apache.flink.table.store.spark.SparkCatalog \
      --conf spark.sql.catalog.tablestore.warehouse=oss://<bucket-name>/ \
      --conf spark.sql.catalog.tablestore.fs.oss.endpoint=oss-cn-hangzhou.aliyun.cs.com \
      --conf spark.sql.catalog.tablestore.fs.oss.accessKey=xxx \
      --conf spark.sql.catalog.tablestore.fs.oss.accessSecret=yyy
    ```
- On Hive side, place `flink-table-store-oss-{{< version >}}.jar` together with `flink-table-store-hive-connector-{{< version >}}.jar` under Hive's auxlib directory, and start like
  - Hive Catalog
    ```sql
    SET tablestore.fs.oss.endpoint=oss-cn-hangzhou.aliyun.cs.com;
    SET tablestore.fs.oss.accessKey=xxx;
    SET tablestore.fs.oss.accessSecret=yyy;

    CREATE EXTERNAL TABLE external_test_table
    STORED BY 'org.apache.flink.table.store.hive.TableStoreHiveStorageHandler'
    LOCATION 'oss://<bucket-name>/<db-name>.db/<table-name>';
    ```



