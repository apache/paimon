---
title: "Configurations"
weight: 100
type: docs
aliases:
- /maintenance/configurations.html
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

# Configuration

### CoreOptions

Core options for paimon.

{{< generated/core_configuration >}}

### CatalogOptions

Options for paimon catalog.

{{< generated/catalog_configuration >}}

### HiveCatalogOptions

Options for Hive catalog.

{{< generated/hive_catalog_configuration >}}

### JdbcCatalogOptions

Options for Jdbc catalog.

{{< generated/jdbc_catalog_configuration >}}

### FlinkCatalogOptions

Flink catalog options for paimon.

{{< generated/flink_catalog_configuration >}}

### FlinkConnectorOptions

Flink connector options for paimon.

{{< generated/flink_connector_configuration >}}

### SparkCatalogOptions

Spark catalog options for paimon.

{{< generated/spark_catalog_configuration >}}

### SparkConnectorOptions

Spark connector options for paimon.

{{< generated/spark_connector_configuration >}}

### ORC Options

{{< generated/orc_configuration >}}

### RocksDB Options

The following options allow users to finely adjust RocksDB for better performance. You can either specify them in table properties or in dynamic table hints.

{{< generated/rocksdb_configuration >}}
