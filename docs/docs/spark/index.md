---
title: "Spark"
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

# Spark

Use Spark SQL, the DataFrame API, or Structured Streaming to work with Paimon tables.
Start with a local example, configure your catalog, then choose the guide for the operation
you need.

![Spark SQL, DataFrames, and Structured Streaming access Paimon through the connector and catalog, with table data stored in the warehouse.](/img/spark-integration.svg)

## Start Here

1. [Quick Start](./quick-start): create a table, write an update, and read the result locally.
2. [Installation](./installation): match the Spark and Scala versions and install the connector.
3. [Catalogs](./catalogs): connect to a filesystem, Hive, JDBC, or REST catalog.
4. [Configuration](./configuration): distinguish catalog, table, session, and operation options.

## Choose a Guide

| Task | Read next |
| --- | --- |
| Create a table, view, or tag | [SQL DDL](./sql-ddl) |
| Change a schema or table property | [Alter Tables](./sql-alter) |
| Work with catalog-managed Format Table partitions | [Format Table Partitions](./format-table) |
| Query current state, time travel, or bounded changes | [SQL Queries](./sql-query) |
| Insert, overwrite, update, delete, or merge rows | [SQL Writes](./sql-write) |
| Import or export CSV, JSON, or Parquet | [COPY INTO](./copy-into) |
| Work from Scala | [DataFrame API](./dataframe) |
| Add columns during writes | [Schema Evolution on Write](./schema-evolution) |
| Build a streaming job | [Structured Streaming](./structured-streaming) |
| Recover a stream and retain unread data | [Streaming Recovery](./streaming-recovery) |
| Compact, expire, migrate, or build indexes | [Procedures](./procedures) |

## Reference

- [Data Types](./data-types): type mappings and version-specific timestamp and geospatial behavior.
- [Default Values](./default-value): defaults for new writes and how to change them.
- [SQL Functions](./sql-functions): partition helpers, blob functions, and user-defined functions.
- [Inspect and Maintain Tables](./auxiliary): schemas, partitions, statistics, and metadata refresh.

Feature requirements are called out on each page. In particular, SQL time travel and streaming
reads require Spark 3.3+, `INSERT ... BY NAME` requires Spark 3.5+, and SQL-defined scalar
functions require Spark 4.0+.
