---
title: "Roadmap"
weight: 1
type: docs
aliases:
- /project/roadmap.html
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

# Roadmap

## Native Format IO

Integrate native ORC & Parquet reader & writer.

## Deletion Vectors (Merge On Write)

1. Primary Key Table Deletion Vectors Mode supports async compaction.
2. Append Table supports DELETE & UPDATE with Deletion Vectors Mode. (Now only Spark SQL)
3. Optimize lookup performance for HDD disk.

## Flink Lookup Join

Support Flink Custom Data Distribution Lookup Join to reach large-scale data lookup join.

## Produce Iceberg snapshots

Introduce a mode to produce Iceberg snapshots.

## Branch

Branch production ready.

## Changelog life cycle decouple

Changelog life cycle decouple supports none changelog-producer.

## Partition Mark Done

Support partition mark done.

## Default File Format

- Default compression is ZSTD with level 1.
- Parquet supports filter push down.
- Parquet supports arrow with row type element.
- Parquet becomes default file format.

## Variant Type

Support Variant Type with Spark 4.0 and Flink 2.0. Unlocking support for semi-structured data.

## Bucketed Join

Support Bucketed Join with Spark SQL to reduce shuffler in Join.

## File Index

Add more index:
1. Bitmap
2. Inverse

## Column Family

Support Column Family for super Wide Table.

## View & Function support

Paimon Catalog supports views and functions.

## Files Schema Evolution Ingestion

Introduce a files Ingestion with Schema Evolution.

## Foreign Key Join

Explore Foreign Key Join solution.
