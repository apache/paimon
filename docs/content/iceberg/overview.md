---
title: "Overview"
weight: 1
type: docs
aliases:
- /iceberg/overview.html
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

Paimon supports generating Iceberg compatible metadata,
so that Paimon tables can be consumed directly by Iceberg readers.

Set the following table options, so that Paimon tables can generate Iceberg compatible metadata.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 20%">Option</th>
      <th class="text-left" style="width: 5%">Default</th>
      <th class="text-left" style="width: 10%">Type</th>
      <th class="text-left" style="width: 60%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>metadata.iceberg.storage</h5></td>
      <td style="word-wrap: break-word;">disabled</td>
      <td>Enum</td>
      <td>
        When set, produce Iceberg metadata after a snapshot is committed, so that Iceberg readers can read Paimon's raw data files.
        <ul>
          <li><code>disabled</code>: Disable Iceberg compatibility support.</li>
          <li><code>table-location</code>: Store Iceberg metadata in each table's directory.</li>
          <li><code>hadoop-catalog</code>: Store Iceberg metadata in a separate directory. This directory can be specified as the warehouse directory of an Iceberg Hadoop catalog.</li>
          <li><code>hive-catalog</code>: Not only store Iceberg metadata like hadoop-catalog, but also create Iceberg external table in Hive.</li>
        </ul>
      </td>
    </tr>
    <tr>
      <td><h5>metadata.iceberg.storage-location</h5></td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Enum</td>
      <td>
        Specifies where to store Iceberg metadata files. If not set, the storage location will default based on the selected metadata.iceberg.storage type.
        <ul>
          <li><code>table-location</code>: Store Iceberg metadata in each table's directory. Useful for standalone Iceberg tables or Iceberg Java API access. Can also be used with Hive Catalog.</li>
          <li><code>catalog-location</code>: Store Iceberg metadata in a separate directory. This is the default behavior when using Hive Catalog or Hadoop Catalog.</li>
        </ul>
      </td>
    </tr>
    </tbody>
</table>

For most SQL users, we recommend setting `'metadata.iceberg.storage' = 'hadoop-catalog'
or `'metadata.iceberg.storage' = 'hive-catalog'`,
so that all tables can be visited as an Iceberg warehouse.
For Iceberg Java API users, you might consider setting `'metadata.iceberg.storage' = 'table-location'`,
so you can visit each table with its table path.
When using `metadata.iceberg.storage = hadoop-catalog` or `hive-catalog`,
you can optionally configure `metadata.iceberg.storage-location` to control where the metadata is stored.
If not set, the default behavior depends on the storage type.

## Supported Types

Paimon Iceberg compatibility currently supports the following data types.

| Paimon Data Type | Iceberg Data Type |
|----------------|-------------------|
| `BOOLEAN`      | `boolean`         |
| `INT`          | `int`             |
| `BIGINT`       | `long`            |
| `FLOAT`        | `float`           |
| `DOUBLE`       | `double`          |
| `DECIMAL`      | `decimal`         |
| `CHAR`         | `string`          |
| `VARCHAR`      | `string`          |
| `BINARY`       | `binary`          |
| `VARBINARY`    | `binary`          |
| `DATE`         | `date`            |
| `TIMESTAMP` (precision 3-6)   | `timestamp`       |
| `TIMESTAMP_LTZ` (precision 3-6) | `timestamptz`     |
| `TIMESTAMP` (precision 7-9)  | `timestamp_ns`    |
| `TIMESTAMP_LTZ` (precision 7-9) | `timestamptz_ns`  |
| `ARRAY`        | `list`            |
| `MAP`          | `map`             |
| `ROW`          | `struct`          |

{{< hint info >}}
**Note on Timestamp Types:**
- `TIMESTAMP` and `TIMESTAMP_LTZ` types with precision from 3 to 6 are mapped to standard Iceberg timestamp types
- `TIMESTAMP` and `TIMESTAMP_LTZ` types with precision from 7 to 9 use nanosecond precision and require Iceberg v3 format
{{< /hint >}}
