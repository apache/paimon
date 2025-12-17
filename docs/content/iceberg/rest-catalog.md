---
title: "Rest Catalog"
weight: 5
type: docs
aliases:
- /iceberg/rest-catalog.html
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

# Rest Catalog

When creating Paimon table, set `'metadata.iceberg.storage' = 'rest-catalog'`.
This option value will not only store Iceberg metadata like hadoop-catalog, but also create table in [iceberg rest catalog](https://iceberg.apache.org/terms/#decoupling-using-the-rest-catalog).
This Paimon table can be accessed from Iceberg Rest catalog later.

You need to provide information about Rest Catalog by setting options prefixed with `'metadata.iceberg.rest.'`, such as 
`'metadata.iceberg.rest.uri' = 'https://localhost/'`. Paimon will try to use these options to initialize an iceberg rest catalog, 
and use this rest catalog to commit metadata.

**Dependency:**

This feature needs dependency: [paimon-iceberg-{{< version >}}.jar](https://repository.apache.org/snapshots/org/apache/paimon/paimon-iceberg/{{< version >}}/), 
and JDK version should be 11+.

You can also manually build the jar from the source code.(need JDK 11+)

To build from source code, [clone the git repository]({{< github_repo >}}).

Build bundled jar with the following command.
- `mvn clean install -DskipTests`

You can find the jar in `./paimon-iceberg/target/paimon-iceberg-{{< version >}}.jar`.

**Example:**

Here is an example using flink sql:
```sql
-- create a paimon table
CREATE TABLE `paimon`.`default`.`T` (
  pt INT,
  k INT,
  v INT,
  PRIMARY KEY (pt, k) NOT ENFORCED
) PARTITIONED BY (pt) WITH (
  'metadata.iceberg.storage' = 'rest-catalog',
  'metadata.iceberg.rest.uri' = 'http://localhost:55807/',
  'metadata.iceberg.rest.warehouse' = 'rck_warehouse',
  'metadata.iceberg.rest.clients' = '1'
);

-- insert some data
INSERT INTO `paimon`.`default`.`T` VALUES(1, 9, 90),(1, 10, 100),(1, 11, 110),(2, 20, 200);

-- create an iceberg rest catalog
CREATE CATALOG `iceberg` WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://localhost:55807/',
  'clients' = '1',
  'cache-enabled' = 'false'
)

-- verify the data in iceberg rest-catalog
SELECT v, k, pt FROM `iceberg`.`default`.`T` ORDER BY pt, k;
/*
the query results:
 90,  9, 1
100, 10, 1
110, 11, 1
200, 20, 2
*/
```
**Note:**

Paimon will firstly write iceberg metadata in a separate directory like hadoop-catalog, and then commit metadata to iceberg rest catalog.
If the two are incompatible, we take the metadata stored in the separate directory as the reference.

There are some cases when committing to iceberg rest catalog:
1. table not exists in iceberg rest-catalog. It'll create the table in rest catalog first, and commit metadata.
2. table exists in iceberg rest-catalog and is compatible with the base metadata stored in the separate directory. It'll directly get the table and commit metadata. 
3. table exists, and isn't compatible with the base metadata stored in the separate directory. It'll **drop the table and recreate the table**, then commit metadata. 

