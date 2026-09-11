---
title: "Hive Catalog"
sidebar_position: 5
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

# Hive Catalog

Use `hive-catalog` to publish an Iceberg table in a Hive metastore. Paimon writes Iceberg metadata
and updates the metastore entry to point to it. Iceberg readers then discover the table through
their own Hive catalog connector.

## Before You Begin

Prepare the Paimon and Iceberg engine connectors as described in the
[append-table walkthrough](./append-table.mdx#before-you-begin). The Paimon writer also needs the
Paimon Hive catalog module and its Hive dependencies; see [Hive catalog setup](../concepts/catalog.md#hive-catalog).
Both the writer and reader need access to the metastore and the files referenced by the table.

## Publish a Table

The following Flink SQL uses a filesystem Paimon catalog and registers the Iceberg representation
in Hive. Replace the warehouse and metastore placeholders.

```sql
SET 'execution.runtime-mode' = 'batch';
SET 'table.dml-sync' = 'true';

CREATE CATALOG paimon_catalog WITH (
    'type' = 'paimon',
    'warehouse' = '<path-to-warehouse>'
);

CREATE DATABASE IF NOT EXISTS paimon_catalog.`default`;

CREATE TABLE paimon_catalog.`default`.animals (
    kind STRING,
    name STRING
) WITH (
    'metadata.iceberg.storage' = 'hive-catalog',
    'metadata.iceberg.uri' = 'thrift://<metastore-host>:9083'
);

INSERT INTO paimon_catalog.`default`.animals VALUES
    ('mammal', 'cat'), ('mammal', 'dog'),
    ('reptile', 'snake'), ('reptile', 'lizard');
```

For Spark, use the same table options in `TBLPROPERTIES`; the
[append-table walkthrough](./append-table.mdx) shows the corresponding syntax.

## Read through Iceberg

In Flink, connect an Iceberg Hive catalog to the same metastore:

```sql
CREATE CATALOG iceberg_hive WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hive',
    'uri' = 'thrift://<metastore-host>:9083',
    'cache-enabled' = 'false'
);

SELECT kind, name FROM iceberg_hive.`default`.animals
WHERE kind = 'mammal' ORDER BY name;
```

```text
kind    name
mammal  cat
mammal  dog
```

See [Trino](./ecosystem.mdx#trino-iceberg) for a reader that uses the same Hive registration.
For primary key tables, the [compaction or deletion-vector requirements](./primary-key-table.mdx)
also apply.

## Table Names and Metadata Location

By default, the Iceberg database and table names match the Paimon names. When the Paimon catalog
also uses the same Hive metastore, give the Iceberg representation a distinct name to avoid
reusing the Paimon table's metastore entry:

```sql
'metadata.iceberg.database' = 'iceberg_analytics',
'metadata.iceberg.table' = 'animals'
```

Readers would then query `iceberg_hive.iceberg_analytics.animals`. For Hive publication,
`metadata.iceberg.database` also accepts multiple databases separated by semicolons.
Aliases change catalog registration names, not the physical metadata path.

Metadata is stored under `<warehouse>/iceberg/<database>/<table>/metadata` by default.
Use `metadata.iceberg.storage-location = table-location` for metadata alongside the Paimon table,
including databases with custom locations. See [metadata layout](./catalogs.md#metadata-layout).

## Connection Options

| Paimon table option | When to use it |
| --- | --- |
| `metadata.iceberg.uri` | Set the Hive metastore Thrift URI explicitly |
| `metadata.iceberg.hive-conf-dir` | Load Hive configuration such as `hive-site.xml` |
| `metadata.iceberg.hadoop-conf-dir` | Load Hadoop configuration needed by the Hive client |
| `metadata.iceberg.hive-client-class` | Use a custom Hive metastore client |
| `metadata.iceberg.hive-skip-update-stats` | Skip updating Hive statistics |

If the URI is not set as a table option, provide `hive.metastore.uris` in the loaded configuration.
See [configuration reference](./configurations.mdx) for defaults and other publication options.

## AWS Glue Catalog

To publish through a Hive-compatible AWS Glue client, set:

```sql
'metadata.iceberg.hive-client-class' = 'com.amazonaws.glue.catalog.metastore.AWSCatalogMetastoreClient'
```

Install a Glue Hive client compatible with your Hive dependencies on the writer classpath, and
configure its AWS region and credentials. The [AWS Glue Data Catalog client](https://github.com/awslabs/aws-glue-data-catalog-client-for-apache-hive-metastore)
provides build and configuration instructions. `metadata.iceberg.glue.skip-archive` controls
whether publication skips archiving Glue table versions.

Reader support has separate constraints; see [Athena](./ecosystem.mdx#aws-athena).
