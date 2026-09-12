---
title: "Tags"
sidebar_position: 7
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

# Iceberg Tags

A Paimon tag can be recorded as a named snapshot reference in the generated Iceberg metadata.
This allows Iceberg readers to query that historical state by name.

## Publication Requirements

The tagged snapshot must already exist in the current Iceberg metadata. If it is absent, Paimon
skips adding the Iceberg reference. For a primary key table, tagging a Paimon snapshot does not
make unpublished changes visible; the [read-mode requirements](./primary-key-table.mdx) still apply.

Tag creation and deletion update the generated metadata file. This is the file read by
`table-location`, Hadoop, and Hive access. Refresh the reader or disable its catalog cache to
observe a changed tag.

:::info REST catalogs

Do not rely on Paimon tag creation or deletion being synchronized to a REST catalog. The tag
callback updates the local metadata file, while REST publication maintains separate catalog
metadata. The example below uses Hadoop catalog access.

:::

## Create and Query a Tag in Flink

Complete the [append-table walkthrough](./append-table.mdx) first, using the Flink tab. It creates
`paimon_catalog.default.cities` and an Iceberg Hadoop catalog with caching disabled.

Choose an existing snapshot ID from the Paimon snapshots table:

```sql
SELECT snapshot_id FROM paimon_catalog.`default`.`cities$snapshots`
ORDER BY snapshot_id;
```

After verifying that snapshot 1 is present in the published Iceberg history, create a tag and query it:

```sql
CALL paimon_catalog.sys.create_tag('default.cities', 'first_batch', 1);

SELECT country, name
FROM iceberg_catalog.`default`.cities /*+ OPTIONS('tag'='first_batch') */
WHERE country = 'germany'
ORDER BY name;
```

For the first insert in the walkthrough, the tagged rows are:

```text
country  name
germany  berlin
germany  hamburg
```

If you reused an existing table, replace `1` with the snapshot you intend to tag. Verify that snapshot
in the Iceberg reader's snapshot history as well; a Paimon snapshot's existence alone is insufficient.

## Retention and Format Changes

Manage tag lifecycle through [Paimon tags](../maintenance/manage-tags.mdx). A tag selects a snapshot;
it does not bypass the publication rules or convert the table's file format.

Tags do not require format v3 by themselves. Upgrade `metadata.iceberg.format-version` only when a
feature such as deletion vectors requires it. A format change rebuilds Iceberg metadata and can
remove previously published history and tag references. Verify the snapshots and tags available
through Iceberg after rebuilding.
