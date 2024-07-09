---
title: "Overview"
weight: 1
type: docs
aliases:
- /concepts/spec/overview.html
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

# Spec Overview

This is the specification for the Paimon table format, this document standardizes the underlying file structure and
design of Paimon.

{{< img src="/img/file-layout.png">}}

## Terms

- Schema: fields, primary keys definition, partition keys definition and options.
- Snapshot: the entrance to all data committed at some specific time point.
- Manifest list: includes several manifest files.
- Manifest: includes several data files or changelog files.
- Data File: contains incremental records.
- Changelog File: contains records produced by changelog-producer.
- Global Index: index for a bucket or partition.
- Data File Index: index for a data file.

Run Flink SQL with Paimon:

```sql
CREATE CATALOG my_catalog WITH (
    'type' = 'paimon',
    'warehouse' = '/your/path'
);       
USE CATALOG my_catalog;

CREATE TABLE my_table (
    k INT PRIMARY KEY NOT ENFORCED,
    f0 INT,
    f1 STRING
);

INSERT INTO my_table VALUES (1, 11, '111');
```

Take a look to the disk:

```shell
warehouse
└── default.db
    └── my_table
        ├── bucket-0
        │   └── data-59f60cb9-44af-48cc-b5ad-59e85c663c8f-0.orc
        ├── index
        │   └── index-5625e6d9-dd44-403b-a738-2b6ea92e20f1-0
        ├── manifest
        │   ├── index-manifest-5d670043-da25-4265-9a26-e31affc98039-0
        │   ├── manifest-6758823b-2010-4d06-aef0-3b1b597723d6-0
        │   ├── manifest-list-9f856d52-5b33-4c10-8933-a0eddfaa25bf-0
        │   └── manifest-list-9f856d52-5b33-4c10-8933-a0eddfaa25bf-1
        ├── schema
        │   └── schema-0
        └── snapshot
            ├── EARLIEST
            ├── LATEST
            └── snapshot-1
```
