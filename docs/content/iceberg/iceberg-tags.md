---
title: "Iceberg Tags"
weight: 4
type: docs
aliases:
- /iceberg/iceberg-tags.html
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

When enable iceberg compatibility, Paimon Tags will also be synced to [Iceberg Tags](https://iceberg.apache.org/docs/nightly/branching/#historical-tags).
Tags are only synced to Iceberg if the referenced snapshot exists in the Iceberg table.

```sql
CREATE CATALOG paimon WITH (
    'type' = 'paimon',
    'warehouse' = '<path-to-warehouse>'
);

CREATE CATALOG iceberg WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hadoop',
    'warehouse' = '<path-to-warehouse>/iceberg',
    'cache-enabled' = 'false' -- disable iceberg catalog caching to quickly see the result
);

-- create tag for paimon table
CALL paimon.sys.create_tag('default.T', 'tag1', 1);

-- query tag in iceberg table
SELECT * FROM iceberg.`default`.T /*+ OPTIONS('tag'='tag1') */;
```
