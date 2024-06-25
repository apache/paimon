---
title: "Deletion Vectors"
weight: 5
type: docs
aliases:
- /append-table/deletion-vectors.html
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

# Deletion Vectors

{{< hint info >}}
Currently only Spark SQL supports `DELETE`, `UPDATE` for append table.
{{< /hint >}}

Consider SQL:
```sql
DELETE FROM my_table WHERE currency = 'UNKNOWN';
```

By default, it will search for the hit files and then rewrite each file to remove the
data that needs to be deleted from the files. This operation is costly.

Deletion vectors mode only marks certain records of the corresponding file for deletion
and writes the deletion file, without rewriting the entire file.

## Usage

By specifying `'deletion-vectors.enabled' = 'true'`, the Deletion Vectors mode can be enabled.
