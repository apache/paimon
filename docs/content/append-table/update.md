---
title: "Update"
weight: 4
type: docs
aliases:
- /append-table/update.html
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

# Update

Now, only Spark SQL supports DELETE & UPDATE, you can take a look to [Spark Write]({{< ref "spark/sql-write" >}}).

Example:
```sql
DELETE FROM my_table WHERE currency = 'UNKNOWN';
```

Update append table has two modes:

1. COW (Copy on Write): search for the hit files and then rewrite each file to remove the data that needs to be deleted
   from the files. This operation is costly.
2. MOW (Merge on Write): By specifying `'deletion-vectors.enabled' = 'true'`, the Deletion Vectors mode can be enabled.
   Only marks certain records of the corresponding file for deletion and writes the deletion file, without rewriting the entire file.
