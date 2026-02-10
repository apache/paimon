---
title: "Manage Tags"
weight: 3
type: docs
aliases:
  - /pypaimon/manage-tags.html
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

# Manage Tags

Just like Java API of Paimon, you can create a [tag]({{< ref "maintenance/manage-tags" >}}) based on a snapshot. The tag will maintain the manifests and data files of the snapshot. 
A typical usage is creating tags daily, then you can maintain the historical data of each day for batch reading.
## Create and Delete Tag

You can create a tag with given name and snapshot ID, and delete a tag with given name.

```python

table = catalog.get_table('database_name.table_name')
table.create_tag("tag2", snapshot_id=2)  # create tag2 based on snapshot 2
table.create_tag("tag2")  # create tag2 based on latest snapshot
table.delete_tag("tag2")  # delete tag2
```

If snapshot_id unset, snapshot_id defaults to the latest.

## Read Tag
You can read data from a specific tag.
```python
from pypaimon.common.options.core_options import CoreOptions

table = catalog.get_table('database_name.table_name')
table.create_tag("tag2", snapshot_id=2)

# Read from tag2 using scan.tag-name option
table_with_tag = table.copy({CoreOptions.SCAN_TAG_NAME.key(): "tag2"})
read_builder = table_with_tag.new_read_builder()
table_scan = read_builder.new_scan()
table_read = read_builder.new_read()
result = table_read.to_arrow(table_scan.plan().splits())
```


