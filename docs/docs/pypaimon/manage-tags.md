---
title: "Manage Tags"
sidebar_position: 3
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

A [tag](../maintenance/manage-tags) retains a snapshot and its referenced manifests
and data files. Use tags to keep a dataset version available for batch reads or
training. The examples assume a `catalog` from [Catalogs and Tables](./catalogs).
For separate writable histories, see [Branches and Rollback](./branches).

## Create and Delete Tag

You can create a tag with given name and snapshot ID, and delete a tag with given name.

```python

table = catalog.get_table('database_name.table_name')
table.create_tag("snapshot_2", snapshot_id=2)  # requires snapshot 2 to exist
table.create_tag("latest_data")  # retain the latest snapshot under another name
table.delete_tag("snapshot_2")
```

If snapshot_id unset, snapshot_id defaults to the latest.

## Rename Tag

You can rename a tag to a new name.

```python

table = catalog.get_table('database_name.table_name')
table.rename_tag("old_tag", "new_tag")  # rename old_tag to new_tag
```

## Read Tag
You can read data from a specific tag.
```python

table = catalog.get_table('database_name.table_name')
table.create_tag("tag2", snapshot_id=2)

# Read from tag2 using scan.tag-name option
table_with_tag = table.copy({"scan.tag-name": "tag2"})
read_builder = table_with_tag.new_read_builder()
table_scan = read_builder.new_scan()
table_read = read_builder.new_read()
result = table_read.to_arrow(table_scan.plan().splits())
```


