---
title: "Data Evolution"
weight: 5
type: docs
aliases:
  - /pypaimon/data-evolution.html
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

# Data Evolution

PyPaimon for Data Evolution mode. See [Data Evolution]({{< ref "append-table/data-evolution" >}}).

## Update Columns By Row ID

You can create `TableUpdate.update_by_arrow_with_row_id` to update columns to data evolution tables.

The input data should include the `_ROW_ID` column, update operation will automatically sort and match each `_ROW_ID` to
its corresponding `first_row_id`, then groups rows with the same `first_row_id` and writes them to a separate file.

```python
simple_pa_schema = pa.schema([
  ('f0', pa.int8()),
  ('f1', pa.int16()),
])
schema = Schema.from_pyarrow_schema(simple_pa_schema,
                                    options={'row-tracking.enabled': 'true', 'data-evolution.enabled': 'true'})
catalog.create_table('default.test_row_tracking', schema, False)
table = catalog.get_table('default.test_row_tracking')

# write all columns
write_builder = table.new_batch_write_builder()
table_write = write_builder.new_write()
table_commit = write_builder.new_commit()
expect_data = pa.Table.from_pydict({
  'f0': [-1, 2],
  'f1': [-1001, 1002]
}, schema=simple_pa_schema)
table_write.write_arrow(expect_data)
table_commit.commit(table_write.prepare_commit())
table_write.close()
table_commit.close()

# update partial columns
write_builder = table.new_batch_write_builder()
table_update = write_builder.new_update().with_update_type(['f0'])
table_commit = write_builder.new_commit()
data2 = pa.Table.from_pydict({
  '_ROW_ID': [0, 1],
  'f0': [5, 6],
}, schema=pa.schema([
  ('_ROW_ID', pa.int64()),
  ('f0', pa.int8()),
]))
cmts = table_update.update_by_arrow_with_row_id(data2)
table_commit.commit(cmts)
table_commit.close()

# content should be:
#   'f0': [5, 6],
#   'f1': [-1001, 1002]
```
