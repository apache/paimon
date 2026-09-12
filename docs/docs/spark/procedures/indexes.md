---
title: "Indexes and Row IDs"
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

# Indexes and Row IDs

Build or remove indexes and maintain row IDs. Read the table-specific requirements in
[Global Indexes](../../multimodal-table/global-index) and
[Data Evolution](../../multimodal-table/data-evolution) before choosing an operation.

For catalog selection and invocation syntax, see [Procedures](../procedures).

## rewrite_file_index

Rewrite the file index for the table.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `where` (`STRING`, optional): partition predicate. Omit for all partitions.

```sql
CALL sys.rewrite_file_index(table => "t");

CALL sys.rewrite_file_index(table => "t", where => "day = '2025-08-17'");
```

## create_global_index

Create global index files for a given column. The table must have `row-tracking.enabled=true`.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `index_column` (`STRING`, required): the name of the column to index.
- `index_type` (`STRING`, required): type of the index to build, e.g. 'btree', 'bitmap', or 'fm'.
- `partitions` (`STRING`, optional): partition filter to limit the partitions on which to build the index. The comma (",") represents "AND", the semicolon (";") represents "OR". Omit for all partitions.
- `options` (`STRING`, optional): additional dynamic options of the table. These override stored table properties and are overridden by explicit procedure arguments.

```sql
CALL sys.create_global_index(table => 'default.T', index_column => 'name', index_type => 'btree');

CALL sys.create_global_index(
  table => 'default.T',
  index_column => 'tag',
  index_type => 'bitmap',
  options => 'sorted-index.records-per-range=1000000'
);

CALL sys.create_global_index(table => 'default.T', index_column => 'content', index_type => 'fm');

CALL sys.create_global_index(
  table => 'default.T',
  index_column => 'name',
  index_type => 'btree',
  partitions => 'pt=p1;pt=p2'
);

CALL sys.create_global_index(
  table => 'default.T',
  index_column => 'content',
  index_type => 'full-text',
  options => 'full-text.tokenizer=ngram,full-text.ngram.min-gram=2,full-text.ngram.max-gram=2'
);

CALL sys.create_global_index(
  table => 'default.T',
  index_column => 'content',
  index_type => 'full-text',
  options => 'full-text.tokenizer=jieba'
);

CALL sys.create_global_index(
  table => 'default.T',
  index_column => 'content',
  index_type => 'full-text',
  options => 'full-text.tokenizer=simple,full-text.stem=true,full-text.remove-stop-words=true'
);
```

## drop_global_index

Drop global index files for a given column.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `index_column` (`STRING`, required): the name of the indexed column.
- `index_type` (`STRING`, required): type of the index to drop, e.g. 'btree'.
- `partitions` (`STRING`, optional): partition filter to limit the partitions from which to drop the index. The comma (",") represents "AND", the semicolon (";") represents "OR". Omit for all partitions.
- `dry_run` (`BOOLEAN`, optional): when true, return the number of index files that would be dropped without committing any change. Default is false.

```sql
CALL sys.drop_global_index(
  table => 'default.T',
  index_column => 'name',
  index_type => 'btree',
  partitions => 'pt=p1'
);

-- Preview what would be dropped without deleting
CALL sys.drop_global_index(
  table => 'default.T',
  index_column => 'name',
  index_type => 'btree',
  dry_run => true
);
```

## reassign_row_id

Reassign row IDs for a data evolution table when partition row-id ranges overlap. The table must
have `row-tracking.enabled=true` and `data-evolution.enabled=true`.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `partitions` (`STRING`, optional): partition filter to limit the partitions to reassign. The comma (",") represents "AND", the semicolon (";") represents "OR". Omit for all partitions.

```sql
CALL sys.reassign_row_id(table => 'default.T');

CALL sys.reassign_row_id(table => 'default.T', partitions => 'dt=2026-05-19');
```
