---
title: "Altering Tables"
weight: 3
type: docs
aliases:
- /how-to/altering-tables.html
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

# Altering Tables

## Changing/Adding Table Properties

The following SQL sets `write-buffer-size` table property to `256 MB`.

{{< tabs "set-properties-example" >}}

{{< tab "Flink" >}}

```sql
ALTER TABLE my_table SET (
    'write-buffer-size' = '256 MB'
);
```

{{< /tab >}}

{{< tab "Spark3" >}}

```sql
ALTER TABLE my_table SET TBLPROPERTIES (
    'write-buffer-size' = '256 MB'
);
```

{{< /tab >}}

{{< /tabs >}}

## Removing Table Properties

The following SQL removes `write-buffer-size` table property.

{{< tabs "unset-properties-example" >}}

{{< tab "Flink" >}}

```sql
ALTER TABLE my_table RESET ('write-buffer-size');
```

{{< /tab >}}

{{< tab "Spark3" >}}

```sql
ALTER TABLE my_table UNSET TBLPROPERTIES ('write-buffer-size');
```

{{< /tab >}}

{{< /tabs >}}

## Adding New Columns

The following SQL adds two columns `c1` and `c2` to table `my_table`.

{{< tabs "add-columns-example" >}}

{{< tab "Spark3" >}}

```sql
ALTER TABLE my_table ADD COLUMNS (
    c1 INT,
    c2 STRING
);
```

{{< /tab >}}

{{< /tabs >}}

## Renaming Column Name
The following SQL renames column `c0` in table `my_table` to `c1`.

{{< tabs "rename-column-name-example" >}}

{{< tab "Spark3" >}}

```sql
ALTER TABLE my_table RENAME COLUMN c0 TO c1;
```

{{< /tab >}}

{{< /tabs >}}

## Dropping Columns
The syntax is:

{{< tabs "drop-columns-syntax" >}}

{{< tab "Spark3" >}}

```sql
ALTER TABLE table_identifier DROP { COLUMN | COLUMNS } [(] col_name [, ... ] [)]
```

{{< /tab >}}

{{< /tabs >}}

The following SQL drops tow columns `c1` and `c2` from table `my_table`.

{{< tabs "drop-columns-example" >}}

{{< tab "Spark3" >}}

```sql
ALTER TABLE my_table DROP COLUMNS (c1, c2);
```

{{< /tab >}}

{{< /tabs >}}

## Changing Column Nullability

The following SQL sets column `coupon_info` to be nullable.

{{< tabs "change-nullability-example" >}}

{{< tab "Spark3" >}}

```sql
ALTER TABLE my_table ALTER COLUMN coupon_info DROP NOT NULL;
```

{{< /tab >}}

{{< /tabs >}}

## Changing Column Comment

The following SQL changes comment of column `buy_count` to `buy count`.

{{< tabs "change-comment-example" >}}

{{< tab "Spark3" >}}

```sql
ALTER TABLE my_table ALTER COLUMN buy_count COMMENT 'buy count';
```

{{< /tab >}}

{{< /tabs >}}
