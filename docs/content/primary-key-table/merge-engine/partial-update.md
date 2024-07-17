---
title: "Partial Update"
weight: 2
type: docs
aliases:
  - /cdc-ingestion/merge-engin/partial-update.html
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

# Partial Update

By specifying `'merge-engine' = 'partial-update'`, users have the ability to update columns of a record through
multiple updates until the record is complete. This is achieved by updating the value fields one by one, using the
latest data under the same primary key. However, null values are not overwritten in the process.

For example, suppose Paimon receives three records:

- `<1, 23.0, 10, NULL>`-
- `<1, NULL, NULL, 'This is a book'>`
- `<1, 25.2, NULL, NULL>`

Assuming that the first column is the primary key, the final result would be `<1, 25.2, 10, 'This is a book'>`.

{{< hint info >}}
For streaming queries, `partial-update` merge engine must be used together with `lookup` or `full-compaction`
[changelog producer]({{< ref "primary-key-table/changelog-producer" >}}). ('input' changelog producer is also supported,
but only returns input records.)
{{< /hint >}}

{{< hint info >}}
By default, Partial update can not accept delete records, you can choose one of the following solutions:

- Configure 'ignore-delete' to ignore delete records.
- Configure 'partial-update.remove-record-on-delete' to remove the whole row when receiving delete records.
- Configure 'sequence-group's to retract partial columns.
  {{< /hint >}}

## Sequence Group

A sequence-field may not solve the disorder problem of partial-update tables with multiple stream updates, because
the sequence-field may be overwritten by the latest data of another stream during multi-stream update.

So we introduce sequence group mechanism for partial-update tables. It can solve:

1. Disorder during multi-stream update. Each stream defines its own sequence-groups.
2. A true partial-update, not just a non-null update.

See example:

```sql
CREATE TABLE t
(
    k   INT,
    a   INT,
    b   INT,
    g_1 INT,
    c   INT,
    d   INT,
    g_2 INT,
    PRIMARY KEY (k) NOT ENFORCED
) WITH (
      'merge-engine' = 'partial-update',
      'fields.g_1.sequence-group' = 'a,b',
      'fields.g_2.sequence-group' = 'c,d'
      );

INSERT INTO t
VALUES (1, 1, 1, 1, 1, 1, 1);

-- g_2 is null, c, d should not be updated
INSERT INTO t
VALUES (1, 2, 2, 2, 2, 2, CAST(NULL AS INT));

SELECT *
FROM t;
-- output 1, 2, 2, 2, 1, 1, 1

-- g_1 is smaller, a, b should not be updated
INSERT INTO t
VALUES (1, 3, 3, 1, 3, 3, 3);

SELECT *
FROM t; -- output 1, 2, 2, 2, 3, 3, 3
```

For `fields.<field-name>.sequence-group`, valid comparative data types include: DECIMAL, TINYINT, SMALLINT, INTEGER,
BIGINT, FLOAT, DOUBLE, DATE, TIME, TIMESTAMP, and TIMESTAMP_LTZ.

You can also configure multiple sorted fields in a `sequence-group`,
like `fields.<field-name1>,<field-name2>.sequence-group`, multiple fields will be compared in order.

See example:

```sql
CREATE TABLE SG
(
    k   INT,
    a   INT,
    b   INT,
    g_1 INT,
    c   INT,
    d   INT,
    g_2 INT,
    g_3 INT,
    PRIMARY KEY (k) NOT ENFORCED
) WITH (
      'merge-engine' = 'partial-update',
      'fields.g_1.sequence-group' = 'a,b',
      'fields.g_2,g_3.sequence-group' = 'c,d'
      );

INSERT INTO SG
VALUES (1, 1, 1, 1, 1, 1, 1, 1);

-- g_2, g_3 should not be updated
INSERT INTO SG
VALUES (1, 2, 2, 2, 2, 2, 1, CAST(NULL AS INT));

SELECT *
FROM SG;
-- output 1, 2, 2, 2, 1, 1, 1, 1

-- g_1 should not be updated
INSERT INTO SG
VALUES (1, 3, 3, 1, 3, 3, 3, 1);

SELECT *
FROM SG;
-- output 1, 2, 2, 2, 3, 3, 3, 1
```

## Aggregation For Partial Update

You can specify aggregation function for the input field, all the functions in the
[Aggregation]({{< ref "primary-key-table/merge-engine#aggregation" >}}) are supported.

See example:

```sql
CREATE TABLE t
(
    k INT,
    a INT,
    b INT,
    c INT,
    d INT,
    PRIMARY KEY (k) NOT ENFORCED
) WITH (
      'merge-engine' = 'partial-update',
      'fields.a.sequence-group' = 'b',
      'fields.b.aggregate-function' = 'first_value',
      'fields.c.sequence-group' = 'd',
      'fields.d.aggregate-function' = 'sum'
      );
INSERT INTO t
VALUES (1, 1, 1, CAST(NULL AS INT), CAST(NULL AS INT));
INSERT INTO t
VALUES (1, CAST(NULL AS INT), CAST(NULL AS INT), 1, 1);
INSERT INTO t
VALUES (1, 2, 2, CAST(NULL AS INT), CAST(NULL AS INT));
INSERT INTO t
VALUES (1, CAST(NULL AS INT), CAST(NULL AS INT), 2, 2);


SELECT *
FROM t; -- output 1, 2, 1, 2, 3
```

You can also configure an aggregation function for a `sequence-group` within multiple sorted fields.

See example:

```sql
CREATE TABLE AGG
(
    k   INT,
    a   INT,
    b   INT,
    g_1 INT,
    c   VARCHAR,
    g_2 INT,
    g_3 INT,
    PRIMARY KEY (k) NOT ENFORCED
) WITH (
      'merge-engine' = 'partial-update',
      'fields.a.aggregate-function' = 'sum',
      'fields.g_1,g_3.sequence-group' = 'a',
      'fields.g_2.sequence-group' = 'c');
-- a in sequence-group g_1, g_3 with sum agg
-- b not in sequence-group
-- c in sequence-group g_2 without agg

INSERT INTO AGG
VALUES (1, 1, 1, 1, '1', 1, 1);

-- g_2 should not be updated
INSERT INTO AGG
VALUES (1, 2, 2, 2, '2', CAST(NULL AS INT), 2);

SELECT *
FROM AGG;
-- output 1, 3, 2, 2, "1", 1, 2

-- g_1, g_3 should not be updated
INSERT INTO AGG
VALUES (1, 3, 3, 2, '3', 3, 1);

SELECT *
FROM AGG;
-- output 1, 6, 3, 2, "3", 3, 2
```

You can specify a default aggregation function for all the input fields with `fields.default-aggregate-function`, see
example:

```sql
CREATE TABLE t
(
    k INT,
    a INT,
    b INT,
    c INT,
    d INT,
    PRIMARY KEY (k) NOT ENFORCED
) WITH (
      'merge-engine' = 'partial-update',
      'fields.a.sequence-group' = 'b',
      'fields.c.sequence-group' = 'd',
      'fields.default-aggregate-function' = 'last_non_null_value',
      'fields.d.aggregate-function' = 'sum'
      );

INSERT INTO t
VALUES (1, 1, 1, CAST(NULL AS INT), CAST(NULL AS INT));
INSERT INTO t
VALUES (1, CAST(NULL AS INT), CAST(NULL AS INT), 1, 1);
INSERT INTO t
VALUES (1, 2, 2, CAST(NULL AS INT), CAST(NULL AS INT));
INSERT INTO t
VALUES (1, CAST(NULL AS INT), CAST(NULL AS INT), 2, 2);


SELECT *
FROM t; -- output 1, 2, 2, 2, 3

```
