---
title: "Partial Update"
sidebar_position: 2
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

Set `merge-engine = partial-update` to combine updates to different columns of the same key.
By default, non-null input values replace the corresponding fields and null input values leave
them unchanged. [Sequence groups](#sequence-group) let independent streams order their own fields
and explicitly replace values with null.

The Flink SQL examples below use bounded inputs. Wait for each `INSERT` job to finish before
running the next statement, and run `SELECT` in batch mode.

## Non-Null Updates

For example, a product's price, quantity, and description can arrive in separate updates:

```sql
CREATE TABLE products (
    product_id BIGINT PRIMARY KEY NOT ENFORCED,
    price DECIMAL(10, 2),
    quantity INT,
    description STRING
) WITH (
    'merge-engine' = 'partial-update'
);

INSERT INTO products VALUES (1, 23.00, 10, CAST(NULL AS STRING));
INSERT INTO products VALUES (1, CAST(NULL AS DECIMAL(10, 2)), CAST(NULL AS INT), 'Book');
INSERT INTO products VALUES (1, 25.20, CAST(NULL AS INT), CAST(NULL AS STRING));

SELECT * FROM products;
-- 1, 25.20, 10, Book
```

The second update fills in the description. The third changes only the price; its null fields
preserve the stored quantity and description. To clear a stored value to null, use a sequence
group with a valid version, as shown below.

:::info

Streaming readers that need the complete updated rows require the `lookup` or `full-compaction`
[changelog producer](../changelog-producer). The `input` producer is also supported, but it
returns the input changes, which may contain only partial rows.

:::

## Sequence Group

A single [sequence field](../sequence-rowkind#sequence-field) orders an entire record. When
independent streams update different columns, give each stream its own sequence group so one
stream's version does not determine whether the other stream's update is accepted.

Configure `fields.<ordering-field>.sequence-group` with the columns that the version protects.
This example separates profile updates from score updates:

```sql
CREATE TABLE profiles (
    user_id BIGINT PRIMARY KEY NOT ENFORCED,
    name STRING,
    city STRING,
    profile_version BIGINT,
    score BIGINT,
    score_version BIGINT
) WITH (
    'merge-engine' = 'partial-update',
    'fields.profile_version.sequence-group' = 'name,city',
    'fields.score_version.sequence-group' = 'score'
);

INSERT INTO profiles VALUES (1, 'Ada', 'London', 10, 40, 5);

-- A newer profile clears city to NULL. A NULL score_version skips the score group.
INSERT INTO profiles VALUES (1, 'Ada', CAST(NULL AS STRING), 11, 999, CAST(NULL AS BIGINT));

-- The old profile is ignored, while the newer score is accepted.
INSERT INTO profiles VALUES (1, 'Old', 'Paris', 9, 50, 6);

SELECT * FROM profiles;
-- 1, Ada, NULL, 11, 50, 6
```

![Profile and score updates use independent versions: profile version 11 clears city, a null score version skips that group, and a later input updates only the score.](/img/primary-key-sequence-groups.svg)

For fields without aggregation, each group follows these rules:

| Incoming ordering value | Effect on the group |
| --- | --- |
| Null | Skip this group; other groups and ungrouped fields can still update |
| Smaller than the stored value | Keep the stored ordering value and protected fields |
| Greater than or equal to the stored value | Replace the ordering value and protected fields, including null values |

Fields outside sequence groups retain the default non-null update behavior. A primary-key
column cannot be an ordering field or a protected field. Common ordering types include `DECIMAL`,
`TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `FLOAT`, `DOUBLE`, `DATE`, `TIME`, `TIMESTAMP`, and
`TIMESTAMP_LTZ`.

### Multiple Ordering Fields

Use multiple ordering fields when one version is insufficient to break ties. For example,
`fields.profile_version,profile_offset.sequence-group = name,city` compares `profile_version`
first, then `profile_offset`. Both ordering columns must be present in the table schema.

The group is skipped only when **all** its ordering fields are null. Otherwise, the tuple
participates in comparison, with null sorting before a non-null value:

| Stored tuple | Incoming tuple | Effect on non-aggregated protected fields |
| --- | --- | --- |
| `(1, 100)` | `(NULL, NULL)` | Skip the group |
| `(1, 100)` | `(1, NULL)` | Keep stored fields: the incoming tuple is smaller |
| `(1, 100)` | `(2, NULL)` | Replace fields: the first ordering value is larger |
| `(1, 100)` | `(1, 100)` | Replace fields: equal versions are accepted |

## Aggregation For Partial Update

Set `fields.<field-name>.aggregate-function` to combine contributions to a field instead of
replacing it. The functions listed in [Aggregation](./aggregation) are available, but every
aggregated value field must belong to a sequence group unless its function is
`last_non_null_value`. Primary-key fields and sequence-group ordering fields are not aggregated
as value fields.

An aggregate changes how older records are handled: an older version can still contribute to
an aggregated field, while non-aggregated fields in the same group keep their stored values.
A group whose ordering fields are all null is still skipped.

For example, profile names use replacement, while `total` accumulates contributions:

```sql
CREATE TABLE profile_totals (
    user_id BIGINT PRIMARY KEY NOT ENFORCED,
    name STRING,
    profile_version BIGINT,
    total BIGINT,
    event_version BIGINT
) WITH (
    'merge-engine' = 'partial-update',
    'fields.profile_version.sequence-group' = 'name',
    'fields.event_version.sequence-group' = 'total',
    'fields.total.aggregate-function' = 'sum'
);

INSERT INTO profile_totals VALUES (1, 'Ada', 10, 5, 100);

-- The older name is ignored. The older total contribution still adds 3.
INSERT INTO profile_totals VALUES (1, 'Older', 9, 3, 90);

SELECT * FROM profile_totals;
-- 1, Ada, 10, 8, 100

-- Update the name; a NULL event_version skips the total contribution.
INSERT INTO profile_totals VALUES (1, 'Bea', 11, 999, CAST(NULL AS BIGINT));

SELECT * FROM profile_totals;
-- 1, Bea, 11, 8, 100
```

The stored ordering tuple advances on newer or equal versions. With `sum`, an older or equal
version still adds its contribution: a sequence group does **not** deduplicate repeated events.
The same rules apply to groups with multiple ordering fields.

:::warning

Sequence groups do not guarantee a complete historical ordering for order-dependent aggregates
such as `first_value` or `listagg`. An older contribution is combined with the current aggregate;
Paimon does not retain and sort all prior contributions by their sequence-group values.

:::

### Default Aggregation Function

`fields.default-aggregate-function` supplies a function for value fields without an explicit
field-level function. The sequence-group requirement above also applies to the default.
Ordering fields and primary-key fields keep their special roles.

For example, this configuration preserves non-null names while adding score contributions:

```sql
CREATE TABLE profile_scores (
    user_id BIGINT PRIMARY KEY NOT ENFORCED,
    name STRING,
    profile_version BIGINT,
    score BIGINT,
    score_version BIGINT
) WITH (
    'merge-engine' = 'partial-update',
    'fields.profile_version.sequence-group' = 'name',
    'fields.score_version.sequence-group' = 'score',
    'fields.default-aggregate-function' = 'last_non_null_value',
    'fields.score.aggregate-function' = 'sum'
);
```

The explicit `sum` takes precedence for `score`. The default `last_non_null_value` applies to
`name`, so a newer profile with a null name now preserves the stored name instead of clearing it.

## Delete Handling

Choose how to process `DELETE` (`-D`) and `UPDATE_BEFORE` (`-U`) records before connecting a
source that produces retractions:

| Configuration | `DELETE` | `UPDATE_BEFORE` |
| --- | --- | --- |
| Default, without sequence groups | Rejected | Rejected |
| `ignore-delete = true` | Ignored | Ignored |
| `partial-update.remove-record-on-delete = true` | Remove the whole row | Ignored |
| Sequence groups | Retract protected fields according to the group version and aggregate function | Same field-level retraction rules |
| Sequence groups with `partial-update.remove-record-on-sequence-group` | Remove the whole row when a configured group accepts the delete's version | Apply field-level retractions; do not remove the whole row |

For a sequence group, a newer or equal retraction clears non-aggregated protected fields to
null. Aggregated fields use their function's retraction behavior, including for older versions;
see each [aggregate function's requirements](./aggregation). A group with all-null ordering
fields is skipped, and ungrouped fields are not retracted.

Set `partial-update.remove-record-on-sequence-group` to a comma-separated list of ordering
field names from the groups whose deletes should remove the whole row. For the `profiles`
schema above, using `profile_version` allows an accepted profile delete to remove the row.

`partial-update.remove-record-on-delete` cannot be combined with sequence groups. Neither
whole-row removal option can be combined with `ignore-delete`.
