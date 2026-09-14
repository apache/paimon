---
title: "Consumers, Views, and Functions"
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

# Consumers, Views, and Functions

Manage consumer positions, partition completion, view dialects, and stored functions.
See [Streaming Recovery](../streaming-recovery), [SQL Functions](../sql-functions), and
[Views](../../concepts/views) for the corresponding feature guides.

For catalog selection and invocation syntax, see [Procedures](../procedures).

## reset_consumer

Reset or delete consumer.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `consumerId` (`STRING`, required): consumer to be reset or deleted.
- `nextSnapshotId` (`BIGINT`, optional): the new next snapshot id of the consumer.

```sql
-- reset the new next snapshot id in the consumer
CALL sys.reset_consumer(table => 'default.T', consumerId => 'myid', nextSnapshotId => 10);

-- delete consumer
CALL sys.reset_consumer(table => 'default.T', consumerId => 'myid');
```

## clear_consumers

Clear consumers.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `includingConsumers` (`STRING`, optional): consumers to be cleared.
- `excludingConsumers` (`STRING`, optional): consumers which not to be cleared.

```sql
-- clear all consumers in the table
CALL sys.clear_consumers(table => 'default.T');

-- clear some consumers in the table (accept regular expression)
CALL sys.clear_consumers(table => 'default.T', includingConsumers => 'myid.*');

-- clear all consumers except excludingConsumers in the table (accept regular expression)
CALL sys.clear_consumers(
  table => 'default.T',
  includingConsumers => '',
  excludingConsumers => 'myid1.*'
);

-- clear all consumers with includingConsumers and excludingConsumers (accept regular expression)
CALL sys.clear_consumers(
  table => 'default.T',
  includingConsumers => 'myid.*',
  excludingConsumers => 'myid1.*'
);
```

## mark_partition_done

Mark partitions as done.

**Arguments**

- `table` (`STRING`, required): the target table identifier.
- `partitions` (`STRING`, required): partitions need to be mark done, If you specify multiple partitions, delimiter is ';'.

```sql
-- mark single partition done
CALL sys.mark_partition_done(table => 'default.T', partitions => 'day=2024-07-01');

-- mark multiple partitions done
CALL sys.mark_partition_done(table => 'default.T', partitions => 'day=2024-07-01;day=2024-07-02');
```

## alter_view_dialect

Alter view dialect.

**Arguments**

- `view` (`STRING`, required): the target view identifier.
- `action` (`STRING`, required): define change action like: add, update, drop.
- `engine` (`STRING`, optional): when engine which is not spark need define it.
- `query` (`STRING`, optional): query for the dialect when action is add and update it couldn't be empty.

```sql
-- add dialect in the view
CALL sys.alter_view_dialect('view_identifier', 'add', 'spark', 'query');

CALL sys.alter_view_dialect(`view` => 'view_identifier', `action` => 'add', `query` => 'query');

-- update dialect in the view
CALL sys.alter_view_dialect('view_identifier', 'update', 'spark', 'query');

CALL sys.alter_view_dialect(`view` => 'view_identifier', `action` => 'update', `query` => 'query');

-- drop dialect in the view
CALL sys.alter_view_dialect('view_identifier', 'drop', 'spark');

CALL sys.alter_view_dialect(`view` => 'view_identifier', `action` => 'drop');
```

## create_function

Create a function.

**Arguments**

- `function` (`STRING`, required): the target function identifier.
- `inputParams` (`STRING`, required): inputParams of the function.
- `returnParams` (`STRING`, required): returnParams of the function.
- `deterministic` (`BOOLEAN`, optional): Whether the function is deterministic.
- `comment` (`STRING`, optional): The comment for the function.
- `options` (`STRING`, optional): the additional dynamic options of the function.

```sql
CALL sys.create_function(
  `function` => 'function_identifier',
  `inputParams` => '[{"id": 0, "name":"length", "type":"INT"}, {"id": 1, "name":"width", "type":"INT"}]',
  `returnParams` => '[{"id": 0, "name":"area", "type":"BIGINT"}]',
  `deterministic` => true,
  `comment` => 'comment',
  `options` => 'k1=v1,k2=v2'
);
```

## alter_function

Alter a function.

**Arguments**

- `function` (`STRING`, required): the target function identifier.
- `change` (`STRING`, required): change of the function.

```sql
CALL sys.alter_function(
  `function` => 'function_identifier',
  `change` => '{"action" : "addDefinition", "name" : "spark", "definition" : {"type" : "lambda", "definition" : "(Integer length, Integer width) -> { return (long) length * width; }", "language": "JAVA" } }'
);
```

## drop_function

Drop a function.

**Arguments**

- `function` (`STRING`, required): the target function identifier.

```sql
CALL sys.drop_function(`function` => 'function_identifier');
```
