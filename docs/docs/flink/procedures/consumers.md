---
title: "Consumers and Query Service"
sidebar_position: 6
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

# Consumers and Query Service

Manage stored consumer progress and start a lookup query service.

See [Procedures](../procedures) for Flink version requirements, argument conventions, and catalog selection.

## reset_consumer

To reset or delete consumer. Arguments:

- `table`: the target table identifier. Cannot be empty.

- `consumerId`: consumer to be reset or deleted.

- nextSnapshotId (Long): the new next snapshot id of the consumer.

**Syntax**

```sql
-- Use named argument
CALL [catalog.]sys.reset_consumer(
    `table` => 'identifier',
    consumer_id => 'consumerId',
    next_snapshot_id => 'nextSnapshotId'
);

-- Use indexed argument
-- reset the new next snapshot id in the consumer
CALL [catalog.]sys.reset_consumer('identifier', 'consumerId', nextSnapshotId);

-- delete consumer
CALL [catalog.]sys.reset_consumer('identifier', 'consumerId');
```

**Example**

```sql
CALL sys.reset_consumer(
    `table` => 'default.T',
    consumer_id => 'myid',
    next_snapshot_id => cast(10 as bigint)
);
```

## clear_consumers

To reset or delete consumer. Arguments:

- `table`: the target table identifier. Cannot be empty.

- `includingConsumers`: consumers to be cleared.

- `excludingConsumers`: consumers which not to be cleared.

**Syntax**

```sql
-- Use named argument
CALL [catalog.]sys.clear_consumers(
    `table` => 'identifier',
    including_consumers => 'includingConsumers',
    excluding_consumers => 'excludingConsumers'
);

-- Use indexed argument
-- clear all consumers in the table
CALL [catalog.]sys.clear_consumers('identifier');

-- clear some consumers in the table (accept regular expression)
CALL [catalog.]sys.clear_consumers('identifier', 'includingConsumers');

-- exclude some consumers (accept regular expression)
CALL [catalog.]sys.clear_consumers('identifier', 'includingConsumers', 'excludingConsumers');
```

**Example**

```sql
CALL sys.clear_consumers(`table` => 'default.T');

CALL sys.clear_consumers(`table` => 'default.T', including_consumers => 'myid.*');

CALL sys.clear_consumers(table => 'default.T', including_consumers => '', excluding_consumers => 'myid1.*');

CALL sys.clear_consumers(
    table => 'default.T',
    including_consumers => 'myid.*',
    excluding_consumers => 'myid1.*'
);
```

## query_service

Start a query service for a table. Arguments:

- `table`: the target table identifier.

- `parallelism`: the query service parallelism.

**Syntax**

```sql
CALL [catalog.]sys.query_service(`table` => 'identifier', parallelism => parallelism);
```

**Example**

```sql
CALL sys.query_service(`table` => 'default.T', parallelism => 4);
```
