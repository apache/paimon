---
title: "Views and Functions"
sidebar_position: 8
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

# Views and Functions

Manage view dialects and catalog functions.

See [Procedures](../procedures) for Flink version requirements, argument conventions, and catalog selection.

## alter_view_dialect

To alter view dialect. Arguments:

- `view`: the target view identifier. Cannot be empty.

- `action`: define change action like: add, update, drop. Cannot be empty.

- `engine`: when engine which is not flink need define it.

- `query`: query for the dialect when action is add and update it couldn't be empty.

**Syntax**

```sql
-- add dialect in the view
CALL [catalog.]sys.alter_view_dialect('view_identifier', 'add', 'flink', 'query');

CALL [catalog.]sys.alter_view_dialect(`view` => 'view_identifier', `action` => 'add', `query` => 'query');

-- update dialect in the view
CALL [catalog.]sys.alter_view_dialect('view_identifier', 'update', 'flink', 'query');

CALL [catalog.]sys.alter_view_dialect(`view` => 'view_identifier', `action` => 'update', `query` => 'query');

-- drop dialect in the view
CALL [catalog.]sys.alter_view_dialect('view_identifier', 'drop', 'flink');

CALL [catalog.]sys.alter_view_dialect(`view` => 'view_identifier', `action` => 'drop');
```

**Example**

```sql
-- add dialect in the view
CALL sys.alter_view_dialect('view_identifier', 'add', 'flink', 'query');

CALL sys.alter_view_dialect(`view` => 'view_identifier', `action` => 'add', `query` => 'query');

-- update dialect in the view
CALL sys.alter_view_dialect('view_identifier', 'update', 'flink', 'query');

CALL sys.alter_view_dialect(`view` => 'view_identifier', `action` => 'update', `query` => 'query');

-- drop dialect in the view
CALL sys.alter_view_dialect('view_identifier', 'drop', 'flink');

CALL sys.alter_view_dialect(`view` => 'view_identifier', `action` => 'drop');
```

## create_function

To create a function. Arguments:

- `function`: the target function identifier. Cannot be empty.

- `inputParams`: inputParams of the function.

- `returnParams`: returnParams of the function.

- `deterministic`: Whether the function is deterministic.

- `comment`: The comment for the function.

- `options`: the additional dynamic options of the function.

**Syntax**

```sql
CALL [catalog.]sys.create_function(
    'function_identifier',
    '[{"id": 0, "name":"length", "type":"INT"}, {"id": 1, "name":"width", "type":"INT"}]',
    '[{"id": 0, "name":"area", "type":"BIGINT"}]',
    true, 'comment', 'k1=v1,k2=v2');
```

**Example**

```sql
CALL sys.create_function(`function` => 'function_identifier',
    inputParams => '[{"id": 0, "name":"length", "type":"INT"}, {"id": 1, "name":"width", "type":"INT"}]',
    returnParams => '[{"id": 0, "name":"area", "type":"BIGINT"}]',
    deterministic => true,
    comment => 'comment',
    options => 'k1=v1,k2=v2'
    );
```

## alter_function

To alter a function. Arguments:

- `function`: the target function identifier. Cannot be empty.

- `change`: change of the function.

**Syntax**

```sql
CALL [catalog.]sys.alter_function(
    'function_identifier',
    '{"action" : "addDefinition", "name" : "flink", "definition" : {"type" : "file", "fileResources" : [{"resourceType": "JAR", "uri": "oss://mybucket/xxxx.jar"}], "language": "JAVA", "className": "xxxx", "functionName": "functionName" } }');
```

**Example**

```sql
CALL sys.alter_function(`function` => 'function_identifier',
    `change` => '{"action" : "addDefinition", "name" : "flink", "definition" : {"type" : "file", "fileResources" : [{"resourceType": "JAR", "uri": "oss://mybucket/xxxx.jar"}], "language": "JAVA", "className": "xxxx", "functionName": "functionName" } }'
    );
```

## drop_function

To drop a function. Arguments:

- `function`: the target function identifier. Cannot be empty.

**Syntax**

```sql
CALL [catalog.]sys.drop_function('function_identifier');
```

**Example**

```sql
CALL sys.drop_function(`function` => 'function_identifier');
```
