---
title: "Python API"
weight: 3
type: docs
aliases:
- /api/python-api.html
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

# Java-based Implementation For Python API

[Python SDK ](https://github.com/apache/paimon-python) has defined Python API for Paimon. Currently, there is only a Java-based implementation.

Java-based implementation will launch a JVM and use `py4j` to execute Java code to read and write Paimon table.

## Environment Settings

### SDK Installing

SDK is published at [paimon-python](https://pypi.org/project/paimon-python/). You can install by
```shell
pip install paimon-python
```

### Java Runtime Environment

This SDK needs JRE 1.8. After installing JRE, make sure that at least one of the following conditions is met:
1. `java` command is available. You can verify it by `java -version`.
2. `JAVA_HOME` and `PATH` variables are set correctly. For example, you can set:
```shell
export JAVA_HOME=/path/to/java-directory
export PATH=$JAVA_HOME/bin:$PATH
```

### Set Environment Variables

Because we need to launch a JVM to access Java code, JVM environment need to be set. Besides, the java code need Hadoop
dependencies, so hadoop environment should be set.

#### Java classpath

The package has set necessary paimon core dependencies (Local/Hadoop FileIO, Avro/Orc/Parquet format support and 
FileSystem/Jdbc/Hive catalog), so If you just test codes in local or in hadoop environment, you don't need to set classpath.

If you need other dependencies such as OSS/S3 filesystem jars, or special format and catalog ,please prepare jars and set 
classpath via one of the following ways:

1. Set system environment variable: ```export _PYPAIMON_JAVA_CLASSPATH=/path/to/jars/*```
2. Set environment variable in Python code:

```python
import os
from paimon_python_java import constants

os.environ[constants.PYPAIMON_JAVA_CLASSPATH] = '/path/to/jars/*'
```

#### JVM args (optional)

You can set JVM args via one of the following ways:

1. Set system environment variable: ```export _PYPAIMON_JVM_ARGS='arg1 arg2 ...'```
2. Set environment variable in Python code:

```python
import os
from paimon_python_java import constants

os.environ[constants.PYPAIMON_JVM_ARGS] = 'arg1 arg2 ...'
```

#### Hadoop classpath

If the machine is in a hadoop environment, please ensure the value of the environment variable HADOOP_CLASSPATH include
path to the common Hadoop libraries, then you don't need to set hadoop.

Otherwise, you should set hadoop classpath via one of the following ways:

1. Set system environment variable: ```export _PYPAIMON_HADOOP_CLASSPATH=/path/to/jars/*```
2. Set environment variable in Python code:

```python
import os
from paimon_python_java import constants

os.environ[constants.PYPAIMON_HADOOP_CLASSPATH] = '/path/to/jars/*'
```

If you just want to test codes in local, we recommend to use [Flink Pre-bundled hadoop jar](https://flink.apache.org/downloads/#additional-components).


## Create Catalog

Before coming into contact with the Table, you need to create a Catalog.

```python
from paimon_python_java import Catalog

# Note that keys and values are all string
catalog_options = {
    'metastore': 'filesystem',
    'warehouse': 'file:///path/to/warehouse'
}
catalog = Catalog.create(catalog_options)
```

## Create Database & Table

You can use the catalog to create table for writing data.

### Create Database (optional)
Table is located in a database. If you want to create table in a new database, you should create it.

```python
catalog.create_database(
  name='database_name', 
  ignore_if_exists=True,    # If you want to raise error if the database exists, set False
  properties={'key': 'value'} # optional database properties
)
```

### Create Schema

Table schema contains fields definition, partition keys, primary keys, table options and comment. For example:

```python
import pyarrow as pa

from paimon_python_api import Schema

pa_schema = pa.schema([
    ('dt', pa.string()),
    ('hh', pa.string()),
    ('pk', pa.int64()),
    ('value', pa.string())
])

schema = Schema(
    pa_schema=pa_schema, 
    partition_keys=['dt', 'hh'],
    primary_keys=['dt', 'hh', 'pk'],
    options={'bucket': '2'},
    comment='my test table'
)
```

All arguments except `pa_schema` is optional. If you have some Pandas data, the `pa_schema` can be extracted from `DataFrame`:

```python
import pandas as pd
import pyarrow as pa

from paimon_python_api import Schema

# Example DataFrame data
data = {
    'dt': ['2024-01-01', '2024-01-01', '2024-01-02'],
    'hh': ['12', '15', '20'],
    'pk': [1, 2, 3],
    'value': ['a', 'b', 'c'],
}
dataframe = pd.DataFrame(data)

# Get Paimon Schema
record_batch = pa.RecordBatch.from_pandas(dataframe)
schema = Schema(
    pa_schema=record_batch.schema, 
    partition_keys=['dt', 'hh'], 
    primary_keys=['dt', 'hh', 'pk'],
    options={'bucket': '2'})
```

### Create Tale
```python
schema = ...
catalog.create_table(
    identifier='database_name.table_name',
    schema=schema, 
    ignore_if_exists=True # If you want to raise error if the table exists, set False
)
```

## Get Table

The Table interface provides tools to read and write table.

```python
table = catalog.get_table('database_name.table_name')
```

## Batch Read

TableRead interface provides parallelly reading for multiple splits. You can set `'max-workers': 'N'` in `catalog_options` 
to set thread numbers when reading splits. `max-workers` is 1 by default, that means TableRead will read splits sequentially 
if you doesn't set `max-workers`.

```python
table = catalog.get_table('database_name.table_name')

# 1. Create table scan and read
read_builder = table.new_read_builder()
table_scan = read_builder.new_scan()
table_read = read_builder.new_read()

# 2. Get splits
splits = table_scan.plan().splits()

# 3. Read splits. Support 3 methods:
# 3.1 Read as pandas.DataFrame
dataframe = table_read.to_pandas(splits)

# 3.2 Read as pyarrow.Table
pa_table = table_read.to_arrow(splits)

# 3.3 Read as pyarrow.RecordBatchReader
record_batch_reader = table_read.to_arrow_batch_reader(splits)
```

## Batch Write

Paimon table write is Two-Phase Commit, you can write many times, but once committed, no more data can be write.

{{< hint warning >}}
Currently, Python SDK doesn't support writing primary key table with `bucket=-1`.
{{< /hint >}}

```python
table = catalog.get_table('database_name.table_name')

# 1. Create table write and commit
write_builder = table.new_batch_write_builder()
# By default, write data will be appended to table.
# If you want to overwrite table:
# write_builder.overwrite()
# If you want to overwrite partition 'dt=2024-01-01': 
# write_builder.overwrite({'dt': '2024-01-01'})

table_write = write_builder.new_write()
table_commit = write_builder.new_commit()

# 2. Write data. Support 3 methods:
# 2.1 Write pandas.DataFrame
dataframe = ...
table_write.write_pandas(dataframe)

# 2.2 Write pyarrow.Table
pa_table = ...
table_write.write_arrow(pa_table)

# 2.3 Write pyarrow.RecordBatch
record_batch = ...
table_write.write_arrow_batch(record_batch)

# 3. Commit data
commit_messages = table_write.prepare_commit()
table_commit.commit(commit_messages)

# 4. Close resources
table_write.close()
table_commit.close()

```

## Data Types

| pyarrow                                  | Paimon   | 
|:-----------------------------------------|:---------|
| pyarrow.int8()                           | TINYINT  |
| pyarrow.int16()                          | SMALLINT |
| pyarrow.int32()                          | INT      |
| pyarrow.int64()                          | BIGINT   |
| pyarrow.float16() <br/>pyarrow.float32() | FLOAT    |
| pyarrow.float64()                        | DOUBLE   |
| pyarrow.string()                         | STRING   |
| pyarrow.boolean()                        | BOOLEAN  |

## Predicate

You can use predicate to filter data when reading. Example:

```python
# table data:
# f0: 0 1 2 3 4
# f1: 5 6 7 8 9
read_builder = table.new_read_builder()
predicate_builder = read_builder.new_predicate_builder()

# build predicate: f0 < 3 && f1 > 5
predicate1 = predicate_builder.less_than('f0', 1);
predicate2 = predicate_builder.greater_than('f1', 5);
predicate = predicate_builder.and_predicates([predicate1, predicate2])

read_builder = read_builder.with_filter(predicate)
table_scan = read_builder.new_scan()
table_read = read_builder.new_read()
splits = table_scan.plan().splits()
dataframe = table_read.to_pandas(splits)

# result:
# f0: 1 2
# f1: 6 7
```

| Predicate kind        | Predicate method                              | 
|:----------------------|:----------------------------------------------|
| p1 and p2             | PredicateBuilder.and_predicates([p1, p2])     |
| p1 or p2              | PredicateBuilder.or_predicates([p1, p2])      |
| f = literal           | PredicateBuilder.equal(f, literal)            |
| f != literal          | PredicateBuilder.not_equal(f, literal)        |
| f < literal           | PredicateBuilder.less_than(f, literal)        |
| f <= literal          | PredicateBuilder.less_or_equal(f, literal)    |
| f > literal           | PredicateBuilder.greater_than(f, literal)     |
| f >= literal          | PredicateBuilder.greater_or_equal(f, literal) |
| f is null             | PredicateBuilder.is_null(f)                   |
| f is not null         | PredicateBuilder.is_not_null(f)               |
| f.startswith(literal) | PredicateBuilder.startswith(f, literal)       |
| f.endswith(literal)   | PredicateBuilder.endswith(f, literal)         |
| f.contains(literal)   | PredicateBuilder.contains(f, literal)         |
| f is in [l1, l2]      | PredicateBuilder.is_in(f, [l1, l2])           |
| f is not in [l1, l2]  | PredicateBuilder.is_not_in(f, [l1, l2])       |
| lower <= f <= upper   | PredicateBuilder.between(f, lower, upper)     |
