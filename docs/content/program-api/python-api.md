---
title: "Python API"
weight: 4
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

SDK is published at [pypaimon](https://pypi.org/project/pypaimon/). You can install by
```shell
pip install pypaimon
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
from pypaimon.py4j import constants

os.environ[constants.PYPAIMON_JAVA_CLASSPATH] = '/path/to/jars/*'
```

#### JVM args (optional)

You can set JVM args via one of the following ways:

1. Set system environment variable: ```export _PYPAIMON_JVM_ARGS='arg1 arg2 ...'```
2. Set environment variable in Python code:

```python
import os
from pypaimon.py4j import constants

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
from pypaimon.py4j import constants

os.environ[constants.PYPAIMON_HADOOP_CLASSPATH] = '/path/to/jars/*'
```

If you just want to test codes in local, we recommend to use [Flink Pre-bundled hadoop jar](https://flink.apache.org/downloads/#additional-components).


## Create Catalog

Before coming into contact with the Table, you need to create a Catalog.

```python
from pypaimon.py4j import Catalog

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

Table schema contains fields definition, partition keys, primary keys, table options and comment. 
The field definition is described by `pyarrow.Schema`. All arguments except fields definition are optional.

Generally, there are two ways to build `pyarrow.Schema`.

First, you can use `pyarrow.schema` method directly, for example:

```python
import pyarrow as pa

from pypaimon import Schema

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

See [Data Types]({{< ref "python-api#data-types" >}}) for all supported `pyarrow-to-paimon` data types mapping. 

Second, if you have some Pandas data, the `pa_schema` can be extracted from `DataFrame`:

```python
import pandas as pd
import pyarrow as pa

from pypaimon import Schema

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
    options={'bucket': '2'},
    comment='my test table'
)
```

### Create Table

After building table schema, you can create corresponding table:

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

### Set Read Parallelism

TableRead interface provides parallelly reading for multiple splits. You can set `'max-workers': 'N'` in `catalog_options`
to set thread numbers for reading splits. `max-workers` is 1 by default, that means TableRead will read splits sequentially
if you doesn't set `max-workers`.

### Get ReadBuilder and Perform pushdown

A `ReadBuilder` is used to build reading utils and perform filter and projection pushdown.

```python
table = catalog.get_table('database_name.table_name')
read_builder = table.new_read_builder()
```

You can use `PredicateBuilder` to build filters and pushdown them by `ReadBuilder`:

```python
# Example filter: ('f0' < 3 OR 'f1' > 6) AND 'f3' = 'A'

predicate_builder = read_builder.new_predicate_builder()

predicate1 = predicate_builder.less_than('f0', 3)
predicate2 = predicate_builder.greater_than('f1', 6)
predicate3 = predicate_builder.or_predicates([predicate1, predicate2])

predicate4 = predicate_builder.equal('f3', 'A')
predicate_5 = predicate_builder.and_predicates([predicate3, predicate4])

read_builder = read_builder.with_filter(predicate_5)
```

See [Predicate]({{< ref "python-api#predicate" >}}) for all supported filters and building methods.

You can also pushdown projection by `ReadBuilder`:

```python
# select f3 and f2 columns
read_builder = read_builder.with_projection(['f3', 'f2'])
```

### Scan Plan

Then you can step into Scan Plan stage to get `splits`:

```python
table_scan = read_builder.new_scan()
splits = table_scan.splits()
```

### Read Splits

Finally, you can read data from the `splits` to various data format.

#### Apache Arrow

This requires `pyarrow` to be installed.

You can read all the data into a `pyarrow.Table`:

```python
table_read = read_builder.new_read()
pa_table = table_read.to_arrow(splits)
print(pa_table)

# pyarrow.Table
# f0: int32
# f1: string
# ----
# f0: [[1,2,3],[4,5,6],...]
# f1: [["a","b","c"],["d","e","f"],...]
```

You can also read data into a `pyarrow.RecordBatchReader` and iterate record batches:

```python
table_read = read_builder.new_read()
for batch in table_read.to_arrow_batch_reader(splits):
    print(batch)

# pyarrow.RecordBatch
# f0: int32
# f1: string
# ----
# f0: [1,2,3]
# f1: ["a","b","c"]
```

#### Pandas

This requires `pandas` to be installed.

You can read all the data into a `pandas.DataFrame`:

```python
table_read = read_builder.new_read()
df = table_read.to_pandas(splits)
print(df)

#    f0 f1
# 0   1  a
# 1   2  b
# 2   3  c
# 3   4  d
# ...
```

#### DuckDB

This requires `duckdb` to be installed.

You can convert the splits into an in-memory DuckDB table and query it:

```python
table_read = read_builder.new_read()
duckdb_con = table_read.to_duckdb(splits, 'duckdb_table')

print(duckdb_con.query("SELECT * FROM duckdb_table").fetchdf())
#    f0 f1
# 0   1  a
# 1   2  b
# 2   3  c
# 3   4  d
# ...

print(duckdb_con.query("SELECT * FROM duckdb_table WHERE f0 = 1").fetchdf())
#    f0 f1
# 0   1  a
```

#### Ray

This requires `ray` to be installed.

You can convert the splits into a Ray dataset and handle it by Ray API:

```python
table_read = read_builder.new_read()
ray_dataset = table_read.to_ray(splits)

print(ray_dataset)
# MaterializedDataset(num_blocks=1, num_rows=9, schema={f0: int32, f1: string})

print(ray_dataset.take(3))
# [{'f0': 1, 'f1': 'a'}, {'f0': 2, 'f1': 'b'}, {'f0': 3, 'f1': 'c'}]

print(ray_dataset.to_pandas())
#    f0 f1
# 0   1  a
# 1   2  b
# 2   3  c
# 3   4  d
# ...
```

## Batch Write

Paimon table write is Two-Phase Commit, you can write many times, but once committed, no more data can be written.

{{< hint warning >}}
Currently, Python SDK doesn't support writing primary key table with `bucket=-1`.
{{< /hint >}}

```python
table = catalog.get_table('database_name.table_name')

# 1. Create table write and commit
write_builder = table.new_batch_write_builder()
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

By default, the data will be appended to table. If you want to overwrite table, you should use `TableWrite#overwrite` API:

```python
# overwrite whole table
write_builder.overwrite()

# overwrite partition 'dt=2024-01-01'
write_builder.overwrite({'dt': '2024-01-01'})
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

