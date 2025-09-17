---
title: "PVFS"
weight: 6
type: docs
aliases:
  - /concepts/rest/pvfs.html
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

# Paimon Virtual Storage

The REST Catalog provides built-in storage, including Paimon Table, Format Table, and Object Table (also known as Fileset or Volume),
both of which require direct access to the file system. And our REST Catalog generates UUID paths, which makes it difficult
to directly access the file system.

So there is PVFS, which can allow users to access it through similar methods `pvfs://catalog_name/database_name/table_name/`,
use the path to access all internal tables in the REST Catalog, including Paimon Table, Format Table, and Object Table.
Another advantage is that all user access to this file system is through the permission system of Paimon REST Catalog,
without the need to maintain another file system permission system.

## API Behavior

For example, if you have a catalog named 'my_catalog', the list behavior should be:

- `listStatus(Path('pvfs://my_catalog/'))`: return all databases, only virtual paths in FileStatus.
- `listStatus(Path('pvfs://my_catalog/my_database'))`: return all tables, only virtual paths in FileStatus.

All paths return virtual paths, reading and writing files will actually read and write data according to the true path
of the table.

- `newInputStream(Path('pvfs://my_catalog/my_database/my_table'))`: get the real path from rest server, and use real filesystem to read data.

## Java SDK

Provide a Java SDK to implement Hadoop FileSystem. In this way, compute engines can integrate 'PVFS' very easy.

For example, Java code can do:

```java
Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.pvfs.impl", "org.apache.paimon.vfs.hadoop.Pvfs");
conf.set("fs.pvfs.impl", "org.apache.paimon.vfs.hadoop.PaimonVirtualFileSystem");
conf.set("fs.pvfs.uri", "http://localhost:10000");
conf.set("fs.pvfs.token.provider", "bear");
conf.set("fs.pvfs.token", "token");
Path path = new Path("pvfs://catalog_name/database_name/table_name/a.csv");
FileSystem fs = path.getFileSystem(conf);
FileStatus fileStatus = fs.getFileStatus(path);
```

For example, Spark SQL can do:

```scala
val spark = SparkSession.builder()
.appName("PVFS CSV Analysis")
.config("spark.hadoop.fs.pvfs.impl", "org.apache.paimon.vfs.hadoop.PaimonVirtualFileSystem")
.config("spark.hadoop.fs.pvfs.uri", "http://localhost:10000")
.config("spark.hadoop.fs.pvfs.token.provider", "bear")
.config("spark.hadoop.fs.pvfs.token", "token")
.getOrCreate()
spark.sql(
s"""
|CREATE TEMPORARY VIEW csv_table
|USING csv
|OPTIONS (
|  path 'pvfs://catalog_name/database_name/my_format_table_name/a.csv',
|  header 'true',
|  inferSchema 'true'
|)
""".stripMargin
)

spark.sql("SELECT * FROM csv_table LIMIT 5").show()
```

For example, use Hadoop shell command:

```xml
<!-- Configure following configuration in hadoop `core-site.xml` -->
<property>
  <name>fs.AbstractFileSystem.pvfs.impl</name>
  <value>org.apache.paimon.vfs.hadoop.Pvfs</value>
</property>

<property>
  <name>fs.pvfs.impl</name>
  <value>org.apache.paimon.vfs.hadoop.PaimonVirtualFileSystem</value>
</property>

<property>
  <name>fs.pvfs.uri</name>
  <value>http://localhost:10000</value>
</property>

<property>
  <name>fs.pvfs.token.provider</name>
  <value>bear</value>
</property>

<property>
  <name>fs.pvfs.token</name>
  <value>token</value>
</property>
```

Example: execute hadoop shell to list the virtual path

```shell
./${HADOOP_HOME}/bin/hadoop dfs -ls pvfs://catalog_name/database_name/table_name
```

## Python SDK

Python SDK provide fsspec style API, can be easily integrated to Python ecosystem.

For example, Python code can do:

```python
import pypaimon

options = {
    'uri': 'key',
    'token.provider': 'bear',
    'token': '<token>'
}
fs = pypaimon.PaimonVirtualFileSystem(options)
fs.ls("pvfs://catalog_name/database_name/table_name")
```

For example, Pyarrow can do:

```python
import pypaimon
import pyarrow.parquet as pq

options = {
    'uri': 'key',
    'token.provider': 'bear',
    'token': '<token>'
}
fs = pypaimon.PaimonVirtualFileSystem(options)
path = 'pvfs://catalog_name/database_name/table_name/a.parquet'
dataset = pq.ParquetDataset(path, filesystem=fs)
table = dataset.read()
df = table.to_pandas()
```

For example, Ray can do:

```python
import pypaimon
import ray

options = {
    'uri': 'key',
    'token.provider': 'bear',
    'token': '<token>'
}
fs = pypaimon.PaimonVirtualFileSystem(options)

ds = ray.data.read_parquet(filesystem=fs,paths="pvfs://....parquet")
```
