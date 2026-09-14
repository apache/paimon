---
title: "PVFS"
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

# Paimon Virtual Storage

Paimon Virtual Storage (PVFS) addresses files by catalog, database, and table name instead of
requiring callers to know the physical storage path:

```text
pvfs://catalog_name/database_name/table_name/path/to/file
```

PVFS resolves these names through the REST Catalog, then performs file operations on the underlying
storage using its storage configuration and credentials. It supports
[Paimon Tables, Format Tables, and Object Tables](./tables).

PVFS provides file access. To read a Paimon table's logical rows, including snapshot selection and
primary-key merging, use a Paimon table reader. Writing files through PVFS does not commit a Paimon
snapshot.

## API Behavior

The catalog name in the URI selects the server-side catalog instance. Configure the REST service
endpoint separately with `fs.pvfs.uri` in Hadoop or `uri` in Python.

| Path | Listing result |
| --- | --- |
| `pvfs://my_catalog/` | Databases in the catalog. |
| `pvfs://my_catalog/my_database/` | Tables in the database. |
| `pvfs://my_catalog/my_database/my_table/` | Files and directories at the table's storage location. |

Listing results use virtual paths. Opening a file such as
`pvfs://my_catalog/my_database/my_table/a.csv` resolves the table's storage location and reads the
corresponding physical file. A catalog, database, or table root is a directory, not an input file.

## Java SDK

Add the `paimon-vfs-hadoop` JAR and the required storage dependencies to your application's
classpath. Hadoop configuration keys prefixed with `fs.pvfs.` are passed to the REST client with
that prefix removed. The examples use [Bearer authentication](./bear); use the appropriate
[DLF options](./dlf) for a DLF service.

### Hadoop FileSystem

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

Configuration conf = new Configuration();
conf.set("fs.AbstractFileSystem.pvfs.impl", "org.apache.paimon.vfs.hadoop.Pvfs");
conf.set("fs.pvfs.impl", "org.apache.paimon.vfs.hadoop.PaimonVirtualFileSystem");
conf.set("fs.pvfs.uri", "http://localhost:10000");
conf.set("fs.pvfs.token.provider", "bear");
conf.set("fs.pvfs.token", "<token>");

Path path = new Path("pvfs://catalog_name/database_name/table_name/a.csv");
FileSystem fs = path.getFileSystem(conf);
try (FSDataInputStream input = fs.open(path)) {
    byte[] buffer = new byte[4096];
    int bytesRead = input.read(buffer);
    // Process the bytes read from the file.
}
```

### Spark

Spark forwards `spark.hadoop.*` settings to Hadoop. For example, read a CSV file from a Format
Table with Scala:

```scala
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder()
  .appName("PVFS CSV Analysis")
  .config("spark.hadoop.fs.pvfs.impl", "org.apache.paimon.vfs.hadoop.PaimonVirtualFileSystem")
  .config("spark.hadoop.fs.pvfs.uri", "http://localhost:10000")
  .config("spark.hadoop.fs.pvfs.token.provider", "bear")
  .config("spark.hadoop.fs.pvfs.token", "<token>")
  .getOrCreate()

val data = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("pvfs://catalog_name/database_name/my_format_table/a.csv")

data.show(5)
```

### Hadoop Shell

Add the following properties inside the `<configuration>` element of `core-site.xml`, and make
the PVFS JAR available to the Hadoop command:

```xml
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
  <value>YOUR_TOKEN</value>
</property>
```

```shell
"$HADOOP_HOME/bin/hadoop" fs -ls pvfs://catalog_name/database_name/table_name/
```

## Python SDK

The Python SDK exposes an `fsspec`-compatible filesystem. Configure the REST endpoint and
authentication, then use virtual paths for file operations:

```python
import pypaimon

options = {
    "uri": "http://localhost:10000",
    "token.provider": "bear",
    "token": "<token>",
}
fs = pypaimon.PaimonVirtualFileSystem(options)
files = fs.ls("pvfs://catalog_name/database_name/table_name/")

with fs.open("pvfs://catalog_name/database_name/table_name/a.csv", "rb") as source:
    first_bytes = source.read(4096)
```

The following examples reuse this `fs` instance to read Parquet files from a Format Table or an
Object Table.

### PyArrow

```python
import pyarrow.parquet as pq

path = "pvfs://catalog_name/database_name/table_name/a.parquet"
dataset = pq.ParquetDataset(path, filesystem=fs)
table = dataset.read()
df = table.to_pandas()
```

### Ray

```python
import ray

dataset = ray.data.read_parquet(
    paths="pvfs://catalog_name/database_name/table_name/a.parquet",
    filesystem=fs,
)
```
