---
title: "Migration From Hive"
weight: 1
type: docs
aliases:
- /migration/migration-from-hive.html
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

# Hive Table Migration

Apache Hive supports ORC, Parquet file formats that could be migrated to Paimon. 
When migrating data to a paimon table, the origin table will be permanently disappeared. So please back up your data if you
still need the original table. The migrated table will be [unware-bucket append-only table]({{< ref "concepts/append-only-table#append-for-scalable-table" >}}).

Now, we can use flink generic catalog with Migrate Table Procedure and Migrate File Procedure to totally migrate a table from hive to paimon.

* Migrate Table Procedure: Paimon table does not exist, use the procedure upgrade hive table to paimon table. Hive table will disappear after action done.
* Migrate File Procedure:  Paimon table already exists, use the procedure to migrate files from hive table to paimon table. **Notice that, Hive table will also disappear after action done.**

<span style="color: red; "> **We highly recomment to back up hive table data before migrating, because migrating action is not atomic. If been interrupted while migrating, you may lose your data.** </span>

## Example for Migration

**Migrate Hive Table**

Command: <br>

***CALL <font color="green">sys.migrate_table</font>(&#39;hive&#39;, &#39;&lt;hive_database&gt;.&lt;hive_tablename&gt;&#39;, &#39;&lt;paimon_tableconf&gt;&#39;);***

**Example**

```sql
CREATE CATALOG PAIMON WITH ('type'='paimon', 'metastore' = 'hive', 'uri' = 'thrift://localhost:9083', 'warehouse'='/path/to/warehouse/');

USE CATALOG PAIMON;

CALL sys.migrate_table('hive', 'default.hivetable', 'file.format=orc');
```
After invoke, hivetable will totally convert to paimon format. Writing and reading the table by old "hive way" will fail.
We can add our table properties while importing by sys.migrate_table('<database>.<tablename>', '<tableproperties>').
<tableproperties> here should be separated by ",".  For example:

```sql
CALL sys.migrate_table('my_db.wait_to_upgrate', 'file.format=orc,read.batch-size=2096,write-only=true')
```

**Migrate Hive File**

Command: <br>

***CALL <font color="green">sys.migrate_file</font>(&#39;hive&#39;, &#39;&lt;hive_database&gt;.&lt;hive_table_name&gt;&#39;, &#39;&lt;paimon_database&gt;.&lt;paimon_tablename&gt;&#39;);***

**Example**

```sql
CREATE CATALOG PAIMON WITH ('type'='paimon-generic', 'hive-conf-dir' = '/path/to/hive-site.xml');

USE CATALOG PAIMON;

CALL sys.migrate_file('hive', 'default.hivetable', 'default.paimontable');
```
After invoke, "hivetable" will disappear. And all files will be moved and renamed to paimon directory. "paimontable" here must have the same
partition keys with "hivetable", and paimontabl should be in unaware-bucket mode.
