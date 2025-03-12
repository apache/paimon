---
title: "SQL Functions"
weight: 2
type: docs
aliases:
- /spark/sql-functions.html
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

# SQL Functions

This section introduce all available Paimon Spark functions.


## max_pt

`max_pt($table_name)`

It accepts a string type literal to specify the table name and return a max-valid-toplevel partition value.
- **valid**: the partition which contains data files
- **toplevel**: only return the first partition value if the table has multi-partition columns

It would throw exception when:
- the table is not a partitioned table
- the partitioned table does not have partition
- all of the partitions do not contains data files

**Example**

```shell
> SELECT max_pt('t');
 20250101
 
> SELECT * FROM t where pt = max_pt('t');
 a, 20250101
```

**Since: 1.1.0**
