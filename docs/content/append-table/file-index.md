---
title: "File Index"
weight: 4
type: docs
aliases:
- /append-table/file-index.html
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

# Data File Index

Define `file-index.bloom-filter.columns`, Paimon will create its corresponding index file for each file. If the index
file is too small, it will be stored directly in the manifest, or in the directory of the data file. Each data file
corresponds to an index file, which has a separate file definition and can contain different types of indexes with
multiple columns.

## Concept

Data file index is an external index file corresponding to a certain data file. If the index file is too small, it will
be stored directly in the manifest, otherwise in the directory of the data file. Each data file corresponds to an index file,
which has a separate file definition and can contain different types of indexes with multiple columns.

## Usage

Different file index may be efficient in different scenario. For example bloom filter may speed up query in point lookup
scenario. Using a bitmap may consume more space but can result in greater accuracy. Though we only realize bloom filter
currently, but other types of index will be supported in the future.

Currently, file index is only supported in append-only table.

`Bloom Filter`:
* `file-index.bloom-filter.columns`: specify the columns that need bloom filter index.
* `file-index.bloom-filter.<column_name>.fpp` to config false positive probability.
* `file-index.bloom-filter.<column_name>.items` to config the expected distinct items in one data file.

More filter types will be supported...

## Procedure

If you want to add file index to existing table, without any rewrite, you can use `rewrite_file_index` procedure. Before
we use the procedure, you should config appropriate configurations in target table. You can use ALTER clause to config
`file-index.<filter-type>.columns` to the table.

How to invoke: see [flink procedures]({{< ref "flink/procedures#procedures" >}}) 
