---
title: "File Layouts"
weight: 3
type: docs
aliases:
- /concepts/file-layouts.html
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

# File Layouts

All files of a table are stored under one base directory. Table Store files are organized in a layered style. The following image illustrates the file layout. Starting from a snapshot file, Table Store readers can recursively access all records from the table.

{{< img src="/img/file-layout.png">}}

## Snapshot Files

All snapshot files are stored in the `snapshot` directory.

A snapshot file is a JSON file containing information about this snapshot, including

* the schema file in use
* the manifest list containing all changes of this snapshot

## Manifest Files

All manifest lists and manifest files are stored in the `manifest` directory.

A manifest list is a list of manifest file names.

A manifest file is a file containing changes about LSM data files and changelog files. For example, which LSM data file is created and which file is deleted in the corresponding snapshot.

## Data Files

Data files are grouped by partitions and buckets. Each bucket directory contains an [LSM tree]({{< ref "docs/concepts/lsm-trees" >}}) and its [changelog files]({{< ref "docs/features/table-types#changelog-producers" >}}).
