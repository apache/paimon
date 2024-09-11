---
title: "Table Index"
weight: 6
type: docs
aliases:
- /concepts/spec/tableindex.html
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

# Table index

Table Index files is in the `index` directory.

## Dynamic Bucket Index

Dynamic bucket index is used to store the correspondence between the hash value of the primary-key and the bucket.

Its structure is very simple, only storing hash values in the file:

HASH_VALUE | HASH_VALUE | HASH_VALUE | HASH_VALUE | ...

HASH_VALUE is the hash value of the primary-key. 4 bytes, BIT_ENDIAN.

## Deletion Vectors

Deletion file is used to store the deleted records position for each data file. Each bucket has one deletion file for
primary key table.

{{< img src="/img/deletion-file.png">}}

The deletion file is a binary file, and the format is as follows:

- First, record version by a byte. Current version is 1.
- Then, record <size of serialized bin, serialized bin, checksum of serialized bin> in sequence.
- Size and checksum are BIT_ENDIAN Integer.

For each serialized bin:

- First, record a const magic number by an int (BIT_ENDIAN). Current the magic number is 1581511376.
- Then, record serialized bitmap. Which is a [RoaringBitmap](https://github.com/RoaringBitmap/RoaringBitmap) (org.roaringbitmap.RoaringBitmap).
