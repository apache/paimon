---
title: "Table Index"
weight: 7
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

HASH_VALUE is the hash value of the primary-key. 4 bytes, BIG_ENDIAN.

## Deletion Vectors

Deletion file is used to store the deleted records position for each data file. Each bucket has one deletion file for
primary key table.

{{< img src="/img/deletion-file.png">}}

The deletion file is a binary file, and the format is as follows:

- First, record version by a byte. Current version is 1.
- Then, record <size of serialized bin, serialized bin, checksum of serialized bin> in sequence.
- Size and checksum are BIG_ENDIAN Integer.

For each serialized bin, its serialization format is determined by `deletion-vectors.bitmap64`. 
Paimon will use a 32-bit bitmap to store deleted records by default, but if `deletion-vectors.bitmap64` is set to true, a 64-bit bitmap will be used.
Serialization of the two bitmaps is different. Note that only 64-bit bitmap implementation is compatible with Iceberg.

Serialized bin for 32-bit bitmap:(default)
- First, record a const magic number by an int (BIG_ENDIAN). Current the magic number is 1581511376.
- Then, record a 32-bit serialized bitmap. Which is a [RoaringBitmap](https://github.com/RoaringBitmap/RoaringBitmap) (org.roaringbitmap.RoaringBitmap).

Serialized bin for 64-bit bitmap:
- First, record a const magic number by an int (LITTLE_ENDIAN). Current the magic number is 1681511377.
- Then, record a 64-bit serialized bitmap. Which supports positive 64-bit positions (the most significant bit must be 0), 
  but is optimized for cases where most positions fit in 32 bits by using an array of 32-bit Roaring bitmaps. The internal bitmap array is grown as needed to accommodate the largest position.
  The serialization of the 64-bit bitmap is as follows:
  - First, record the size of bitmaps array by a long (LITTLE_ENDIAN).
  - Then, record the index by an int (LITTLE_ENDIAN) and serialized bytes of each bitmap in the array in sequence.
