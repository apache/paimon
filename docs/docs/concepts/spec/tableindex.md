---
title: "Table Index"
sidebar_position: 7
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

# Table Index

Table index files normally live in the `index` directory and are referenced through an
[index manifest](./manifest#index-manifest). Configured external paths or
`index-file-in-data-file-dir` can change their location. These indexes differ from
[per-file column indexes](./fileindex).

This page specifies dynamic-bucket hash indexes and deletion vectors. For global query indexes,
see [Global Index](../../primary-key-table/global-index) and the
[index manifest metadata](./manifest#index-manifest). To inspect the index files of a table, query
the [`table_indexes` system table](../system-tables#table-indexes-table).

## Dynamic Bucket Index

A dynamic-bucket index tracks the primary-key hashes assigned to a bucket. Each index file stores
a sequence of 4-byte, big-endian hash values; the index manifest identifies the partition and
bucket to which the file belongs.

```text
hash_0 (4 bytes) | hash_1 (4 bytes) | hash_2 (4 bytes) | ...
```

## Deletion Vectors

A deletion vector records deleted row positions in a data file. A deletion file stores zero or
more serialized deletion vectors. The index manifest metadata maps each data file to its
vector's offset, length, and cardinality; the binary payload itself does not contain file names.

Bucketed primary-key tables maintain deletion files per bucket when deletion vectors are enabled.
Other write paths can group vectors into files according to the configured target size.

[![A deletion file contains a version byte followed by payload-size, bitmap-payload, and CRC32 blocks. The payload has distinct 32-bit and 64-bit bitmap encodings.](/img/concepts-deletion-vectors.svg)](/img/concepts-deletion-vectors.svg)

The deletion file is a binary file, and the format is as follows:

- First, record version by a byte. Current version is 1.
- Then, repeat `payload size | serialized payload | CRC32 checksum` for each vector.
- Payload size and checksum are 4-byte BIG_ENDIAN integers. The payload includes its magic number.

The payload-size field above is distinct from the vector length recorded in index metadata.
In the current format, the metadata length for a 32-bit vector counts only the payload; for a
64-bit vector it also counts the 4-byte size field and the 4-byte checksum.

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
