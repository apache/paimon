---
title: "File Index"
weight: 7
type: docs
aliases:
- /concepts/spec/fileindex.html
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

# File index

Define `file-index.${index_type}.columns`, Paimon will create its corresponding index file for each file. If the index
file is too small, it will be stored directly in the manifest, or in the directory of the data file. Each data file
corresponds to an index file, which has a separate file definition and can contain different types of indexes with
multiple columns.

## Index File

File index file format. Put all column and offset in the header.

<pre>
 ______________________________________    _____________________
|     magic    ｜version｜head length  |
|--------------------------------------|
|            column number             |
|--------------------------------------|
|   column 1        ｜ index number    |
|--------------------------------------|
|  index name 1 ｜start pos ｜length   |
|--------------------------------------|
|  index name 2 ｜start pos ｜length   |
|--------------------------------------|
|  index name 3 ｜start pos ｜length   |
|--------------------------------------|            HEAD
|   column 2        ｜ index number    |
|--------------------------------------|
|  index name 1 ｜start pos ｜length   |
|--------------------------------------|
|  index name 2 ｜start pos ｜length   |
|--------------------------------------|
|  index name 3 ｜start pos ｜length   |
|--------------------------------------|
|                 ...                  |
|--------------------------------------|
|                 ...                  |
|--------------------------------------|
|  redundant length ｜redundant bytes  |
|--------------------------------------|    ---------------------
|                BODY                  |
|                BODY                  |
|                BODY                  |             BODY
|                BODY                  |
|______________________________________|    _____________________
*
magic:                            8 bytes long, value is 1493475289347502L, BIG_ENDIAN
version:                          4 bytes int, BIG_ENDIAN
head length:                      4 bytes int, BIG_ENDIAN
column number:                    4 bytes int, BIG_ENDIAN
column x name:                    var bytes, Java modified-utf-8
index number:                     4 bytes int (how many column items below), BIG_ENDIAN
index name x:                     var bytes, Java modified-utf-8
start pos:                        4 bytes int, BIG_ENDIAN
length:                           4 bytes int, BIG_ENDIAN
redundant length:                 4 bytes int (for compatibility with later versions, in this version, content is zero)
redundant bytes:                  var bytes (for compatibility with later version, in this version, is empty)
BODY:                             column index bytes + column index bytes + column index bytes + .......
</pre>

## Index: BloomFilter 

Define `'file-index.bloom-filter.columns'`.

Content of bloom filter index is simple: 
- numHashFunctions 4 bytes int, BIG_ENDIAN
- bloom filter bytes

This class use (64-bits) long hash. Store the num hash function (one integer) and bit set bytes only. Hash bytes type 
(like varchar, binary, etc.) using xx hash, hash numeric type by [specified number hash](http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm).

## Index: Bitmap

* `file-index.bitmap.columns`: specify the columns that need bitmap index.
* `file-index.bitmap.<column_name>.index-block-size`: to config secondary index block size, default value is 16kb.

{{< tabs "bitmap" >}}

{{< tab "V2" >}}

Bitmap file index format (V2):

<pre>

Bitmap file index format (V2)
+-------------------------------------------------+-----------------
｜ version (1 byte) = 2                           ｜
+-------------------------------------------------+
｜ row count (4 bytes int)                        ｜
+-------------------------------------------------+
｜ non-null value bitmap number (4 bytes int)     ｜
+-------------------------------------------------+
｜ has null value (1 byte)                        ｜
+-------------------------------------------------+
｜ null value offset (4 bytes if has null value)  ｜       HEAD
+-------------------------------------------------+
｜ null bitmap length (4 bytes if has null value) ｜
+-------------------------------------------------+
｜ bitmap index block number (4 bytes int)        ｜
+-------------------------------------------------+
｜ value 1 | offset 1                             ｜
+-------------------------------------------------+
｜ value 2 | offset 2                             ｜
+-------------------------------------------------+
｜ ...                                            ｜
+-------------------------------------------------+
｜ bitmap body offset (4 bytes int)               ｜
+-------------------------------------------------+-----------------
｜ bitmap index block 1                           ｜
+-------------------------------------------------+
｜ bitmap index block 2                           ｜  INDEX BLOCKS
+-------------------------------------------------+
｜ ...                                            ｜
+-------------------------------------------------+-----------------
｜ serialized bitmap 1                            ｜
+-------------------------------------------------+
｜ serialized bitmap 2                            ｜
+-------------------------------------------------+  BITMAP BLOCKS
｜ serialized bitmap 3                            ｜
+-------------------------------------------------+
｜ ...                                            ｜
+-------------------------------------------------+-----------------

index block format:
+-------------------------------------------------+
｜ entry number (4 bytes int)                     ｜
+-------------------------------------------------+
｜ value 1 | offset 1 | length 1                  ｜
+-------------------------------------------------+
｜ value 2 | offset 2 | length 2                  ｜
+-------------------------------------------------+
｜ ...                                            ｜
+-------------------------------------------------+

value x:                       var bytes for any data type (as bitmap identifier)
offset:                        4 bytes int (when it is negative, it represents that there is only one value
                                 and its position is the inverse of the negative value)
length:                        4 bytes int
  
</pre>

{{< /tab >}}

{{< tab "V1 (Legacy)" >}}

(Legacy) Bitmap file index format (V1):

You can configure `file-index.bitmap.<column_name>.version` to use legacy bitmap version 1.

<pre>

Bitmap file index format (V1)
+-------------------------------------------------+-----------------
| version (1 byte)                                |
+-------------------------------------------------+
| row count (4 bytes int)                         |
+-------------------------------------------------+
| non-null value bitmap number (4 bytes int)      |
+-------------------------------------------------+
| has null value (1 byte)                         |
+-------------------------------------------------+
| null value offset (4 bytes if has null value)   |       HEAD
+-------------------------------------------------+
| value 1 | offset 1                              |
+-------------------------------------------------+
| value 2 | offset 2                              |
+-------------------------------------------------+
| value 3 | offset 3                              |
+-------------------------------------------------+
| ...                                             |
+-------------------------------------------------+-----------------
| serialized bitmap 1                             |
+-------------------------------------------------+
| serialized bitmap 2                             |
+-------------------------------------------------+       BODY
| serialized bitmap 3                             |
+-------------------------------------------------+
| ...                                             |
+-------------------------------------------------+-----------------
*
value x:                       var bytes for any data type (as bitmap identifier)
offset:                        4 bytes int (when it is negative, it represents that there is only one value
                                 and its position is the inverse of the negative value)
</pre>

{{< /tab >}}

{{< /tabs >}}

Integers are all BIG_ENDIAN.

Bitmap only support the following data type: TinyIntType, SmallIntType, IntType, BigIntType, DateType, TimeType,
LocalZonedTimestampType, TimestampType, CharType, VarCharType, StringType, BooleanType.

## Index: Bit-Slice Index Bitmap

BSI file index is a numeric range index, used to accelerate range query, it can be used with bitmap index.

Define `'file-index.bsi.columns'`.

BSI file index format (V1):

<pre>
BSI file index format (V1)
+-------------------------------------------------+
| version (1 byte)                                |
+-------------------------------------------------+
| row count (4 bytes int)                         |
+-------------------------------------------------+
| has positive value (1 byte)                     |
+-------------------------------------------------+
| positive BSI serialized (if has positive value) |       
+-------------------------------------------------+
| has negative value (1 byte)                     |
+-------------------------------------------------+
| negative BSI serialized (if has negative value) |       
+-------------------------------------------------+
</pre>

BSI serialized format (V1):
<pre>
BSI serialized format (V1)
+-------------------------------------------------+
| version (1 byte)                                |
+-------------------------------------------------+
| min value (8 bytes long)                        |
+-------------------------------------------------+
| max value (8 bytes long)                        |
+-------------------------------------------------+
| serialized existence bitmap                     |       
+-------------------------------------------------+
| bit slice bitmap count (4 bytes int)            |
+-------------------------------------------------+
| serialized bit 0 bitmap                         |
+-------------------------------------------------+
| serialized bit 1 bitmap                         |
+-------------------------------------------------+
| serialized bit 2 bitmap                         |
+-------------------------------------------------+
| ...                                             |
+-------------------------------------------------+
</pre>

BSI only support the following data type: TinyIntType, SmallIntType, IntType, BigIntType, DateType, LocalZonedTimestamp,
TimestampType, DecimalType.
