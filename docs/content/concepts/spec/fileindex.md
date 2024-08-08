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
  _____________________________________    _____________________
｜     magic    ｜version｜head length ｜
｜-------------------------------------｜
｜            column number            ｜
｜-------------------------------------｜
｜   column 1        ｜ index number   ｜
｜-------------------------------------｜
｜  index name 1 ｜start pos ｜length  ｜
｜-------------------------------------｜
｜  index name 2 ｜start pos ｜length  ｜
｜-------------------------------------｜
｜  index name 3 ｜start pos ｜length  ｜
｜-------------------------------------｜            HEAD
｜   column 2        ｜ index number   ｜
｜-------------------------------------｜
｜  index name 1 ｜start pos ｜length  ｜
｜-------------------------------------｜
｜  index name 2 ｜start pos ｜length  ｜
｜-------------------------------------｜
｜  index name 3 ｜start pos ｜length  ｜
｜-------------------------------------｜
｜                 ...                 ｜
｜-------------------------------------｜
｜                 ...                 ｜
｜-------------------------------------｜
｜  redundant length ｜redundant bytes ｜
｜-------------------------------------｜    ---------------------
｜                BODY                 ｜
｜                BODY                 ｜
｜                BODY                 ｜             BODY
｜                BODY                 ｜
｜_____________________________________｜    _____________________
*
magic:                            8 bytes long, value is 1493475289347502L, BIT_ENDIAN
version:                          4 bytes int, BIT_ENDIAN
head length:                      4 bytes int, BIT_ENDIAN
column number:                    4 bytes int, BIT_ENDIAN
column x name:                    2 bytes short BIT_ENDIAN and Java modified-utf-8
index number:                     4 bytes int (how many column items below), BIT_ENDIAN
index name x:                     2 bytes short BIT_ENDIAN and Java modified-utf-8
start pos:                        4 bytes int, BIT_ENDIAN
length:                           4 bytes int, BIT_ENDIAN
redundant length:                 4 bytes int (for compatibility with later versions, in this version, content is zero)
redundant bytes:                  var bytes (for compatibility with later version, in this version, is empty)
BODY:                             column index bytes + column index bytes + column index bytes + .......
</pre>

## Column Index Bytes: BloomFilter

Define `'file-index.bloom-filter.columns'`.

Content of bloom filter index is simple: 
- numHashFunctions 4 bytes int, BIT_ENDIAN
- bloom filter bytes

This class use (64-bits) long hash. Store the num hash function (one integer) and bit set bytes only. Hash bytes type 
(like varchar, binary, etc.) using xx hash, hash numeric type by [specified number hash](http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm).

## Column Index Bytes: Bitmap

Define `'file-index.bitmap.columns'`.

Bitmap file index format (V1):

<pre>
Bitmap file index format (V1)
+-------------------------------------------------+-----------------
｜ version (1 byte)                               ｜
+-------------------------------------------------+
｜ row count (4 bytes int)                        ｜
+-------------------------------------------------+
｜ non-null value bitmap number (4 bytes int)     ｜
+-------------------------------------------------+
｜ has null value (1 byte)                        ｜
+-------------------------------------------------+
｜ null value offset (4 bytes if has null value)  ｜       HEAD
+-------------------------------------------------+
｜ value 1 | offset 1                             ｜
+-------------------------------------------------+
｜ value 2 | offset 2                             ｜
+-------------------------------------------------+
｜ value 3 | offset 3                             ｜
+-------------------------------------------------+
｜ ...                                            ｜
+-------------------------------------------------+-----------------
｜ serialized bitmap 1                            ｜
+-------------------------------------------------+
｜ serialized bitmap 2                            ｜
+-------------------------------------------------+       BODY
｜ serialized bitmap 3                            ｜
+-------------------------------------------------+
｜ ...                                            ｜
+-------------------------------------------------+-----------------
*
value x:                       var bytes for any data type (as bitmap identifier)
offset:                        4 bytes int (when it is negative, it represents that there is only one value
                                 and its position is the inverse of the negative value)
</pre>

Integer are all BIT_ENDIAN.
