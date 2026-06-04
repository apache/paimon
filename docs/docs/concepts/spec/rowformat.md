---
title: "Row Format"
sidebar_position: 8
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

# Row Format Specification

The Row format (`.row`) is a row-oriented file format optimized for O(1) random access by row number. It is designed for scenarios where fast point lookups by row position are critical, such as deletion vector applications and changelog materialization.

## File Layout

A `.row` file consists of three sections:

```
+====================================================================+
|                        ROW FILE (.row)                              |
+====================================================================+
| Data Block 0 (ZSTD compressed)                                     |
| Data Block 1 (ZSTD compressed)                                     |
| ...                                                                |
| Data Block K (ZSTD compressed)                                     |
+--------------------------------------------------------------------+
| Block Index (Delta+ZigZag+Varint encoded)                          |
+--------------------------------------------------------------------+
| Footer (fixed 32 bytes)                                            |
+====================================================================+
```

## Data Block

Each data block is independently ZSTD Level 1 compressed. The uncompressed content has the following layout:

```
+-----------------------------------------------------------+
| row_0_bytes | row_1_bytes | ... | row_N_bytes             |
+-----------------------------------------------------------+
| offset[0] (int32 LE) | offset[1] | ... | offset[N]       |
+-----------------------------------------------------------+
| row_count (int32 LE)                                      |
+-----------------------------------------------------------+
```

- **Row data region**: Each row is serialized sequentially using the compact row format (see below).
- **Offset array**: An array of int32 little-endian values, one per row, storing the byte offset of each row within the uncompressed block.
- **Row count**: A single int32 little-endian value at the very end of the block, storing the number of rows in this block.

A new block is flushed when the estimated uncompressed size reaches the configured block size threshold (default 64 KB, configurable via `file.block-size`).

### Row Serialization Format

Each row is serialized as:

```
+-----------------------------------------------+
| null_bitmap | field_0 | field_1 | ... | field_N |
+-----------------------------------------------+
```

**Null bitmap**: `ceil(arity / 8)` bytes. Bit `i` is set (1) if field `i` is null. The bit position is `byte[i/8] & (1 << (i%8))`. Non-null fields are serialized in order; null fields occupy no space beyond the bitmap bit.

### Primitive Type Encoding

All multi-byte primitives use **little-endian** byte order.

| Paimon Type | Encoding |
|---|---|
| BOOLEAN | 1 byte: 0 = false, 1 = true |
| TINYINT | 1 byte signed |
| SMALLINT | 2 bytes int16 LE |
| INT / DATE / TIME | 4 bytes int32 LE |
| BIGINT | 8 bytes int64 LE |
| FLOAT | 4 bytes IEEE 754 LE |
| DOUBLE | 8 bytes IEEE 754 LE |
| CHAR / VARCHAR | varint(length) + UTF-8 bytes |
| BINARY / VARBINARY | varint(length) + raw bytes |
| DECIMAL(P, S) where P <= 18 | 8 bytes int64 LE (unscaled long) |
| DECIMAL(P, S) where P > 18 | varint(length) + unscaled bytes (big-endian two's complement) |
| TIMESTAMP(P) where P <= 3 | 8 bytes int64 LE (epoch millis) |
| TIMESTAMP(P) where P > 3 | 8 bytes int64 LE (epoch millis) + varint(nanoOfMillisecond) |
| VARIANT | varint(len1) + value bytes + varint(len2) + metadata bytes |

### Varint Encoding

Variable-length integer encoding (unsigned LEB128):
- Each byte uses 7 bits for data and 1 bit (MSB) as continuation flag.
- If MSB = 1, more bytes follow. If MSB = 0, this is the last byte.
- Maximum 5 bytes for int32 values.

### Complex Type Encoding

**ARRAY**:

```
varint(size) | null_bitmap[ceil(size/8) bytes] | element_0 | element_1 | ... | element_N
```

Null bitmap uses the same bit layout as row nulls. Non-null elements are serialized in order using the element type's encoding.

**MAP**:

A map is serialized as two arrays (keys array followed by values array):

```
[keys array] [values array]
```

Each array follows the ARRAY encoding above (varint size + null bitmap + elements). Both keys and values support null entries.

**ROW (nested)**:

Nested rows use the same format as top-level rows:

```
null_bitmap[ceil(arity/8) bytes] | field_0 | field_1 | ... | field_N
```

## Block Index

The block index stores metadata for all blocks, enabling binary search to locate the block containing a given row number.

```
+--------------------------------------------------------------------+
| varint(len_0) | encoded_block_compressed_sizes                      |
| varint(len_1) | encoded_block_uncompressed_sizes                    |
| varint(len_2) | encoded_block_row_starts                            |
+--------------------------------------------------------------------+
```

Each of the three arrays is encoded using **Delta + ZigZag + Varint** compression:
1. Compute deltas between consecutive values
2. ZigZag encode each delta (maps signed to unsigned)
3. Varint encode each ZigZag value

This is highly efficient for monotonically increasing sequences (row starts) and similar-valued sequences (sizes).

The arrays are:
- **blockCompressedSizes**: Compressed size of each block. Block offsets are derived by prefix sum (first block starts at file position 0).
- **blockUncompressedSizes**: Uncompressed size of each block (needed to allocate decompression buffer)
- **blockRowStarts**: Cumulative row count at the start of each block (for binary search)

## Footer

The footer is a fixed 32-byte structure at the end of the file:

```
+-----------------------------------------------+
| totalRowCount   | int64  | 8 bytes | LE       |
| blockCount      | int32  | 4 bytes | LE       |
| indexOffset      | int64  | 8 bytes | LE       |
| indexLength      | int32  | 4 bytes | LE       |
| version          | int8   | 1 byte  |          |
| reserved         |        | 3 bytes |          |
| magic            | int32  | 4 bytes | LE       |
+-----------------------------------------------+
```

- **totalRowCount**: Total number of rows in the file.
- **blockCount**: Number of data blocks.
- **indexOffset**: Byte offset in the file where the block index starts.
- **indexLength**: Length in bytes of the block index section.
- **version**: Format version, currently `1`.
- **reserved**: 3 bytes reserved for future use (must be 0).
- **magic**: `0x524F5753` (ASCII "ROWS"), used for format validation.

## Row Number Lookup Algorithm

To read a specific row by its global row number:

1. **Read Footer**: Seek to file end - 32 bytes, read the 32-byte footer. Validate magic number.
2. **Read Block Index**: Seek to `indexOffset`, read `indexLength` bytes, decode the three arrays. Compute block offsets by prefix sum of `blockCompressedSizes[]`.
3. **Binary Search**: Search `blockRowStarts[]` to find block `b` where `blockRowStarts[b] <= rowNum < blockRowStarts[b+1]`.
4. **Read Block**: Seek to `blockOffset(b)`, read `blockCompressedSizes[b]` bytes.
5. **Decompress**: ZSTD decompress into a buffer of size `blockUncompressedSizes[b]`.
6. **Locate Row**: Compute `localIdx = rowNum - blockRowStarts[b]`. Read `offsets[localIdx]` from the offset array at the end of the decompressed block.
7. **Deserialize**: Read the row starting at the computed offset using the row serialization format.

## Projection

Column projection is applied after full row deserialization. Since the compact row format serializes fields sequentially without per-field offset metadata, individual fields cannot be skipped during deserialization. After the complete row is deserialized, a projection mapping selects the requested columns.

## Selection (Deletion Vectors)

Row selection via `RoaringBitmap32` enables efficient filtering:

1. For each block, check if the selection bitmap intersects with `[blockRowStart, blockRowEnd)`.
2. If no intersection, skip the entire block (no I/O or decompression).
3. If there is an intersection, decompress the block and only deserialize the selected rows using their local indices.

## Configuration

| Option | Default | Description |
|---|---|---|
| `file.block-size` | 64 KB | Uncompressed block size threshold. Larger blocks improve compression ratio but increase read amplification for point lookups. |
| ZSTD Level | 1 | Fixed at level 1 for fast compression with reasonable ratio. |
