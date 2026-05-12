/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.format.mosaic;

import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;

import java.util.ArrayList;
import java.util.List;

/** Constants and utilities for the Mosaic file format. */
public class MosaicSpec {

    public static final byte[] MAGIC = new byte[] {'M', 'O', 'S', 'A'};
    public static final byte VERSION = 1;

    public static final int FOOTER_SIZE = 32;

    public static final byte COMPRESSION_NONE = 0;
    public static final byte COMPRESSION_ZSTD = 1;

    public static final int DEFAULT_NUM_BUCKETS = 100;

    // Column encoding types (2 bits each in encoding flags)
    public static final byte ENCODING_PLAIN = 0;
    public static final byte ENCODING_CONST = 1;
    public static final byte ENCODING_DICT = 2;
    public static final byte ENCODING_ALL_NULL = 3;

    public static int assignBucket(String fieldName, int numBuckets) {
        return Math.floorMod(fieldName.hashCode(), numBuckets);
    }

    /**
     * Groups columns by bucket. Returns an array where each element is the list of global column
     * indices assigned to that bucket.
     */
    public static int[][] groupColumnsByBucket(RowType rowType, int numBuckets) {
        List<DataField> fields = rowType.getFields();
        List<List<Integer>> buckets = new ArrayList<>(numBuckets);
        for (int i = 0; i < numBuckets; i++) {
            buckets.add(new ArrayList<>());
        }
        for (int i = 0; i < fields.size(); i++) {
            int bucketId = assignBucket(fields.get(i).name(), numBuckets);
            buckets.get(bucketId).add(i);
        }
        int[][] result = new int[numBuckets][];
        for (int i = 0; i < numBuckets; i++) {
            List<Integer> list = buckets.get(i);
            result[i] = new int[list.size()];
            for (int j = 0; j < list.size(); j++) {
                result[i][j] = list.get(j);
            }
        }
        return result;
    }

    public static byte compressionToByte(String compression) {
        if (compression == null || compression.isEmpty() || "none".equalsIgnoreCase(compression)) {
            return COMPRESSION_NONE;
        }
        if ("zstd".equalsIgnoreCase(compression)) {
            return COMPRESSION_ZSTD;
        }
        throw new IllegalArgumentException("Unsupported Mosaic compression: " + compression);
    }

    /** Metadata for a single row group. */
    public static class RowGroupMeta {
        public final int numRows;
        public final long[] bucketOffsets;
        public final int[] compressedSizes;
        public final int[] uncompressedSizes;

        public RowGroupMeta(
                int numRows, long[] bucketOffsets, int[] compressedSizes, int[] uncompressedSizes) {
            this.numRows = numRows;
            this.bucketOffsets = bucketOffsets;
            this.compressedSizes = compressedSizes;
            this.uncompressedSizes = uncompressedSizes;
        }
    }
}
