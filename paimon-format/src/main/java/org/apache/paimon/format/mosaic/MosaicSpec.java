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
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/** Constants and utilities for the Mosaic file format. */
public class MosaicSpec {

    public static final byte[] MAGIC = new byte[] {'M', 'O', 'S', 'A'};
    public static final byte VERSION = 1;

    public static final int FOOTER_SIZE = 32;

    public static final byte COMPRESSION_NONE = 0;
    public static final byte COMPRESSION_ZSTD = 1;

    // Column encoding types (2 bits each in encoding flags)
    public static final byte ENCODING_PLAIN = 0;
    public static final byte ENCODING_CONST = 1;
    public static final byte ENCODING_DICT = 2;
    public static final byte ENCODING_ALL_NULL = 3;

    /** Range-based bucket assignment: columns sorted by name are evenly distributed. */
    public static int assignBucket(int sortedPosition, int numColumns, int numBuckets) {
        return (int) ((long) sortedPosition * numBuckets / numColumns);
    }

    /**
     * Groups columns by bucket using range-based assignment. Columns are sorted by name, then
     * evenly distributed across buckets. Returns an array where each element is the list of global
     * column indices assigned to that bucket (in name-sorted order within each bucket).
     */
    public static int[][] groupColumnsByBucket(RowType rowType, int numBuckets) {
        List<DataField> fields = rowType.getFields();
        int numColumns = fields.size();

        Integer[] sortedIndices = new Integer[numColumns];
        for (int i = 0; i < numColumns; i++) {
            sortedIndices[i] = i;
        }
        Arrays.sort(sortedIndices, Comparator.comparing(a -> fields.get(a).name()));

        List<List<Integer>> buckets = new ArrayList<>(numBuckets);
        for (int i = 0; i < numBuckets; i++) {
            buckets.add(new ArrayList<>());
        }
        for (int sortedPos = 0; sortedPos < numColumns; sortedPos++) {
            int bucketId = assignBucket(sortedPos, numColumns, numBuckets);
            buckets.get(bucketId).add(sortedIndices[sortedPos]);
        }

        return MosaicUtils.toIntArrays(buckets);
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
