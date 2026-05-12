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
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.format.mosaic.MosaicUtils.readVarint;
import static org.apache.paimon.format.mosaic.MosaicUtils.writeVarint;

/** Schema block for the Mosaic file format. Stores column metadata and bucket assignments. */
public class MosaicSchema {

    private final int numBuckets;
    private final List<ColumnMeta> columns;
    private final int[][] bucketToGlobalIndices;

    private MosaicSchema(int numBuckets, List<ColumnMeta> columns, int[][] bucketToGlobalIndices) {
        this.numBuckets = numBuckets;
        this.columns = columns;
        this.bucketToGlobalIndices = bucketToGlobalIndices;
    }

    public static MosaicSchema create(RowType rowType, int numBuckets) {
        int[][] bucketMapping = MosaicSpec.groupColumnsByBucket(rowType, numBuckets);
        List<DataField> fields = rowType.getFields();
        List<ColumnMeta> columns = new ArrayList<>(fields.size());

        int[] columnToBucket = new int[fields.size()];
        int[] columnToIndexInBucket = new int[fields.size()];
        for (int b = 0; b < numBuckets; b++) {
            for (int localIdx = 0; localIdx < bucketMapping[b].length; localIdx++) {
                int globalIdx = bucketMapping[b][localIdx];
                columnToBucket[globalIdx] = b;
                columnToIndexInBucket[globalIdx] = localIdx;
            }
        }

        for (int i = 0; i < fields.size(); i++) {
            DataField field = fields.get(i);
            columns.add(
                    new ColumnMeta(
                            field.id(),
                            field.name(),
                            field.type(),
                            columnToBucket[i],
                            columnToIndexInBucket[i]));
        }

        return new MosaicSchema(numBuckets, columns, bucketMapping);
    }

    public int numBuckets() {
        return numBuckets;
    }

    public int[][] bucketToGlobalIndices() {
        return bucketToGlobalIndices;
    }

    public DataType[] getBucketColumnTypes(int bucketId) {
        int[] globalIndices = bucketToGlobalIndices[bucketId];
        DataType[] types = new DataType[globalIndices.length];
        for (int i = 0; i < globalIndices.length; i++) {
            types[i] = columns.get(globalIndices[i]).type;
        }
        return types;
    }

    /** Returns the set of bucket IDs that contain at least one projected column. */
    public Set<Integer> getRequiredBuckets(RowType projectedRowType) {
        Set<String> projectedNames = new HashSet<>(projectedRowType.getFieldNames());
        Set<Integer> requiredBuckets = new HashSet<>();
        for (ColumnMeta col : columns) {
            if (projectedNames.contains(col.name)) {
                requiredBuckets.add(col.bucketId);
            }
        }
        return requiredBuckets;
    }

    /**
     * For a given bucket, returns the mapping from local column indices within the bucket to output
     * positions in the projected row. The array index is the local column index, and the value is
     * the output position (-1 means skip). Returns null if no columns in this bucket are projected.
     */
    public int[] getProjectionMapping(int bucketId, RowType projectedRowType) {
        Map<String, Integer> projectedNameToPos = new HashMap<>();
        List<String> projectedNames = projectedRowType.getFieldNames();
        for (int i = 0; i < projectedNames.size(); i++) {
            projectedNameToPos.put(projectedNames.get(i), i);
        }

        int[] globalIndices = bucketToGlobalIndices[bucketId];
        int[] localToOutput = new int[globalIndices.length];
        Arrays.fill(localToOutput, -1);
        boolean hasProjection = false;
        for (int localIdx = 0; localIdx < globalIndices.length; localIdx++) {
            ColumnMeta col = columns.get(globalIndices[localIdx]);
            Integer outputPos = projectedNameToPos.get(col.name);
            if (outputPos != null) {
                localToOutput[localIdx] = outputPos;
                hasProjection = true;
            }
        }
        return hasProjection ? localToOutput : null;
    }

    public byte[] serialize() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        writeVarint(out, columns.size());
        writeVarint(out, numBuckets);

        for (ColumnMeta col : columns) {
            writeVarint(out, col.fieldId);
            writeVarint(out, col.bucketId);
            writeVarint(out, col.indexInBucket);
            byte[] nameBytes = col.name.getBytes(StandardCharsets.UTF_8);
            writeVarint(out, nameBytes.length);
            out.write(nameBytes);
            MosaicTypes.writeType(out, col.type);
        }

        out.flush();
        return baos.toByteArray();
    }

    public static MosaicSchema deserialize(byte[] data) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));

        int numColumns = readVarint(in);
        int numBuckets = readVarint(in);

        List<ColumnMeta> columns = new ArrayList<>(numColumns);
        List<List<Integer>> bucketLists = new ArrayList<>(numBuckets);
        for (int i = 0; i < numBuckets; i++) {
            bucketLists.add(new ArrayList<>());
        }

        for (int i = 0; i < numColumns; i++) {
            int fieldId = readVarint(in);
            int bucketId = readVarint(in);
            int indexInBucket = readVarint(in);
            int nameLen = readVarint(in);
            byte[] nameBytes = new byte[nameLen];
            in.readFully(nameBytes);
            String name = new String(nameBytes, StandardCharsets.UTF_8);
            DataType type = MosaicTypes.readType(in);
            columns.add(new ColumnMeta(fieldId, name, type, bucketId, indexInBucket));
            bucketLists.get(bucketId).add(i);
        }

        int[][] bucketToGlobal = new int[numBuckets][];
        for (int b = 0; b < numBuckets; b++) {
            List<Integer> list = bucketLists.get(b);
            bucketToGlobal[b] = new int[list.size()];
            for (int j = 0; j < list.size(); j++) {
                bucketToGlobal[b][j] = list.get(j);
            }
        }

        return new MosaicSchema(numBuckets, columns, bucketToGlobal);
    }

    /** Metadata for a single column. */
    public static class ColumnMeta {
        public final int fieldId;
        public final String name;
        public final DataType type;
        public final int bucketId;
        public final int indexInBucket;

        public ColumnMeta(
                int fieldId, String name, DataType type, int bucketId, int indexInBucket) {
            this.fieldId = fieldId;
            this.name = name;
            this.type = type;
            this.bucketId = bucketId;
            this.indexInBucket = indexInBucket;
        }
    }
}
