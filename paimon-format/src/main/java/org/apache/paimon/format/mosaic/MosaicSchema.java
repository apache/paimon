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

import org.apache.paimon.data.Decimal;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.RowType;

import javax.annotation.Nullable;

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
import static org.apache.paimon.format.mosaic.MosaicUtils.varintSize;
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
        int numColumns = fields.size();

        // Derive each column's bucketId from bucketMapping
        int[] globalToBucket = new int[numColumns];
        for (int b = 0; b < bucketMapping.length; b++) {
            for (int globalIdx : bucketMapping[b]) {
                globalToBucket[globalIdx] = b;
            }
        }

        List<ColumnMeta> columns = new ArrayList<>(numColumns);
        for (int i = 0; i < numColumns; i++) {
            DataField field = fields.get(i);
            columns.add(new ColumnMeta(field.id(), field.name(), field.type(), globalToBucket[i]));
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

    /**
     * For a given bucket, returns cast functions for columns that need type widening. Array index
     * is the local column index within the bucket. Null entry means no cast needed. Returns null if
     * no columns need casting.
     */
    @Nullable
    public TypeCast[] getCasts(int bucketId, RowType projectedRowType) {
        Map<String, DataType> projectedNameToType = new HashMap<>();
        for (DataField field : projectedRowType.getFields()) {
            projectedNameToType.put(field.name(), field.type());
        }

        int[] globalIndices = bucketToGlobalIndices[bucketId];
        TypeCast[] casts = new TypeCast[globalIndices.length];
        boolean hasCast = false;
        for (int localIdx = 0; localIdx < globalIndices.length; localIdx++) {
            ColumnMeta col = columns.get(globalIndices[localIdx]);
            DataType projectedType = projectedNameToType.get(col.name);
            if (projectedType != null && !col.type.equalsIgnoreNullable(projectedType)) {
                casts[localIdx] = TypeCast.resolve(projectedType);
                hasCast = true;
            }
        }
        return hasCast ? casts : null;
    }

    /** Casts a value to a wider type during schema evolution. */
    @FunctionalInterface
    public interface TypeCast {
        Object cast(Object value);

        static TypeCast resolve(DataType targetType) {
            switch (targetType.getTypeRoot()) {
                case SMALLINT:
                    return v -> ((Number) v).shortValue();
                case INTEGER:
                case DATE:
                case TIME_WITHOUT_TIME_ZONE:
                    return v -> ((Number) v).intValue();
                case BIGINT:
                    return v -> ((Number) v).longValue();
                case FLOAT:
                    return v -> ((Number) v).floatValue();
                case DOUBLE:
                    return v -> ((Number) v).doubleValue();
                case DECIMAL:
                    DecimalType dt = (DecimalType) targetType;
                    return v ->
                            v instanceof Decimal
                                    ? Decimal.fromBigDecimal(
                                            ((Decimal) v).toBigDecimal(),
                                            dt.getPrecision(),
                                            dt.getScale())
                                    : v;
                default:
                    return v -> v;
            }
        }
    }

    static final byte NAME_ENCODING_FRONT_CODE = 0;
    static final byte NAME_ENCODING_BPE = 1;

    public byte[] serialize() throws IOException {
        // Sort columns by name for front-coding (serialization order only)
        Integer[] sortOrder = new Integer[columns.size()];
        for (int i = 0; i < sortOrder.length; i++) {
            sortOrder[i] = i;
        }
        Arrays.sort(sortOrder, (a, b) -> columns.get(a).name.compareTo(columns.get(b).name));

        // Collect raw name bytes in sorted order
        byte[][] rawNames = new byte[columns.size()][];
        for (int i = 0; i < columns.size(); i++) {
            rawNames[i] = columns.get(sortOrder[i]).name.getBytes(StandardCharsets.UTF_8);
        }

        // Try mode 0: sorted front-coding only
        int plainSize = frontCodedSize(rawNames);

        // Try mode 1: BPE + front-coding (only if ASCII-only)
        byte[][] bpeRules = null;
        byte[][] bpeNames = null;
        int bpeSize = Integer.MAX_VALUE;
        if (MosaicBpe.isAsciiOnly(rawNames)) {
            bpeRules = MosaicBpe.buildVocabulary(rawNames);
            if (bpeRules.length > 0) {
                bpeNames = new byte[rawNames.length][];
                for (int i = 0; i < rawNames.length; i++) {
                    bpeNames[i] = MosaicBpe.encode(rawNames[i], bpeRules);
                }
                bpeSize = 1 + bpeRules.length * 2 + frontCodedSize(bpeNames);
            }
        }

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);

        writeVarint(out, columns.size());
        writeVarint(out, numBuckets);

        if (bpeSize < plainSize) {
            out.writeByte(NAME_ENCODING_BPE);
            writeVarint(out, bpeRules.length);
            for (byte[] rule : bpeRules) {
                out.writeByte(rule[0]);
                out.writeByte(rule[1]);
            }
            writeFrontCoded(out, bpeNames, sortOrder);
        } else {
            out.writeByte(NAME_ENCODING_FRONT_CODE);
            writeFrontCoded(out, rawNames, sortOrder);
        }

        out.flush();
        return baos.toByteArray();
    }

    private void writeFrontCoded(DataOutputStream out, byte[][] nameBytes, Integer[] sortOrder)
            throws IOException {
        byte[] prev = new byte[0];
        for (int i = 0; i < columns.size(); i++) {
            ColumnMeta col = columns.get(sortOrder[i]);
            writeVarint(out, col.fieldId);

            byte[] cur = nameBytes[i];
            int shared = commonPrefixLength(prev, cur);
            writeVarint(out, shared);
            writeVarint(out, cur.length - shared);
            out.write(cur, shared, cur.length - shared);
            prev = cur;

            MosaicTypes.writeType(out, col.type);
        }
    }

    private static int frontCodedSize(byte[][] names) {
        int size = 0;
        byte[] prev = new byte[0];
        for (byte[] name : names) {
            int shared = commonPrefixLength(prev, name);
            int suffixLen = name.length - shared;
            size += varintSize(shared) + varintSize(suffixLen) + suffixLen;
            prev = name;
        }
        return size;
    }

    public static MosaicSchema deserialize(byte[] data) throws IOException {
        DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));

        int numColumns = readVarint(in);
        int numBuckets = readVarint(in);

        byte nameEncoding = in.readByte();
        byte[][] bpeRules = null;
        if (nameEncoding == NAME_ENCODING_BPE) {
            int numRules = readVarint(in);
            bpeRules = new byte[numRules][2];
            for (int r = 0; r < numRules; r++) {
                bpeRules[r][0] = in.readByte();
                bpeRules[r][1] = in.readByte();
            }
        }

        List<ColumnMeta> columns = new ArrayList<>(numColumns);
        List<List<Integer>> bucketLists = new ArrayList<>(numBuckets);
        for (int i = 0; i < numBuckets; i++) {
            bucketLists.add(new ArrayList<>());
        }

        byte[] prevEncoded = new byte[0];
        for (int i = 0; i < numColumns; i++) {
            int fieldId = readVarint(in);

            int shared = readVarint(in);
            int suffixLen = readVarint(in);
            byte[] encoded = new byte[shared + suffixLen];
            System.arraycopy(prevEncoded, 0, encoded, 0, shared);
            in.readFully(encoded, shared, suffixLen);
            prevEncoded = encoded;

            byte[] nameBytes = bpeRules != null ? MosaicBpe.decode(encoded, bpeRules) : encoded;
            String name = new String(nameBytes, StandardCharsets.UTF_8);
            DataType type = MosaicTypes.readType(in);
            int bucketId = MosaicSpec.assignBucket(i, numColumns, numBuckets);
            columns.add(new ColumnMeta(fieldId, name, type, bucketId));
            bucketLists.get(bucketId).add(i);
        }

        int[][] bucketToGlobal = MosaicUtils.toIntArrays(bucketLists);

        return new MosaicSchema(numBuckets, columns, bucketToGlobal);
    }

    private static int commonPrefixLength(byte[] a, byte[] b) {
        int len = Math.min(a.length, b.length);
        for (int i = 0; i < len; i++) {
            if (a[i] != b[i]) {
                return i;
            }
        }
        return len;
    }

    /** Metadata for a single column. */
    public static class ColumnMeta {
        public final int fieldId;
        public final String name;
        public final DataType type;
        public final int bucketId;

        public ColumnMeta(int fieldId, String name, DataType type, int bucketId) {
            this.fieldId = fieldId;
            this.name = name;
            this.type = type;
            this.bucketId = bucketId;
        }
    }
}
