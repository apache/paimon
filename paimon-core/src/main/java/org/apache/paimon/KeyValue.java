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

package org.apache.paimon;

import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.paimon.table.SystemFields.LEVEL;
import static org.apache.paimon.table.SystemFields.SEQUENCE_NUMBER;
import static org.apache.paimon.table.SystemFields.VALUE_KIND;

/**
 * A key value, including user key, sequence number, value kind and value. This object can be
 * reused.
 */
public class KeyValue {

    public static final long UNKNOWN_SEQUENCE = -1;
    public static final int UNKNOWN_LEVEL = -1;

    private InternalRow key;
    // determined after written into memory table or read from file
    private long sequenceNumber;
    private RowKind valueKind;
    private InternalRow value;
    // determined after read from file
    private int level;

    public KeyValue replace(InternalRow key, RowKind valueKind, InternalRow value) {
        return replace(key, UNKNOWN_SEQUENCE, valueKind, value);
    }

    public KeyValue replace(
            InternalRow key, long sequenceNumber, RowKind valueKind, InternalRow value) {
        this.key = key;
        this.sequenceNumber = sequenceNumber;
        this.valueKind = valueKind;
        this.value = value;
        this.level = UNKNOWN_LEVEL;
        return this;
    }

    public KeyValue replaceKey(InternalRow key) {
        this.key = key;
        return this;
    }

    public KeyValue replaceValue(InternalRow value) {
        this.value = value;
        return this;
    }

    public KeyValue replaceValueKind(RowKind valueKind) {
        this.valueKind = valueKind;
        return this;
    }

    public InternalRow key() {
        return key;
    }

    public long sequenceNumber() {
        return sequenceNumber;
    }

    public RowKind valueKind() {
        return valueKind;
    }

    public boolean isAdd() {
        return valueKind.isAdd();
    }

    public InternalRow value() {
        return value;
    }

    public int level() {
        return level;
    }

    public KeyValue setLevel(int level) {
        this.level = level;
        return this;
    }

    public static RowType schema(RowType keyType, RowType valueType) {
        return new RowType(createKeyValueFields(keyType.getFields(), valueType.getFields()));
    }

    public static RowType schemaWithLevel(RowType keyType, RowType valueType) {
        List<DataField> fields = new ArrayList<>(schema(keyType, valueType).getFields());
        fields.add(LEVEL);
        return new RowType(fields);
    }

    /**
     * Create key-value fields.
     *
     * @param keyFields the key fields
     * @param valueFields the value fields
     * @return the table fields
     */
    public static List<DataField> createKeyValueFields(
            List<DataField> keyFields, List<DataField> valueFields) {
        List<DataField> fields = new ArrayList<>(keyFields.size() + valueFields.size() + 2);
        fields.addAll(keyFields);
        fields.add(SEQUENCE_NUMBER);
        fields.add(VALUE_KIND);
        fields.addAll(valueFields);
        return fields;
    }

    public static int[][] project(
            int[][] keyProjection, int[][] valueProjection, int numKeyFields) {
        int[][] projection = new int[keyProjection.length + 2 + valueProjection.length][];

        // key
        for (int i = 0; i < keyProjection.length; i++) {
            projection[i] = new int[keyProjection[i].length];
            System.arraycopy(keyProjection[i], 0, projection[i], 0, keyProjection[i].length);
        }

        // seq
        projection[keyProjection.length] = new int[] {numKeyFields};

        // value kind
        projection[keyProjection.length + 1] = new int[] {numKeyFields + 1};

        // value
        for (int i = 0; i < valueProjection.length; i++) {
            int idx = keyProjection.length + 2 + i;
            projection[idx] = new int[valueProjection[i].length];
            System.arraycopy(valueProjection[i], 0, projection[idx], 0, valueProjection[i].length);
            projection[idx][0] += numKeyFields + 2;
        }

        return projection;
    }

    @VisibleForTesting
    public KeyValue copy(
            InternalRowSerializer keySerializer, InternalRowSerializer valueSerializer) {
        return new KeyValue()
                .replace(
                        keySerializer.copy(key),
                        sequenceNumber,
                        valueKind,
                        valueSerializer.copy(value))
                .setLevel(level);
    }

    @VisibleForTesting
    public String toString(RowType keyType, RowType valueType) {
        String keyString = rowDataToString(key, keyType);
        String valueString = rowDataToString(value, valueType);
        return String.format(
                "{kind: %s, seq: %d, key: (%s), value: (%s), level: %d}",
                valueKind.name(), sequenceNumber, keyString, valueString, level);
    }

    public static String rowDataToString(InternalRow row, RowType type) {
        return IntStream.range(0, type.getFieldCount())
                .mapToObj(
                        i ->
                                String.valueOf(
                                        InternalRowUtils.createNullCheckingFieldGetter(
                                                        type.getTypeAt(i), i)
                                                .getFieldOrNull(row)))
                .collect(Collectors.joining(", "));
    }
}
