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

package org.apache.flink.table.store.file;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.store.file.schema.AtomicDataType;
import org.apache.flink.table.store.file.schema.DataField;
import org.apache.flink.table.store.utils.RowDataUtils;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A key value, including user key, sequence number, value kind and value. This object can be
 * reused.
 */
public class KeyValue {
    private static final String SEQUENCE_NUMBER = "_SEQUENCE_NUMBER";
    private static final String VALUE_KIND = "_VALUE_KIND";

    public static final long UNKNOWN_SEQUENCE = -1;
    public static final int UNKNOWN_LEVEL = -1;

    private RowData key;
    // determined after written into memory table or read from file
    private long sequenceNumber;
    private RowKind valueKind;
    private RowData value;
    // determined after read from file
    private int level;

    public KeyValue replace(RowData key, RowKind valueKind, RowData value) {
        return replace(key, UNKNOWN_SEQUENCE, valueKind, value);
    }

    public KeyValue replace(RowData key, long sequenceNumber, RowKind valueKind, RowData value) {
        this.key = key;
        this.sequenceNumber = sequenceNumber;
        this.valueKind = valueKind;
        this.value = value;
        this.level = UNKNOWN_LEVEL;
        return this;
    }

    public KeyValue replaceKey(RowData key) {
        this.key = key;
        return this;
    }

    public RowData key() {
        return key;
    }

    public long sequenceNumber() {
        return sequenceNumber;
    }

    public RowKind valueKind() {
        return valueKind;
    }

    public RowData value() {
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
        List<RowType.RowField> fields = new ArrayList<>(keyType.getFields());
        fields.add(new RowType.RowField(SEQUENCE_NUMBER, new BigIntType(false)));
        fields.add(new RowType.RowField(VALUE_KIND, new TinyIntType(false)));
        fields.addAll(valueType.getFields());
        return new RowType(fields);
    }

    /**
     * Create key-value fields, we need to add a const value to the id of value field to ensure that
     * they are consistent when compared by field id. For example, there are two table with key
     * value fields as follows
     *
     * <ul>
     *   <li>Table1 key fields: 1->a, 2->b, 3->c; value fields: 0->value_count
     *   <li>Table2 key fields: 1->c, 3->d, 4->a, 5->b; value fields: 0->value_count
     * </ul>
     *
     * <p>We will use 5 as maxKeyId, and create fields for Table1/Table2 as follows
     *
     * <ul>
     *   <li>Table1 fields: 1->a, 2->b, 3->c, 6->seq, 7->kind, 8->value_count
     *   <li>Table2 fields: 1->c, 3->d, 4->a, 5->b, 6->seq, 7->kind, 8->value_count
     * </ul>
     *
     * <p>Then we can compare these two table fields with the field id.
     *
     * @param keyFields the key fields
     * @param valueFields the value fields
     * @param maxKeyId the max key id
     * @return the table fields
     */
    public static List<DataField> createKeyValueFields(
            List<DataField> keyFields, List<DataField> valueFields, final int maxKeyId) {
        checkState(maxKeyId >= keyFields.stream().mapToInt(DataField::id).max().orElse(0));

        List<DataField> fields = new ArrayList<>(keyFields.size() + valueFields.size() + 2);
        fields.addAll(keyFields);
        fields.add(
                new DataField(
                        maxKeyId + 1, SEQUENCE_NUMBER, new AtomicDataType(new BigIntType(false))));
        fields.add(
                new DataField(
                        maxKeyId + 2, VALUE_KIND, new AtomicDataType(new TinyIntType(false))));
        for (DataField valueField : valueFields) {
            DataField newValueField =
                    new DataField(
                            valueField.id() + maxKeyId + 3,
                            valueField.name(),
                            valueField.type(),
                            valueField.description());
            fields.add(newValueField);
        }

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
    public KeyValue copy(RowDataSerializer keySerializer, RowDataSerializer valueSerializer) {
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

    public static String rowDataToString(RowData row, RowType type) {
        return IntStream.range(0, type.getFieldCount())
                .mapToObj(
                        i ->
                                String.valueOf(
                                        RowDataUtils.createNullCheckingFieldGetter(
                                                        type.getTypeAt(i), i)
                                                .getFieldOrNull(row)))
                .collect(Collectors.joining(", "));
    }
}
