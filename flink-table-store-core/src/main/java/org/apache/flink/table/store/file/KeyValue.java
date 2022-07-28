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
import org.apache.flink.table.store.utils.RowDataUtils;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.types.RowKind;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * A key value, including user key, sequence number, value kind and value. This object can be
 * reused.
 */
public class KeyValue {

    public static final long UNKNOWN_SEQUENCE = -1;

    private RowData key;
    private long sequenceNumber;
    private RowKind valueKind;
    private RowData value;

    public KeyValue setValue(RowData value) {
        this.value = value;
        return this;
    }

    public KeyValue replace(RowData key, RowKind valueKind, RowData value) {
        return replace(key, UNKNOWN_SEQUENCE, valueKind, value);
    }

    public KeyValue replace(RowData key, long sequenceNumber, RowKind valueKind, RowData value) {
        this.key = key;
        this.sequenceNumber = sequenceNumber;
        this.valueKind = valueKind;
        this.value = value;
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

    public static RowType schema(RowType keyType, RowType valueType) {
        List<RowType.RowField> fields = new ArrayList<>(keyType.getFields());
        fields.add(new RowType.RowField("_SEQUENCE_NUMBER", new BigIntType(false)));
        fields.add(new RowType.RowField("_VALUE_KIND", new TinyIntType(false)));
        fields.addAll(valueType.getFields());
        return new RowType(fields);
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
                        valueSerializer.copy(value));
    }

    @VisibleForTesting
    public String toString(RowType keyType, RowType valueType) {
        String keyString = rowDataToString(key, keyType);
        String valueString = rowDataToString(value, valueType);
        return "{kind: "
                + valueKind.name()
                + ", seq: "
                + sequenceNumber
                + ", key: ("
                + keyString
                + "), value: ("
                + valueString
                + ")}";
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
