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

package org.apache.paimon.flink.source;

import org.apache.paimon.flink.Projection;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

/** Restores before images when a generated primary key cannot be declared in Flink's schema. */
class GeneratedPrimaryKeyChangelogNormalizer extends AbstractStreamOperator<RowData>
        implements OneInputStreamOperator<RowData, RowData> {

    private final RowType rowType;
    private transient RowDataSerializer serializer;
    private transient ValueState<RowData> previousRow;

    GeneratedPrimaryKeyChangelogNormalizer(RowType rowType) {
        this.rowType = rowType;
    }

    static DataStream<RowData> normalize(
            DataStream<RowData> input, RowType rowType, int[] primaryKeys) {
        RowType keyType = Projection.of(primaryKeys).project(rowType);
        return input.keyBy(
                        new PrimaryKeySelector(rowType, primaryKeys), InternalTypeInfo.of(keyType))
                .transform(
                        "Paimon generated primary key changelog",
                        InternalTypeInfo.of(rowType),
                        new GeneratedPrimaryKeyChangelogNormalizer(rowType));
    }

    @Override
    public void open() throws Exception {
        super.open();
        serializer = new RowDataSerializer(rowType);
        previousRow =
                getRuntimeContext()
                        .getState(new ValueStateDescriptor<>("previous-row", serializer));
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData row = element.getValue();
        RowData previous = previousRow.value();
        switch (row.getRowKind()) {
            case INSERT:
            case UPDATE_AFTER:
                if (previous != null) {
                    RowData before = serializer.copy(previous);
                    before.setRowKind(RowKind.UPDATE_BEFORE);
                    output.collect(element.copy(before));
                }
                RowData current = serializer.copy(row);
                current.setRowKind(RowKind.INSERT);
                // State and output must not share mutable row data, including with heap state.
                previousRow.update(serializer.copy(current));
                current.setRowKind(previous == null ? RowKind.INSERT : RowKind.UPDATE_AFTER);
                output.collect(element.copy(current));
                break;
            case DELETE:
                if (previous != null) {
                    RowData deleted = serializer.copy(previous);
                    deleted.setRowKind(RowKind.DELETE);
                    output.collect(element.copy(deleted));
                }
                previousRow.clear();
                break;
            default:
                throw new IllegalArgumentException(
                        "Expected an upsert changelog, but received " + row.getRowKind());
        }
    }

    /** Copies the key so mutable source rows cannot change a key already in use. */
    static class PrimaryKeySelector implements KeySelector<RowData, RowData> {

        private final RowType rowType;
        private final int[] primaryKeys;
        private final RowType keyType;
        private transient RowData.FieldGetter[] getters;
        private transient RowDataSerializer serializer;

        PrimaryKeySelector(RowType rowType, int[] primaryKeys) {
            this.rowType = rowType;
            this.primaryKeys = primaryKeys.clone();
            this.keyType = Projection.of(primaryKeys).project(rowType);
        }

        @Override
        public RowData getKey(RowData row) {
            if (getters == null) {
                getters = new RowData.FieldGetter[primaryKeys.length];
                for (int i = 0; i < primaryKeys.length; i++) {
                    getters[i] =
                            RowData.createFieldGetter(
                                    rowType.getTypeAt(primaryKeys[i]), primaryKeys[i]);
                }
                serializer = new RowDataSerializer(keyType);
            }
            GenericRowData key = new GenericRowData(primaryKeys.length);
            for (int i = 0; i < getters.length; i++) {
                key.setField(i, getters[i].getFieldOrNull(row));
            }
            return serializer.toBinaryRow(key).copy();
        }
    }
}
