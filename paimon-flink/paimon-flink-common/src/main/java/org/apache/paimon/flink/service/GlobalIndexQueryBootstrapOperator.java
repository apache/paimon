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

package org.apache.paimon.flink.service;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.data.serializer.InternalSerializers;
import org.apache.paimon.flink.utils.RuntimeContextUtils;
import org.apache.paimon.reader.RecordReaderIterator;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils;
import org.apache.paimon.table.query.GlobalIndexQueryServiceUtils.QuerySpec;
import org.apache.paimon.table.source.InnerTableRead;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.SplitSerializer;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.ProjectedRow;

import org.apache.flink.metrics.Counter;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.List;

import static org.apache.paimon.table.query.GlobalIndexQueryServiceUtils.normalizeKey;
import static org.apache.paimon.utils.SerializationUtils.serializeBinaryRow;

/** Reads one pinned-snapshot shard and materializes key/value rows for key-hash executors. */
public class GlobalIndexQueryBootstrapOperator extends AbstractStreamOperator<InternalRow>
        implements OneInputStreamOperator<InternalRow, InternalRow> {

    private static final long serialVersionUID = 1L;

    public static final int START = 0;
    public static final int PUT = 1;
    public static final int COMPLETE = 2;
    public static final int NOT_READY = 3;

    public static final int TARGET = 3;

    private final FileStoreTable table;
    private final String lookupField;
    private final List<String> valueFields;
    private final int numExecutors;

    private transient QuerySpec spec;
    private transient RowType readType;
    private transient InternalRowSerializer keySerializer;
    private transient InternalRowSerializer valueSerializer;
    private transient ProjectedRow keyProjection;
    private transient ProjectedRow valueProjection;
    private transient Counter nullLookupKeysSkipped;
    private transient InnerTableRead read;

    public GlobalIndexQueryBootstrapOperator(
            FileStoreTable table, String lookupField, List<String> valueFields, int numExecutors) {
        this.table = table;
        this.lookupField = lookupField;
        this.valueFields = valueFields;
        this.numExecutors = numExecutors;
    }

    public static RowType outputType() {
        return RowType.of(
                DataTypes.BIGINT(),
                DataTypes.BIGINT(),
                DataTypes.INT(),
                DataTypes.INT(),
                DataTypes.INT(),
                DataTypes.BYTES(),
                DataTypes.BYTES(),
                DataTypes.STRING());
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.spec = GlobalIndexQueryServiceUtils.querySpec(table, lookupField, valueFields);
        this.readType = table.rowType().project(spec.bootstrapProjection());
        this.keySerializer = InternalSerializers.create(readType.project(new int[] {0}));
        int[] valuePositions = new int[valueFields.size()];
        for (int i = 0; i < valuePositions.length; i++) {
            valuePositions[i] = i + 1;
        }
        this.valueSerializer = InternalSerializers.create(readType.project(valuePositions));
        this.keyProjection = ProjectedRow.from(new int[] {0});
        this.valueProjection = ProjectedRow.from(valuePositions);
        this.read = table.newRead().withReadType(readType);
        this.nullLookupKeysSkipped =
                getRuntimeContext()
                        .getMetricGroup()
                        .counter("globalIndexQueryNullLookupKeysSkipped");
    }

    @Override
    public void processElement(StreamRecord<InternalRow> streamRecord) throws Exception {
        InternalRow event = streamRecord.getValue();
        long generation = event.getLong(0);
        long snapshotId = event.getLong(1);
        int monitorType = event.getInt(2);
        String reason = event.getString(5).toString();
        int bootstrapId = RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext());

        if (monitorType == GlobalIndexQuerySnapshotMonitor.NOT_READY) {
            for (int target = 0; target < numExecutors; target++) {
                emit(generation, snapshotId, NOT_READY, target, bootstrapId, null, null, reason);
            }
            return;
        }
        if (monitorType == GlobalIndexQuerySnapshotMonitor.START) {
            // Every target observes START before any PUT from this input channel.
            for (int target = 0; target < numExecutors; target++) {
                emit(generation, snapshotId, START, target, bootstrapId, null, null, "");
            }
            return;
        }
        if (monitorType == GlobalIndexQuerySnapshotMonitor.SPLIT) {
            Split split = SplitSerializer.deserialize(event.getBinary(4));
            try (RecordReaderIterator<InternalRow> rows =
                    new RecordReaderIterator<>(read.createReader(split))) {
                while (rows.hasNext()) {
                    InternalRow row = rows.next();
                    if (row.isNullAt(0)) {
                        // Null is outside the service lookup domain. Non-null keys remain globally
                        // unique; callers receive a validation error rather than a false MISS.
                        nullLookupKeysSkipped.inc();
                        continue;
                    }
                    BinaryRow key =
                            normalizeKey(keySerializer.toBinaryRow(keyProjection.replaceRow(row)));
                    BinaryRow value =
                            valueSerializer.toBinaryRow(valueProjection.replaceRow(row)).copy();
                    int target = GlobalIndexQueryServiceUtils.route(key, numExecutors);
                    emit(
                            generation,
                            snapshotId,
                            PUT,
                            target,
                            bootstrapId,
                            serializeBinaryRow(key),
                            serializeBinaryRow(value),
                            "");
                }
            }
            return;
        }
        if (monitorType == GlobalIndexQuerySnapshotMonitor.COMPLETE) {
            for (int target = 0; target < numExecutors; target++) {
                emit(generation, snapshotId, COMPLETE, target, bootstrapId, null, null, "");
            }
            return;
        }
        throw new IllegalArgumentException(
                "Unknown global-index monitor event type " + monitorType);
    }

    private void emit(
            long generation,
            long snapshotId,
            int type,
            int target,
            int bootstrapId,
            byte[] key,
            byte[] value,
            String message) {
        output.collect(
                new StreamRecord<>(
                        GenericRow.of(
                                generation,
                                snapshotId,
                                type,
                                target,
                                bootstrapId,
                                key,
                                value,
                                BinaryString.fromString(message))));
    }
}
