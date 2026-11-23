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

package org.apache.paimon.flink.globalindex;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.shuffle.RangeShuffle;
import org.apache.paimon.globalindex.sorted.NullBitmapBuffer;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowUtils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;

import java.io.IOException;

/** Compresses scalar null rows only after their shuffle range has been assigned. */
class NullBitmapShuffleOperator
        extends TableStreamOperator<Tuple2<Integer, Tuple2<InternalRow, RowData>>>
        implements RangeShuffle.AssignedRangeOperator<InternalRow>, BoundedOneInput {

    private static final long serialVersionUID = 1L;

    private final RowType keyType;
    private final boolean adaptive;
    private transient boolean compress;
    private transient int observedRows;
    private transient int observedNulls;
    private transient int windowSize;
    private transient NullBitmapBuffer nullRows;
    private transient InternalRow representativeKey;
    private transient int rangeIndex;
    private transient int taskId;

    NullBitmapShuffleOperator(RowType keyType) {
        this(keyType, true);
    }

    NullBitmapShuffleOperator(RowType keyType, boolean adaptive) {
        this.keyType = keyType;
        this.adaptive = adaptive;
    }

    @Override
    public TypeInformation<RowData> transportValueType() {
        return new NullBitmapTransportTypeInfo(keyType);
    }

    @Override
    public void open() throws Exception {
        super.open();
        nullRows = new NullBitmapBuffer();
        compress = !adaptive;
        windowSize = 256;
    }

    @Override
    public void processElement(StreamRecord<Tuple2<Integer, Tuple2<InternalRow, RowData>>> element)
            throws IOException {
        Tuple2<Integer, Tuple2<InternalRow, RowData>> assigned = element.getValue();
        RowData row = assigned.f1.f1;
        boolean isNull = row.isNullAt(1);
        // Decide for the following window; the initial sample passes through immediately.
        boolean compressThisRow = compress;
        if (adaptive) {
            observedRows++;
            observedNulls += isNull ? 1 : 0;
            if (observedRows == windowSize) {
                compress = observedNulls * 5 >= observedRows * 4;
                if (!compress) {
                    flush();
                    compressThisRow = false;
                }
                observedRows = 0;
                observedNulls = 0;
                windowSize = 4096;
            }
        }
        int nextTaskId = row.getInt(0);
        if (!compressThisRow || !isNull) {
            output.collect(element);
            return;
        }
        if (!nullRows.isEmpty() && (rangeIndex != assigned.f0 || taskId != nextTaskId)) {
            flush();
        }
        long rowId = row.getLong(2);
        if (nullRows.isEmpty() || rowId < nullRows.firstRowId()) {
            representativeKey = (InternalRow) InternalRowUtils.copy(assigned.f1.f0, keyType);
        }
        rangeIndex = assigned.f0;
        taskId = nextTaskId;
        nullRows.add(rowId);
        if (nullRows.isFull()) {
            flush();
        }
    }

    @Override
    public void endInput() throws IOException {
        flush();
    }

    private void flush() throws IOException {
        if (!nullRows.isEmpty()) {
            long firstRowId = nullRows.firstRowId();
            output.collect(
                    new StreamRecord<>(
                            Tuple2.of(
                                    rangeIndex,
                                    Tuple2.of(
                                            representativeKey,
                                            GenericRowData.of(
                                                    taskId, null, firstRowId, nullRows.drain())))));
            representativeKey = null;
        }
    }
}
