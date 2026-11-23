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

import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.flink.shuffle.RangeShuffle;
import org.apache.paimon.utils.RoaringNavigableMap64;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.TableStreamOperator;

import java.io.IOException;

/** Restores original scalar rows and sort keys after exchange, before local sorting. */
class NullBitmapExpandOperator extends TableStreamOperator<Tuple2<InternalRow, RowData>>
        implements RangeShuffle.ExchangedRowOperator<InternalRow> {

    private static final long serialVersionUID = 1L;

    @Override
    public void processElement(StreamRecord<Tuple2<InternalRow, RowData>> element)
            throws IOException {
        RowData row = element.getValue().f1;
        if (row.getArity() == 3) {
            output.collect(element);
            return;
        }
        RoaringNavigableMap64 nullRows = new RoaringNavigableMap64();
        nullRows.deserialize(row.getBinary(3));
        int taskId = row.getInt(0);
        for (long rowId : nullRows) {
            output.collect(
                    new StreamRecord<>(
                            Tuple2.of(
                                    GenericRow.of(taskId, null, rowId),
                                    GenericRowData.of(taskId, null, rowId))));
        }
    }
}
