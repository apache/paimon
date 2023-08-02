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

package org.apache.paimon.flink.sink.index;

import org.apache.paimon.flink.sink.RowDataPartitionKeyExtractor;
import org.apache.paimon.flink.utils.ProjectToRowDataFunction;
import org.apache.paimon.table.Table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.paimon.flink.FlinkRowData.toFlinkRowKind;

/** A {@link OneInputStreamOperator} for {@link GlobalIndexAssigner}. */
public class GlobalIndexAssignerOperator<T> extends AbstractStreamOperator<Tuple2<T, Integer>>
        implements OneInputStreamOperator<Tuple2<KeyPartOrRow, T>, Tuple2<T, Integer>> {

    private static final long serialVersionUID = 1L;

    private final GlobalIndexAssigner<T> assigner;

    public GlobalIndexAssignerOperator(GlobalIndexAssigner<T> assigner) {
        this.assigner = assigner;
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);
        File[] tmpDirs =
                getContainingTask().getEnvironment().getIOManager().getSpillingDirectories();
        File tmpDir = tmpDirs[ThreadLocalRandom.current().nextInt(tmpDirs.length)];
        assigner.open(
                tmpDir,
                getRuntimeContext().getNumberOfParallelSubtasks(),
                getRuntimeContext().getIndexOfThisSubtask(),
                this::collect);
    }

    @Override
    public void processElement(StreamRecord<Tuple2<KeyPartOrRow, T>> streamRecord)
            throws Exception {
        Tuple2<KeyPartOrRow, T> tuple2 = streamRecord.getValue();
        T value = tuple2.f1;
        switch (tuple2.f0) {
            case KEY_PART:
                assigner.bootstrap(value);
                break;
            case ROW:
                assigner.process(value);
                break;
        }
    }

    private void collect(T value, int bucket) {
        output.collect(new StreamRecord<>(new Tuple2<>(value, bucket)));
    }

    @Override
    public void close() throws IOException {
        this.assigner.close();
    }

    public static GlobalIndexAssignerOperator<RowData> forRowData(Table table) {
        return new GlobalIndexAssignerOperator<>(createRowDataAssigner(table));
    }

    public static GlobalIndexAssigner<RowData> createRowDataAssigner(Table t) {
        return new GlobalIndexAssigner<>(
                t,
                RowDataPartitionKeyExtractor::new,
                new ProjectToRowDataFunction(t.rowType(), t.partitionKeys()),
                (rowData, rowKind) -> {
                    rowData.setRowKind(toFlinkRowKind(rowKind));
                    return rowData;
                });
    }
}
