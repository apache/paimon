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

package org.apache.paimon.flink.postpone;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.disk.IOManagerImpl;
import org.apache.paimon.flink.sink.Committable;
import org.apache.paimon.flink.utils.BoundedTwoInputOperator;
import org.apache.paimon.flink.utils.RuntimeContextUtils;
import org.apache.paimon.operation.FileSystemWriteRestore;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.PostponeUtils.CompactBucket;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.FixedBucketRowKeyExtractor;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.apache.paimon.utils.Pair;

import org.apache.flink.streaming.api.operators.InputSelection;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.LinkedHashSet;
import java.util.Set;

/** Writes postponed rows and compacts existing Level-0 buckets with the same table writer. */
public class PostponeBucketCompactOperator
        extends BoundedTwoInputOperator<InternalRow, CompactBucket, Committable> {

    private static final long serialVersionUID = 1L;

    private final FileStoreTable table;
    private final String commitUser;
    private final long snapshotId;

    private transient IOManagerImpl ioManager;
    private transient TableWriteImpl<?> write;
    private transient FixedBucketRowKeyExtractor rowKeyExtractor;
    private transient Set<Pair<BinaryRow, Integer>> buckets;
    private transient boolean[] endedInputs;

    public PostponeBucketCompactOperator(FileStoreTable table, String commitUser, long snapshotId) {
        this.table = table;
        this.commitUser = commitUser;
        this.snapshotId = snapshotId;
    }

    @Override
    public void open() throws Exception {
        super.open();
        ioManager =
                new IOManagerImpl(
                        getContainingTask()
                                .getEnvironment()
                                .getIOManager()
                                .getSpillingDirectoriesPaths());
        write =
                table.newWrite(
                                commitUser,
                                RuntimeContextUtils.getIndexOfThisSubtask(getRuntimeContext()))
                        .withIOManager(ioManager);
        write.withWriteRestore(
                new FileSystemWriteRestore(
                        table.coreOptions(),
                        table.snapshotManager(),
                        table.store().newScan(),
                        table.store().newIndexFileHandler(),
                        snapshotId));
        rowKeyExtractor = new FixedBucketRowKeyExtractor(table.schema());
        buckets = new LinkedHashSet<>();
        endedInputs = new boolean[2];
    }

    @Override
    public InputSelection nextSelection() {
        return InputSelection.ALL;
    }

    @Override
    public void processElement1(StreamRecord<InternalRow> element) throws Exception {
        InternalRow row = element.getValue();
        rowKeyExtractor.setRecord(row);
        buckets.add(Pair.of(rowKeyExtractor.partition().copy(), rowKeyExtractor.bucket()));
        write.write(row);
    }

    @Override
    public void processElement2(StreamRecord<CompactBucket> element) {
        CompactBucket bucket = element.getValue();
        buckets.add(Pair.of(bucket.partition().copy(), bucket.bucket()));
    }

    @Override
    public void endInput(int inputId) throws Exception {
        endedInputs[inputId - 1] = true;
        if (endedInputs[0] && endedInputs[1]) {
            for (Pair<BinaryRow, Integer> bucket : buckets) {
                write.compact(bucket.getLeft(), bucket.getRight(), false);
            }
            for (CommitMessage message : write.prepareCommit(true, Long.MAX_VALUE)) {
                output.collect(new StreamRecord<>(new Committable(Long.MAX_VALUE, message)));
            }
        }
    }

    @Override
    public void close() throws Exception {
        try {
            if (write != null) {
                write.close();
            }
        } finally {
            if (ioManager != null) {
                ioManager.close();
            }
            super.close();
        }
    }
}
