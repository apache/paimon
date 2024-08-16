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

import org.apache.paimon.append.UnawareAppendCompactionTask;
import org.apache.paimon.append.UnawareAppendTableCompactionCoordinator;
import org.apache.paimon.table.FileStoreTable;

import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Either;

import java.util.List;

import static org.apache.paimon.utils.Preconditions.checkArgument;

/**
 * A {@link OneInputStreamOperator} to accept commit messages and send append compact coordinate
 * compact task to downstream operators.
 */
public class AppendBypassCoordinateOperator<CommitT>
        extends AbstractStreamOperator<Either<CommitT, UnawareAppendCompactionTask>>
        implements OneInputStreamOperator<CommitT, Either<CommitT, UnawareAppendCompactionTask>>,
                ProcessingTimeService.ProcessingTimeCallback {

    private final FileStoreTable table;

    private transient UnawareAppendTableCompactionCoordinator coordinator;

    public AppendBypassCoordinateOperator(FileStoreTable table) {
        this.table = table;
        this.chainingStrategy = ChainingStrategy.NEVER;
    }

    @Override
    public void open() throws Exception {
        super.open();
        checkArgument(
                getRuntimeContext().getNumberOfParallelSubtasks() == 1,
                "Compaction Coordinator parallelism in paimon MUST be one.");
        this.coordinator = new UnawareAppendTableCompactionCoordinator(table, true, null);
        long intervalMs = table.coreOptions().continuousDiscoveryInterval().toMillis();
        getProcessingTimeService().scheduleWithFixedDelay(this, 0, intervalMs);
    }

    @Override
    public void onProcessingTime(long time) {
        while (true) {
            List<UnawareAppendCompactionTask> tasks = coordinator.run();
            for (UnawareAppendCompactionTask task : tasks) {
                output.collect(new StreamRecord<>(Either.Right(task)));
            }

            if (tasks.isEmpty()) {
                break;
            }
        }
    }

    @Override
    public void processElement(StreamRecord<CommitT> record) throws Exception {
        output.collect(new StreamRecord<>(Either.Left(record.getValue())));
    }
}
