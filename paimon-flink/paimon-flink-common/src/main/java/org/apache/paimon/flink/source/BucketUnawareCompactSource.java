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
import org.apache.paimon.flink.sink.CompactionTaskTypeInfo;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.EndOfScanException;
import org.apache.paimon.utils.Preconditions;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Source Function for unaware-bucket Compaction.
 *
 * <p>Note: The function is the source function of unaware-bucket compactor coordinator. It will
 * read the latest snapshot continuously by compactionCoordinator, and generate new compaction
 * tasks. The source function is used in unaware-bucket compaction job (both stand-alone and
 * write-combined). Besides, we don't need to save state in this function, it will invoke a full
 * scan when starting up, and scan continuously for the following snapshot.
 */
public class BucketUnawareCompactSource extends RichSourceFunction<UnawareAppendCompactionTask> {

    private static final Logger LOG = LoggerFactory.getLogger(BucketUnawareCompactSource.class);
    private static final String COMPACTION_COORDINATOR_NAME = "Compaction Coordinator";

    private final FileStoreTable table;
    private final boolean streaming;
    private final long scanInterval;
    private final Predicate filter;
    private transient UnawareAppendTableCompactionCoordinator compactionCoordinator;
    private transient SourceContext<UnawareAppendCompactionTask> ctx;
    private volatile boolean isRunning = true;

    public BucketUnawareCompactSource(
            FileStoreTable table,
            boolean isStreaming,
            long scanInterval,
            @Nullable Predicate filter) {
        this.table = table;
        this.streaming = isStreaming;
        this.scanInterval = scanInterval;
        this.filter = filter;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        compactionCoordinator =
                new UnawareAppendTableCompactionCoordinator(table, streaming, filter);
        Preconditions.checkArgument(
                this.getRuntimeContext().getNumberOfParallelSubtasks() == 1,
                "Compaction Operator parallelism in paimon MUST be one.");
    }

    @Override
    public void run(SourceContext<UnawareAppendCompactionTask> sourceContext) throws Exception {
        this.ctx = sourceContext;
        while (isRunning) {
            boolean isEmpty;
            synchronized (ctx.getCheckpointLock()) {
                if (!isRunning) {
                    return;
                }
                try {
                    // do scan and plan action, emit append-only compaction tasks.
                    List<UnawareAppendCompactionTask> tasks = compactionCoordinator.run();
                    isEmpty = tasks.isEmpty();
                    tasks.forEach(ctx::collect);
                } catch (EndOfScanException esf) {
                    LOG.info("Catching EndOfStreamException, the stream is finished.");
                    return;
                }
            }

            if (isEmpty) {
                Thread.sleep(scanInterval);
            }
        }
    }

    @Override
    public void cancel() {
        if (ctx != null) {
            synchronized (ctx.getCheckpointLock()) {
                isRunning = false;
            }
        } else {
            isRunning = false;
        }
    }

    public static DataStreamSource<UnawareAppendCompactionTask> buildSource(
            StreamExecutionEnvironment env,
            BucketUnawareCompactSource source,
            boolean streaming,
            String tableIdentifier) {
        final StreamSource<UnawareAppendCompactionTask, BucketUnawareCompactSource> sourceOperator =
                new StreamSource<>(source);
        return (DataStreamSource<UnawareAppendCompactionTask>)
                new DataStreamSource<>(
                                env,
                                new CompactionTaskTypeInfo(),
                                sourceOperator,
                                false,
                                COMPACTION_COORDINATOR_NAME + " : " + tableIdentifier,
                                streaming ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED)
                        .setParallelism(1)
                        .setMaxParallelism(1);
    }
}
