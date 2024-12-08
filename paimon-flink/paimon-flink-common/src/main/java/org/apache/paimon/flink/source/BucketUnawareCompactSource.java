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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Source for unaware-bucket Compaction.
 *
 * <p>Note: The function is the source of unaware-bucket compactor coordinator. It will read the
 * latest snapshot continuously by compactionCoordinator, and generate new compaction tasks. The
 * source is used in unaware-bucket compaction job (both stand-alone and write-combined). Besides,
 * we don't need to save state in this source, it will invoke a full scan when starting up, and scan
 * continuously for the following snapshot.
 */
public class BucketUnawareCompactSource
        extends AbstractNonCoordinatedSource<UnawareAppendCompactionTask> {

    private static final Logger LOG = LoggerFactory.getLogger(BucketUnawareCompactSource.class);
    private static final String COMPACTION_COORDINATOR_NAME = "Compaction Coordinator";

    private final FileStoreTable table;
    private final boolean streaming;
    private final long scanInterval;
    private final Predicate filter;

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
    public Boundedness getBoundedness() {
        return streaming ? Boundedness.CONTINUOUS_UNBOUNDED : Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<UnawareAppendCompactionTask, SimpleSourceSplit> createReader(
            SourceReaderContext readerContext) throws Exception {
        Preconditions.checkArgument(
                readerContext.currentParallelism() == 1,
                "Compaction Operator parallelism in paimon MUST be one.");
        return new BucketUnawareCompactSourceReader(table, streaming, filter, scanInterval);
    }

    /** BucketUnawareCompactSourceReader. */
    public static class BucketUnawareCompactSourceReader
            extends AbstractNonCoordinatedSourceReader<UnawareAppendCompactionTask> {
        private final UnawareAppendTableCompactionCoordinator compactionCoordinator;
        private final long scanInterval;
        private long lastFetchTimeMillis = 0L;

        public BucketUnawareCompactSourceReader(
                FileStoreTable table, boolean streaming, Predicate filter, long scanInterval) {
            this.scanInterval = scanInterval;
            compactionCoordinator =
                    new UnawareAppendTableCompactionCoordinator(table, streaming, filter);
        }

        @Override
        public InputStatus pollNext(ReaderOutput<UnawareAppendCompactionTask> readerOutput)
                throws Exception {
            long sleepTimeMillis = scanInterval - System.currentTimeMillis() + lastFetchTimeMillis;
            if (sleepTimeMillis > 0) {
                Thread.sleep(sleepTimeMillis);
            }

            try {
                // do scan and plan action, emit append-only compaction tasks.
                List<UnawareAppendCompactionTask> tasks = compactionCoordinator.run();
                tasks.forEach(readerOutput::collect);
                return InputStatus.MORE_AVAILABLE;
            } catch (EndOfScanException esf) {
                LOG.info("Catching EndOfStreamException, the stream is finished.");
                return InputStatus.END_OF_INPUT;
            } finally {
                lastFetchTimeMillis = System.currentTimeMillis();
            }
        }
    }

    public static DataStreamSource<UnawareAppendCompactionTask> buildSource(
            StreamExecutionEnvironment env,
            BucketUnawareCompactSource source,
            String tableIdentifier) {
        return (DataStreamSource<UnawareAppendCompactionTask>)
                env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                COMPACTION_COORDINATOR_NAME + " : " + tableIdentifier,
                                new CompactionTaskTypeInfo())
                        .setParallelism(1)
                        .setMaxParallelism(1);
    }
}
