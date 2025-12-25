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

import org.apache.paimon.append.dataevolution.DataEvolutionCompactCoordinator;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.flink.sink.DataEvolutionCompactionTaskTypeInfo;
import org.apache.paimon.partition.PartitionPredicate;
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

/** Source for data-evolution table Compaction. */
public class DataEvolutionTableCompactSource
        extends AbstractNonCoordinatedSource<DataEvolutionCompactTask> {

    private static final Logger LOG =
            LoggerFactory.getLogger(DataEvolutionTableCompactSource.class);
    private static final String COMPACTION_COORDINATOR_NAME =
            "DataEvolution Compaction Coordinator";

    private final FileStoreTable table;
    private final PartitionPredicate partitionFilter;

    public DataEvolutionTableCompactSource(
            FileStoreTable table, @Nullable PartitionPredicate partitionFilter) {
        this.table = table;
        this.partitionFilter = partitionFilter;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<DataEvolutionCompactTask, SimpleSourceSplit> createReader(
            SourceReaderContext readerContext) throws Exception {
        Preconditions.checkArgument(
                readerContext.currentParallelism() == 1,
                "Compaction Operator parallelism in paimon MUST be one.");
        return new CompactSourceReader(table, partitionFilter);
    }

    /** BucketUnawareCompactSourceReader. */
    public static class CompactSourceReader
            extends AbstractNonCoordinatedSourceReader<DataEvolutionCompactTask> {
        private final DataEvolutionCompactCoordinator compactionCoordinator;

        public CompactSourceReader(FileStoreTable table, PartitionPredicate partitions) {
            compactionCoordinator = new DataEvolutionCompactCoordinator(table, partitions, false);
        }

        @Override
        public InputStatus pollNext(ReaderOutput<DataEvolutionCompactTask> readerOutput)
                throws Exception {
            try {
                // do scan and plan action, emit data-evolution compaction tasks.
                List<DataEvolutionCompactTask> tasks = compactionCoordinator.plan();
                tasks.forEach(readerOutput::collect);
            } catch (EndOfScanException esf) {
                LOG.info("Catching EndOfScanException, the job is finished.");
                return InputStatus.END_OF_INPUT;
            }

            return InputStatus.MORE_AVAILABLE;
        }
    }

    public static DataStreamSource<DataEvolutionCompactTask> buildSource(
            StreamExecutionEnvironment env,
            DataEvolutionTableCompactSource source,
            String tableIdentifier) {
        return (DataStreamSource<DataEvolutionCompactTask>)
                env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                COMPACTION_COORDINATOR_NAME + " : " + tableIdentifier,
                                new DataEvolutionCompactionTaskTypeInfo())
                        .setParallelism(1)
                        .setMaxParallelism(1);
    }
}
