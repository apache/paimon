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

import org.apache.paimon.Snapshot;
import org.apache.paimon.append.dataevolution.DataEvolutionCompactTask;
import org.apache.paimon.append.dataevolution.DataEvolutionDeletionVectorMaterializeCoordinator;
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

/** Source which plans tasks to physically apply data-evolution deletion vectors. */
public class DataEvolutionDeletionVectorMaterializeSource
        extends AbstractNonCoordinatedSource<DataEvolutionCompactTask> {

    private static final Logger LOG =
            LoggerFactory.getLogger(DataEvolutionDeletionVectorMaterializeSource.class);
    private static final String COORDINATOR_NAME =
            "Data Evolution Deletion Vector Materialize Coordinator";

    private final FileStoreTable table;
    @Nullable private final PartitionPredicate partitionFilter;
    private final Snapshot snapshot;

    public DataEvolutionDeletionVectorMaterializeSource(
            FileStoreTable table, @Nullable PartitionPredicate partitionFilter, Snapshot snapshot) {
        this.table = table;
        this.partitionFilter = partitionFilter;
        this.snapshot = snapshot;
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
                "Deletion vector materialize operator parallelism in paimon MUST be one.");
        return new MaterializeSourceReader(table, partitionFilter, snapshot);
    }

    /** Reader which plans materialization tasks in bounded batches. */
    public static class MaterializeSourceReader
            extends AbstractNonCoordinatedSourceReader<DataEvolutionCompactTask> {

        private final DataEvolutionDeletionVectorMaterializeCoordinator coordinator;

        public MaterializeSourceReader(
                FileStoreTable table, @Nullable PartitionPredicate partitions, Snapshot snapshot) {
            this.coordinator =
                    new DataEvolutionDeletionVectorMaterializeCoordinator(
                            table, partitions, snapshot);
        }

        @Override
        public InputStatus pollNext(ReaderOutput<DataEvolutionCompactTask> readerOutput)
                throws Exception {
            try {
                List<DataEvolutionCompactTask> tasks = coordinator.plan();
                tasks.forEach(readerOutput::collect);
            } catch (EndOfScanException ignored) {
                LOG.info("All deletion vectors have been planned for materialization.");
                return InputStatus.END_OF_INPUT;
            }
            return InputStatus.MORE_AVAILABLE;
        }
    }

    public static DataStreamSource<DataEvolutionCompactTask> buildSource(
            StreamExecutionEnvironment env,
            DataEvolutionDeletionVectorMaterializeSource source,
            String tableIdentifier) {
        return (DataStreamSource<DataEvolutionCompactTask>)
                env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                COORDINATOR_NAME + " : " + tableIdentifier,
                                new DataEvolutionCompactionTaskTypeInfo())
                        .setParallelism(1)
                        .setMaxParallelism(1);
    }
}
