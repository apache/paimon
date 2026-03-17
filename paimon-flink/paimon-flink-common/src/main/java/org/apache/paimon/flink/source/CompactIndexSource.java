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
import org.apache.paimon.flink.sink.BTreeGlobalIndexCompactTaskTypeInfo;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexCompactCoordinator;
import org.apache.paimon.globalindex.btree.BTreeGlobalIndexCompactTask;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.FileStoreTable;

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

import java.util.List;

/**
 * Source for distributed BTree global index compaction.
 *
 * <p>Runs with parallelism 1 as a coordinator. Scans index manifest entries from the latest
 * snapshot, plans compaction tasks using {@link BTreeGlobalIndexCompactCoordinator}, and emits
 * {@link BTreeGlobalIndexCompactTask}s to downstream workers.
 */
public class CompactIndexSource extends AbstractNonCoordinatedSource<BTreeGlobalIndexCompactTask> {

    private static final Logger LOG = LoggerFactory.getLogger(CompactIndexSource.class);
    private static final String SOURCE_NAME = "BTree Index Compaction Coordinator";

    private final FileStoreTable table;

    public CompactIndexSource(FileStoreTable table) {
        this.table = table;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SourceReader<BTreeGlobalIndexCompactTask, SimpleSourceSplit> createReader(
            SourceReaderContext readerContext) {
        Preconditions.checkArgument(
                readerContext.currentParallelism() == 1,
                "CompactIndex Source parallelism must be 1.");
        return new CompactIndexSourceReader(table);
    }

    /** Source reader that plans BTree global index compaction tasks. */
    private static class CompactIndexSourceReader
            extends AbstractNonCoordinatedSourceReader<BTreeGlobalIndexCompactTask> {

        private final FileStoreTable table;
        private boolean emitted = false;

        CompactIndexSourceReader(FileStoreTable table) {
            this.table = table;
        }

        @Override
        public InputStatus pollNext(ReaderOutput<BTreeGlobalIndexCompactTask> readerOutput) {
            if (emitted) {
                return InputStatus.END_OF_INPUT;
            }
            emitted = true;

            IndexFileHandler handler = table.store().newIndexFileHandler();
            Snapshot snapshot = table.snapshotManager().latestSnapshot();
            if (snapshot == null) {
                LOG.info("No snapshot found, nothing to compact.");
                return InputStatus.END_OF_INPUT;
            }

            List<IndexManifestEntry> entries = handler.scan(snapshot, "btree");
            if (entries.isEmpty()) {
                LOG.info("No btree index entries found, nothing to compact.");
                return InputStatus.END_OF_INPUT;
            }

            Options options = table.coreOptions().toConfiguration();
            BTreeGlobalIndexCompactCoordinator coordinator =
                    new BTreeGlobalIndexCompactCoordinator(options);

            if (!coordinator.meetsMinFilesThreshold(entries)) {
                LOG.info("No group meets min-files threshold, skipping compaction.");
                return InputStatus.END_OF_INPUT;
            }

            List<BTreeGlobalIndexCompactTask> tasks = coordinator.plan(entries);
            LOG.info("Planned {} compaction tasks for btree global index.", tasks.size());
            for (BTreeGlobalIndexCompactTask task : tasks) {
                readerOutput.collect(task);
            }
            return InputStatus.END_OF_INPUT;
        }
    }

    public static DataStreamSource<BTreeGlobalIndexCompactTask> buildSource(
            StreamExecutionEnvironment env, CompactIndexSource source, String tableIdentifier) {
        return (DataStreamSource<BTreeGlobalIndexCompactTask>)
                env.fromSource(
                                source,
                                WatermarkStrategy.noWatermarks(),
                                SOURCE_NAME + " : " + tableIdentifier,
                                new BTreeGlobalIndexCompactTaskTypeInfo())
                        .setParallelism(1)
                        .setMaxParallelism(1);
    }
}
