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
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.log.LogSourceProvider;
import org.apache.paimon.flink.metrics.FlinkMetricRegistry;
import org.apache.paimon.options.Options;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.DataTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.base.source.hybrid.HybridSource;
import org.apache.flink.connector.base.source.hybrid.HybridSource.SourceFactory;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Map;

/** Log {@link SourceFactory} from {@link StaticFileStoreSplitEnumerator}. */
public class LogHybridSourceFactory
        implements SourceFactory<RowData, Source<RowData, ?, ?>, StaticFileStoreSplitEnumerator> {

    private final LogSourceProvider provider;

    public LogHybridSourceFactory(LogSourceProvider provider) {
        this.provider = provider;
    }

    @Override
    public Source<RowData, ?, ?> create(
            HybridSource.SourceSwitchContext<StaticFileStoreSplitEnumerator> context) {
        StaticFileStoreSplitEnumerator enumerator = context.getPreviousEnumerator();
        Map<Integer, Long> logOffsets = null;
        if (enumerator != null) {
            Snapshot snapshot = enumerator.snapshot();
            if (snapshot != null) {
                logOffsets = snapshot.logOffsets();
            }
        }
        return provider.createSource(logOffsets);
    }

    public static FlinkSource buildHybridFirstSource(
            Table table, @Nullable int[][] projectedFields, @Nullable Predicate predicate) {
        if (!(table instanceof DataTable)) {
            throw new UnsupportedOperationException(
                    String.format(
                            "FlinkHybridFirstSource only accepts DataTable. Unsupported table type: '%s'.",
                            table.getClass().getSimpleName()));
        }

        DataTable dataTable = (DataTable) table;

        return new FlinkHybridFirstSource(
                table.newReadBuilder().withProjection(projectedFields).withFilter(predicate),
                dataTable.snapshotManager(),
                dataTable.coreOptions().toConfiguration());
    }

    /** The first source of a log {@link HybridSource}. */
    private static class FlinkHybridFirstSource extends FlinkSource {

        private static final long serialVersionUID = 3L;

        private final SnapshotManager snapshotManager;
        private final Options options;

        public FlinkHybridFirstSource(
                ReadBuilder readBuilder, SnapshotManager snapshotManager, Options options) {
            super(readBuilder, null);
            this.snapshotManager = snapshotManager;
            this.options = options;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
                SplitEnumeratorContext<FileStoreSourceSplit> context,
                PendingSplitsCheckpoint checkpoint) {
            Long snapshotId = null;
            Collection<FileStoreSourceSplit> splits;
            if (checkpoint == null) {
                FileStoreSourceSplitGenerator splitGenerator = new FileStoreSourceSplitGenerator();
                // get snapshot id and splits from scan
                StreamTableScan scan = readBuilder.newStreamScan();
                // register scan metrics
                if (context.metricGroup() != null) {
                    ((StreamDataTableScan) scan)
                            .withMetricsRegistry(new FlinkMetricRegistry(context.metricGroup()));
                }
                splits = splitGenerator.createSplits(scan.plan());
                Long nextSnapshotId = scan.checkpoint();
                if (nextSnapshotId != null) {
                    snapshotId = nextSnapshotId - 1;
                }
            } else {
                // restore from checkpoint
                snapshotId = checkpoint.currentSnapshotId();
                splits = checkpoint.splits();
            }

            Snapshot snapshot = snapshotId == null ? null : snapshotManager.snapshot(snapshotId);
            return new StaticFileStoreSplitEnumerator(
                    context,
                    snapshot,
                    StaticFileStoreSource.createSplitAssigner(
                            context,
                            options.get(FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_BATCH_SIZE),
                            options.get(FlinkConnectorOptions.SCAN_SPLIT_ENUMERATOR_ASSIGN_MODE),
                            splits));
        }
    }
}
