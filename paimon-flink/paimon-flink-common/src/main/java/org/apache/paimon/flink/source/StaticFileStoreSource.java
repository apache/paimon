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

import org.apache.paimon.flink.metrics.FlinkMetricRegistry;
import org.apache.paimon.flink.source.assigners.FIFOSplitAssigner;
import org.apache.paimon.flink.source.assigners.PreAssignSplitAssigner;
import org.apache.paimon.flink.source.assigners.SplitAssigner;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableScan;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;

import static org.apache.paimon.flink.FlinkConnectorOptions.SplitAssignMode;

/** Bounded {@link FlinkSource} for reading records. It does not monitor new snapshots. */
public class StaticFileStoreSource extends FlinkSource {

    private static final long serialVersionUID = 3L;

    private final int splitBatchSize;

    private final SplitAssignMode splitAssignMode;

    @Nullable private final DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo;

    public StaticFileStoreSource(
            ReadBuilder readBuilder,
            @Nullable Long limit,
            int splitBatchSize,
            SplitAssignMode splitAssignMode) {
        this(readBuilder, limit, splitBatchSize, splitAssignMode, null);
    }

    public StaticFileStoreSource(
            ReadBuilder readBuilder,
            @Nullable Long limit,
            int splitBatchSize,
            SplitAssignMode splitAssignMode,
            @Nullable DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo) {
        super(readBuilder, limit);
        this.splitBatchSize = splitBatchSize;
        this.splitAssignMode = splitAssignMode;
        this.dynamicPartitionFilteringInfo = dynamicPartitionFilteringInfo;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            PendingSplitsCheckpoint checkpoint) {
        Collection<FileStoreSourceSplit> splits =
                checkpoint == null ? getSplits(context) : checkpoint.splits();
        SplitAssigner splitAssigner =
                createSplitAssigner(context, splitBatchSize, splitAssignMode, splits);
        return new StaticFileStoreSplitEnumerator(
                context, null, splitAssigner, dynamicPartitionFilteringInfo);
    }

    private List<FileStoreSourceSplit> getSplits(SplitEnumeratorContext context) {
        FileStoreSourceSplitGenerator splitGenerator = new FileStoreSourceSplitGenerator();
        TableScan scan = readBuilder.dropStats().newScan();
        // register scan metrics
        if (context.metricGroup() != null) {
            ((InnerTableScan) scan)
                    .withMetricsRegistry(new FlinkMetricRegistry(context.metricGroup()));
        }
        return splitGenerator.createSplits(scan.plan());
    }

    public static SplitAssigner createSplitAssigner(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            int splitBatchSize,
            SplitAssignMode splitAssignMode,
            Collection<FileStoreSourceSplit> splits) {
        switch (splitAssignMode) {
            case FAIR:
                return new PreAssignSplitAssigner(splitBatchSize, context, splits);
            case PREEMPTIVE:
                return new FIFOSplitAssigner(splits);
            default:
                throw new UnsupportedOperationException(
                        "Unsupported assign mode " + splitAssignMode);
        }
    }
}
