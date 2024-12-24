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

import org.apache.paimon.CoreOptions;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.NestedProjectedRowData;
import org.apache.paimon.flink.metrics.FlinkMetricRegistry;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.StreamTableScan;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.metrics.groups.SplitEnumeratorMetricGroup;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

/** Unbounded {@link FlinkSource} for reading records. It continuously monitors new snapshots. */
public class ContinuousFileStoreSource extends FlinkSource {

    private static final long serialVersionUID = 4L;

    protected final Map<String, String> options;
    protected final BucketMode bucketMode;

    public ContinuousFileStoreSource(
            ReadBuilder readBuilder, Map<String, String> options, @Nullable Long limit) {
        this(readBuilder, options, limit, BucketMode.HASH_FIXED, null);
    }

    public ContinuousFileStoreSource(
            ReadBuilder readBuilder,
            Map<String, String> options,
            @Nullable Long limit,
            BucketMode bucketMode,
            @Nullable NestedProjectedRowData rowData) {
        super(readBuilder, limit, rowData);
        this.options = options;
        this.bucketMode = bucketMode;
    }

    @Override
    public Boundedness getBoundedness() {
        Long boundedWatermark = CoreOptions.fromMap(options).scanBoundedWatermark();
        return boundedWatermark != null ? Boundedness.BOUNDED : Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> restoreEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            PendingSplitsCheckpoint checkpoint) {
        Long nextSnapshotId = null;
        Collection<FileStoreSourceSplit> splits = new ArrayList<>();
        if (checkpoint != null) {
            nextSnapshotId = checkpoint.currentSnapshotId();
            splits = checkpoint.splits();
        }
        StreamTableScan scan = readBuilder.newStreamScan();
        if (metricGroup(context) != null) {
            ((StreamDataTableScan) scan)
                    .withMetricsRegistry(new FlinkMetricRegistry(context.metricGroup()));
        }
        scan.restore(nextSnapshotId);
        return buildEnumerator(context, splits, nextSnapshotId, scan);
    }

    @Nullable
    private SplitEnumeratorMetricGroup metricGroup(SplitEnumeratorContext<?> context) {
        try {
            return context.metricGroup();
        } catch (NullPointerException ignore) {
            // ignore NPE for some Flink versions
            return null;
        }
    }

    protected SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> buildEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            Collection<FileStoreSourceSplit> splits,
            @Nullable Long nextSnapshotId,
            StreamTableScan scan) {
        Options options = Options.fromMap(this.options);
        return new ContinuousFileSplitEnumerator(
                context,
                splits,
                nextSnapshotId,
                options.get(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL).toMillis(),
                scan,
                bucketMode,
                options.get(CoreOptions.SCAN_MAX_SPLITS_PER_TASK),
                options.get(FlinkConnectorOptions.STREAMING_READ_SHUFFLE_BUCKET_WITH_PARTITION));
    }
}
