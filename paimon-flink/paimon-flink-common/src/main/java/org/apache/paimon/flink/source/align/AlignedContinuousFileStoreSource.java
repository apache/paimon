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

package org.apache.paimon.flink.source.align;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.FlinkConnectorOptions;
import org.apache.paimon.flink.NestedProjectedRowData;
import org.apache.paimon.flink.source.ContinuousFileStoreSource;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.PendingSplitsCheckpoint;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.BucketMode;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.StreamTableScan;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.base.source.reader.SourceReaderOptions;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Map;

import static org.apache.paimon.disk.IOManagerImpl.splitPaths;

/** Used to create {@link AlignedSourceReader} and {@link AlignedContinuousFileSplitEnumerator}. */
public class AlignedContinuousFileStoreSource extends ContinuousFileStoreSource {

    public AlignedContinuousFileStoreSource(
            ReadBuilder readBuilder,
            Map<String, String> options,
            @Nullable Long limit,
            BucketMode bucketMode,
            @Nullable NestedProjectedRowData rowData) {
        super(readBuilder, options, limit, bucketMode, rowData);
    }

    @Override
    public SourceReader<RowData, FileStoreSourceSplit> createReader(SourceReaderContext context) {
        IOManager ioManager =
                IOManager.create(
                        splitPaths(
                                context.getConfiguration()
                                        .get(org.apache.flink.configuration.CoreOptions.TMP_DIRS)));
        FileStoreSourceReaderMetrics sourceReaderMetrics =
                new FileStoreSourceReaderMetrics(context.metricGroup());
        return new AlignedSourceReader(
                context,
                readBuilder.newRead(),
                sourceReaderMetrics,
                ioManager,
                limit,
                new FutureCompletingBlockingQueue<>(
                        context.getConfiguration().get(SourceReaderOptions.ELEMENT_QUEUE_CAPACITY)),
                rowData);
    }

    @Override
    protected SplitEnumerator<FileStoreSourceSplit, PendingSplitsCheckpoint> buildEnumerator(
            SplitEnumeratorContext<FileStoreSourceSplit> context,
            Collection<FileStoreSourceSplit> splits,
            @Nullable Long nextSnapshotId,
            StreamTableScan scan) {
        Options options = Options.fromMap(this.options);
        return new AlignedContinuousFileSplitEnumerator(
                context,
                splits,
                nextSnapshotId,
                options.get(CoreOptions.CONTINUOUS_DISCOVERY_INTERVAL).toMillis(),
                scan,
                bucketMode,
                options.get(FlinkConnectorOptions.SOURCE_CHECKPOINT_ALIGN_TIMEOUT).toMillis(),
                options.get(CoreOptions.SCAN_MAX_SPLITS_PER_TASK),
                options.get(FlinkConnectorOptions.STREAMING_READ_SHUFFLE_BUCKET_WITH_PARTITION));
    }
}
