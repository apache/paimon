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

package org.apache.paimon.flink.source.shardread;

import org.apache.paimon.disk.IOManager;
import org.apache.paimon.flink.NestedProjectedRowData;
import org.apache.paimon.flink.metrics.FlinkMetricRegistry;
import org.apache.paimon.flink.source.DynamicPartitionFilteringInfo;
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.flink.source.FileStoreSourceSplitGenerator;
import org.apache.paimon.flink.source.FlinkSource;
import org.apache.paimon.flink.source.StaticFileStoreSource;
import org.apache.paimon.flink.source.metrics.FileStoreSourceReaderMetrics;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableRead;
import org.apache.paimon.table.source.TableScan;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.metrics.groups.SourceReaderMetricGroup;
import org.apache.flink.table.data.RowData;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.data.BinaryRow.EMPTY_ROW;
import static org.apache.paimon.disk.IOManagerImpl.splitPaths;
import static org.apache.paimon.flink.FlinkConnectorOptions.SplitAssignMode;

/** Bounded {@link FlinkSource} for reading records of shard read. */
public class ShardStaticFileStoreSource extends StaticFileStoreSource {

    private static final long serialVersionUID = 3L;

    private final long latestSnapshotId;

    public ShardStaticFileStoreSource(
            ReadBuilder readBuilder,
            @Nullable Long limit,
            int splitBatchSize,
            SplitAssignMode splitAssignMode,
            @Nullable DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo,
            @Nullable NestedProjectedRowData rowData,
            long latestSnapshotId) {
        super(
                readBuilder,
                limit,
                splitBatchSize,
                splitAssignMode,
                dynamicPartitionFilteringInfo,
                rowData);

        this.latestSnapshotId = latestSnapshotId;
    }

    @Override
    public SourceReader<RowData, FileStoreSourceSplit> createReader(SourceReaderContext context) {
        IOManager ioManager =
                IOManager.create(splitPaths(context.getConfiguration().get(CoreOptions.TMP_DIRS)));
        SourceReaderMetricGroup metricGroup = context.metricGroup();
        FileStoreSourceReaderMetrics sourceReaderMetrics =
                new FileStoreSourceReaderMetrics(metricGroup);
        TableRead tableRead =
                readBuilder.newRead().withMetricRegistry(new FlinkMetricRegistry(metricGroup));
        TableScan tableScan =
                readBuilder
                        .withSnapshot(latestSnapshotId)
                        .withShard(context.getIndexOfSubtask(), context.currentParallelism())
                        .newScan();

        return new ShardSourceReader(
                context,
                tableRead,
                tableScan,
                sourceReaderMetrics,
                ioManager,
                limit,
                NestedProjectedRowData.copy(rowData));
    }

    @Override
    public List<FileStoreSourceSplit> getSplits(SplitEnumeratorContext context) {
        FileStoreSourceSplitGenerator splitGenerator = new FileStoreSourceSplitGenerator();

        List<Split> splits = new ArrayList<>(context.currentParallelism());
        for (int i = 0; i < context.currentParallelism(); ++i) {
            splits.add(
                    DataSplit.builder()
                            .withPartition(EMPTY_ROW)
                            .withBucket(0)
                            .withDataFiles(Collections.emptyList())
                            .withBucketPath("")
                            .build());
        }
        return splitGenerator.createSplits(splits);
    }
}
