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

import org.apache.paimon.flink.NestedProjectedRowData;
import org.apache.paimon.flink.metrics.FlinkMetricRegistry;
import org.apache.paimon.flink.source.assigners.FIFOSplitAssigner;
import org.apache.paimon.flink.source.assigners.PreAssignSplitAssigner;
import org.apache.paimon.flink.source.assigners.SplitAssigner;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.List;

import static org.apache.paimon.flink.FlinkConnectorOptions.SplitAssignMode;

/** Bounded {@link FlinkSource} for reading records. It does not monitor new snapshots. */
public class StaticFileStoreSource extends FlinkSource {

    private static final Logger LOG = LoggerFactory.getLogger(StaticFileStoreSource.class);

    private static final long serialVersionUID = 3L;

    private final int splitBatchSize;

    private final SplitAssignMode splitAssignMode;

    @Nullable private final DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo;

    public StaticFileStoreSource(
            ReadBuilder readBuilder,
            @Nullable Long limit,
            int splitBatchSize,
            SplitAssignMode splitAssignMode,
            boolean blobAsDescriptor) {
        this(readBuilder, limit, splitBatchSize, splitAssignMode, null, null, blobAsDescriptor);
    }

    public StaticFileStoreSource(
            ReadBuilder readBuilder,
            @Nullable Long limit,
            int splitBatchSize,
            SplitAssignMode splitAssignMode,
            @Nullable DynamicPartitionFilteringInfo dynamicPartitionFilteringInfo,
            @Nullable NestedProjectedRowData rowData,
            boolean blobAsDescriptor) {
        super(readBuilder, limit, rowData, blobAsDescriptor);
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

        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "Restoring enumerator with {} splits (from {}). Split details:",
                    splits.size(),
                    checkpoint == null ? "new scan" : "checkpoint");
            logSplitDetails(splits);
        }

        SplitAssigner splitAssigner =
                createSplitAssigner(context, splitBatchSize, splitAssignMode, splits);
        return new StaticFileStoreSplitEnumerator(
                context, null, splitAssigner, dynamicPartitionFilteringInfo);
    }

    private List<FileStoreSourceSplit> getSplits(
            SplitEnumeratorContext<FileStoreSourceSplit> context) {
        FileStoreSourceSplitGenerator splitGenerator = new FileStoreSourceSplitGenerator();
        TableScan scan = readBuilder.newScan();
        // register scan metrics
        if (context.metricGroup() != null) {
            ((InnerTableScan) scan)
                    .withMetricRegistry(new FlinkMetricRegistry(context.metricGroup()));
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

    private void logSplitDetails(Collection<FileStoreSourceSplit> splits) {
        int index = 0;
        for (FileStoreSourceSplit fileStoreSplit : splits) {
            Split split = fileStoreSplit.split();
            StringBuilder sb = new StringBuilder();
            sb.append(
                    String.format(
                            "Split[%d]: id=%s, recordsToSkip=%d",
                            index++, fileStoreSplit.splitId(), fileStoreSplit.recordsToSkip()));

            if (split instanceof DataSplit) {
                DataSplit dataSplit = (DataSplit) split;
                sb.append(
                        String.format(
                                ", snapshotId=%d, bucket=%d, totalBuckets=%s, bucketPath=%s, rawConvertible=%s, isStreaming=%s, rowCount=%d",
                                dataSplit.snapshotId(),
                                dataSplit.bucket(),
                                dataSplit.totalBuckets(),
                                dataSplit.bucketPath(),
                                dataSplit.rawConvertible(),
                                dataSplit.isStreaming(),
                                dataSplit.rowCount()));

                // Log partition info
                if (dataSplit.partition() != null) {
                    String partitionStr = extractPartitionFromBucketPath(dataSplit.bucketPath());
                    if (partitionStr != null && !partitionStr.isEmpty()) {
                        sb.append(", partition=").append(partitionStr);
                    }
                }

                // Log data files info
                List<DataFileMeta> dataFiles = dataSplit.dataFiles();
                if (dataFiles != null && !dataFiles.isEmpty()) {
                    sb.append(String.format(", dataFiles=[count=%d", dataFiles.size()));
                    long totalFileSize = 0;
                    long totalRowCount = 0;
                    for (DataFileMeta file : dataFiles) {
                        totalFileSize += file.fileSize();
                        totalRowCount += file.rowCount();
                    }
                    sb.append(
                            String.format(
                                    ", totalSize=%d, totalRows=%d", totalFileSize, totalRowCount));
                    if (dataFiles.size() <= 5) {
                        // Log individual files if there are few
                        sb.append(", files=[");
                        for (int i = 0; i < dataFiles.size(); i++) {
                            DataFileMeta file = dataFiles.get(i);
                            if (i > 0) {
                                sb.append(", ");
                            }
                            sb.append(
                                    String.format(
                                            "{name=%s, size=%d, rows=%d, level=%d}",
                                            file.fileName(),
                                            file.fileSize(),
                                            file.rowCount(),
                                            file.level()));
                        }
                        sb.append("]");
                    } else {
                        // Log first and last file if there are many
                        sb.append(
                                String.format(
                                        ", firstFile={name=%s, size=%d, rows=%d}, lastFile={name=%s, size=%d, rows=%d}",
                                        dataFiles.get(0).fileName(),
                                        dataFiles.get(0).fileSize(),
                                        dataFiles.get(0).rowCount(),
                                        dataFiles.get(dataFiles.size() - 1).fileName(),
                                        dataFiles.get(dataFiles.size() - 1).fileSize(),
                                        dataFiles.get(dataFiles.size() - 1).rowCount()));
                    }
                    sb.append("]");
                }

                // Log before files if present
                List<DataFileMeta> beforeFiles = dataSplit.beforeFiles();
                if (beforeFiles != null && !beforeFiles.isEmpty()) {
                    sb.append(String.format(", beforeFiles=[count=%d]", beforeFiles.size()));
                }

                // Log deletion files if present
                if (dataSplit.deletionFiles().isPresent()) {
                    sb.append(
                            String.format(
                                    ", deletionFiles=[count=%d]",
                                    dataSplit.deletionFiles().get().size()));
                }
            } else {
                sb.append(", splitType=").append(split.getClass().getSimpleName());
            }

            LOG.debug(sb.toString());
        }
    }

    /**
     * Extract partition path from bucketPath. BucketPath format is usually:
     * {warehouse}/{database.db}/{table}/{partition_path}/bucket-{bucket_id} or
     * {warehouse}/{database.db}/{table}/bucket-{bucket_id} (for non-partitioned tables)
     *
     * <p>This method extracts only the partition part (e.g., "ds=20251203" or "ds=20251203/hr=12"),
     * excluding warehouse, database, and table paths.
     */
    private String extractPartitionFromBucketPath(String bucketPath) {
        if (bucketPath == null || bucketPath.isEmpty()) {
            return null;
        }

        // Find the last occurrence of "bucket-"
        int bucketIndex = bucketPath.lastIndexOf("bucket-");
        if (bucketIndex <= 0) {
            return null;
        }

        // Extract the path before "bucket-"
        String pathBeforeBucket = bucketPath.substring(0, bucketIndex);

        // Remove trailing slash if present
        if (pathBeforeBucket.endsWith("/")) {
            pathBeforeBucket = pathBeforeBucket.substring(0, pathBeforeBucket.length() - 1);
        }

        if (pathBeforeBucket.isEmpty()) {
            return null;
        }

        // Split path into segments
        String[] segments = pathBeforeBucket.split("/");
        if (segments.length == 0) {
            return null;
        }

        // Find partition segments: segments that contain "=" (key=value format) or look like
        // partition values
        // Partition segments are typically at the end of the path, after table name
        // Strategy: find the last segment that contains "=", then include all segments from there
        // to the end
        int partitionStartIndex = -1;
        for (int i = segments.length - 1; i >= 0; i--) {
            String segment = segments[i];
            if (segment.contains("=")) {
                // Found partition segment (key=value format)
                partitionStartIndex = i;
                break;
            }
        }

        // If no "=" found, check if last segment looks like a partition value (not a table name)
        // Table names usually don't end with numbers or dates, partition values often do
        if (partitionStartIndex == -1) {
            String lastSegment = segments[segments.length - 1];
            // Heuristic: if last segment looks like a partition value (contains digits, dates,
            // etc.)
            // and is not likely a table name, treat it as partition
            if (lastSegment.matches(".*\\d+.*") && !lastSegment.endsWith(".db")) {
                partitionStartIndex = segments.length - 1;
            }
        }

        // Extract partition segments
        if (partitionStartIndex >= 0) {
            StringBuilder partitionPath = new StringBuilder();
            for (int i = partitionStartIndex; i < segments.length; i++) {
                if (partitionPath.length() > 0) {
                    partitionPath.append("/");
                }
                partitionPath.append(segments[i]);
            }
            String result = partitionPath.toString();

            // If path is too long, just show the last meaningful part
            if (result.length() > 100) {
                return "..." + result.substring(Math.max(0, result.length() - 80));
            }

            return result;
        }

        // No partition found (non-partitioned table)
        return null;
    }
}
