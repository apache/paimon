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

package org.apache.paimon.table.source;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.Snapshot;
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.metrics.MetricRegistry;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.source.snapshot.CompactedStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousCompactorStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousFromSnapshotFullStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousFromSnapshotStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousFromTimestampStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousLatestStartingScanner;
import org.apache.paimon.table.source.snapshot.FileCreationTimeStartingScanner;
import org.apache.paimon.table.source.snapshot.FullCompactedStartingScanner;
import org.apache.paimon.table.source.snapshot.FullStartingScanner;
import org.apache.paimon.table.source.snapshot.IncrementalStartingScanner;
import org.apache.paimon.table.source.snapshot.IncrementalTagStartingScanner;
import org.apache.paimon.table.source.snapshot.IncrementalTimeStampStartingScanner;
import org.apache.paimon.table.source.snapshot.SnapshotReader;
import org.apache.paimon.table.source.snapshot.StartingScanner;
import org.apache.paimon.table.source.snapshot.StaticFromSnapshotStartingScanner;
import org.apache.paimon.table.source.snapshot.StaticFromTagStartingScanner;
import org.apache.paimon.table.source.snapshot.StaticFromTimestampStartingScanner;
import org.apache.paimon.table.source.snapshot.StaticFromWatermarkStartingScanner;
import org.apache.paimon.tag.Tag;
import org.apache.paimon.utils.Filter;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.paimon.CoreOptions.FULL_COMPACTION_DELTA_COMMITS;
import static org.apache.paimon.CoreOptions.INCREMENTAL_BETWEEN;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.Preconditions.checkNotNull;

/** An abstraction layer above {@link FileStoreScan} to provide input split generation. */
public abstract class AbstractDataTableScan implements DataTableScan {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractDataTableScan.class);

    private final CoreOptions options;
    protected final SnapshotReader snapshotReader;

    protected AbstractDataTableScan(CoreOptions options, SnapshotReader snapshotReader) {
        this.options = options;
        this.snapshotReader = snapshotReader;
    }

    @VisibleForTesting
    public AbstractDataTableScan withBucket(int bucket) {
        snapshotReader.withBucket(bucket);
        return this;
    }

    @Override
    public AbstractDataTableScan withBucketFilter(Filter<Integer> bucketFilter) {
        snapshotReader.withBucketFilter(bucketFilter);
        return this;
    }

    @Override
    public AbstractDataTableScan withPartitionFilter(Map<String, String> partitionSpec) {
        snapshotReader.withPartitionFilter(partitionSpec);
        return this;
    }

    @Override
    public AbstractDataTableScan withPartitionFilter(List<BinaryRow> partitions) {
        snapshotReader.withPartitionFilter(partitions);
        return this;
    }

    @Override
    public AbstractDataTableScan withPartitionsFilter(List<Map<String, String>> partitions) {
        snapshotReader.withPartitionsFilter(partitions);
        return this;
    }

    @Override
    public AbstractDataTableScan withLevelFilter(Filter<Integer> levelFilter) {
        snapshotReader.withLevelFilter(levelFilter);
        return this;
    }

    public AbstractDataTableScan withMetricsRegistry(MetricRegistry metricsRegistry) {
        snapshotReader.withMetricRegistry(metricsRegistry);
        return this;
    }

    @Override
    public AbstractDataTableScan dropStats() {
        snapshotReader.dropStats();
        return this;
    }

    public CoreOptions options() {
        return options;
    }

    protected StartingScanner createStartingScanner(boolean isStreaming) {
        SnapshotManager snapshotManager = snapshotReader.snapshotManager();
        CoreOptions.StreamScanMode type =
                options.toConfiguration().get(CoreOptions.STREAM_SCAN_MODE);
        switch (type) {
            case COMPACT_BUCKET_TABLE:
                checkArgument(
                        isStreaming, "Set 'streaming-compact' in batch mode. This is unexpected.");
                return new ContinuousCompactorStartingScanner(snapshotManager);
            case FILE_MONITOR:
                return new FullStartingScanner(snapshotManager);
        }

        // read from consumer id
        String consumerId = options.consumerId();
        if (isStreaming && consumerId != null && !options.consumerIgnoreProgress()) {
            ConsumerManager consumerManager = snapshotReader.consumerManager();
            Optional<Consumer> consumer = consumerManager.consumer(consumerId);
            if (consumer.isPresent()) {
                return new ContinuousFromSnapshotStartingScanner(
                        snapshotManager,
                        consumer.get().nextSnapshot(),
                        options.changelogLifecycleDecoupled());
            }
        }

        CoreOptions.StartupMode startupMode = options.startupMode();
        switch (startupMode) {
            case LATEST_FULL:
                return new FullStartingScanner(snapshotManager);
            case LATEST:
                return isStreaming
                        ? new ContinuousLatestStartingScanner(snapshotManager)
                        : new FullStartingScanner(snapshotManager);
            case COMPACTED_FULL:
                if (options.changelogProducer() == ChangelogProducer.FULL_COMPACTION
                        || options.toConfiguration().contains(FULL_COMPACTION_DELTA_COMMITS)) {
                    int deltaCommits =
                            options.toConfiguration()
                                    .getOptional(FULL_COMPACTION_DELTA_COMMITS)
                                    .orElse(1);
                    return new FullCompactedStartingScanner(snapshotManager, deltaCommits);
                } else {
                    return new CompactedStartingScanner(snapshotManager);
                }
            case FROM_TIMESTAMP:
                Long startupMillis = options.scanTimestampMills();
                return isStreaming
                        ? new ContinuousFromTimestampStartingScanner(
                                snapshotManager,
                                startupMillis,
                                options.changelogLifecycleDecoupled())
                        : new StaticFromTimestampStartingScanner(snapshotManager, startupMillis);
            case FROM_FILE_CREATION_TIME:
                Long fileCreationTimeMills = options.scanFileCreationTimeMills();
                return new FileCreationTimeStartingScanner(snapshotManager, fileCreationTimeMills);
            case FROM_SNAPSHOT:
                if (options.scanSnapshotId() != null) {
                    return isStreaming
                            ? new ContinuousFromSnapshotStartingScanner(
                                    snapshotManager,
                                    options.scanSnapshotId(),
                                    options.changelogLifecycleDecoupled())
                            : new StaticFromSnapshotStartingScanner(
                                    snapshotManager, options.scanSnapshotId());
                } else if (options.scanWatermark() != null) {
                    checkArgument(!isStreaming, "Cannot scan from watermark in streaming mode.");
                    return new StaticFromWatermarkStartingScanner(
                            snapshotManager, options().scanWatermark());
                } else if (options.scanTagName() != null) {
                    checkArgument(!isStreaming, "Cannot scan from tag in streaming mode.");
                    return new StaticFromTagStartingScanner(
                            snapshotManager, options().scanTagName());
                } else {
                    throw new UnsupportedOperationException("Unknown snapshot read mode");
                }
            case FROM_SNAPSHOT_FULL:
                Long scanSnapshotId = options.scanSnapshotId();
                checkNotNull(
                        scanSnapshotId,
                        "scan.snapshot-id must be set when startupMode is FROM_SNAPSHOT_FULL.");
                return isStreaming
                        ? new ContinuousFromSnapshotFullStartingScanner(
                                snapshotManager, scanSnapshotId)
                        : new StaticFromSnapshotStartingScanner(snapshotManager, scanSnapshotId);
            case INCREMENTAL:
                checkArgument(!isStreaming, "Cannot read incremental in streaming mode.");
                return createIncrementalStartingScanner(snapshotManager);
            default:
                throw new UnsupportedOperationException(
                        "Unknown startup mode " + startupMode.name());
        }
    }

    private StartingScanner createIncrementalStartingScanner(SnapshotManager snapshotManager) {
        CoreOptions.IncrementalBetweenScanMode scanType = options.incrementalBetweenScanMode();
        ScanMode scanMode;
        switch (scanType) {
            case AUTO:
                scanMode =
                        options.changelogProducer() == ChangelogProducer.NONE
                                ? ScanMode.DELTA
                                : ScanMode.CHANGELOG;
                break;
            case DELTA:
                scanMode = ScanMode.DELTA;
                break;
            case CHANGELOG:
                scanMode = ScanMode.CHANGELOG;
                break;
            default:
                throw new UnsupportedOperationException(
                        "Unknown incremental scan type " + scanType.name());
        }

        Options conf = options.toConfiguration();
        TagManager tagManager =
                new TagManager(snapshotManager.fileIO(), snapshotManager.tablePath());
        if (conf.contains(CoreOptions.INCREMENTAL_BETWEEN)) {
            Pair<String, String> incrementalBetween = options.incrementalBetween();
            Optional<Tag> startTag = tagManager.get(incrementalBetween.getLeft());
            Optional<Tag> endTag = tagManager.get(incrementalBetween.getRight());
            if (startTag.isPresent() && endTag.isPresent()) {
                Snapshot start = startTag.get().trimToSnapshot();
                Snapshot end = endTag.get().trimToSnapshot();

                LOG.info(
                        "{} start and end are parsed to tag with snapshot id {} to {}.",
                        INCREMENTAL_BETWEEN.key(),
                        start.id(),
                        end.id());

                if (end.id() <= start.id()) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Tag end %s with snapshot id %s should be larger than tag start %s with snapshot id %s",
                                    incrementalBetween.getRight(),
                                    end.id(),
                                    incrementalBetween.getLeft(),
                                    start.id()));
                }
                return new IncrementalTagStartingScanner(snapshotManager, start, end);
            } else {
                long startId, endId;
                try {
                    startId = Long.parseLong(incrementalBetween.getLeft());
                    endId = Long.parseLong(incrementalBetween.getRight());
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Didn't find two tags for start '%s' and end '%s', and they are not two snapshot Ids. "
                                            + "Please set two tags or two snapshot Ids.",
                                    incrementalBetween.getLeft(), incrementalBetween.getRight()));
                }
                return new IncrementalStartingScanner(snapshotManager, startId, endId, scanMode);
            }
        } else if (conf.contains(CoreOptions.INCREMENTAL_BETWEEN_TIMESTAMP)) {
            Pair<String, String> incrementalBetween = options.incrementalBetween();
            return new IncrementalTimeStampStartingScanner(
                    snapshotManager,
                    Long.parseLong(incrementalBetween.getLeft()),
                    Long.parseLong(incrementalBetween.getRight()),
                    scanMode);
        } else if (conf.contains(CoreOptions.INCREMENTAL_TO_AUTO_TAG)) {
            String endTag = options.incrementalToAutoTag();
            return IncrementalTagStartingScanner.create(snapshotManager, endTag, options);
        } else {
            throw new UnsupportedOperationException("Unknown incremental read mode.");
        }
    }
}
