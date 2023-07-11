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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.consumer.Consumer;
import org.apache.paimon.consumer.ConsumerManager;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.operation.FileStoreScan;
import org.apache.paimon.table.source.snapshot.CompactedStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousCompactorStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousFromSnapshotFullStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousFromSnapshotStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousFromTimestampStartingScanner;
import org.apache.paimon.table.source.snapshot.ContinuousLatestStartingScanner;
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
import org.apache.paimon.utils.Pair;

import java.util.List;
import java.util.Optional;

import static org.apache.paimon.CoreOptions.FULL_COMPACTION_DELTA_COMMITS;
import static org.apache.paimon.utils.Preconditions.checkArgument;

/** An abstraction layer above {@link FileStoreScan} to provide input split generation. */
public abstract class AbstractInnerTableScan implements InnerTableScan {

    private final CoreOptions options;
    protected final SnapshotReader snapshotReader;

    protected AbstractInnerTableScan(CoreOptions options, SnapshotReader snapshotReader) {
        this.options = options;
        this.snapshotReader = snapshotReader;
    }

    @VisibleForTesting
    public AbstractInnerTableScan withBucket(int bucket) {
        snapshotReader.withBucket(bucket);
        return this;
    }

    public CoreOptions options() {
        return options;
    }

    protected StartingScanner createStartingScanner(boolean isStreaming) {
        CoreOptions.StreamingCompactionType type =
                options.toConfiguration().get(CoreOptions.STREAMING_COMPACT);
        switch (type) {
            case NORMAL:
                {
                    checkArgument(
                            isStreaming,
                            "Set 'streaming-compact' in batch mode. This is unexpected.");
                    return new ContinuousCompactorStartingScanner();
                }
            case BUCKET_UNAWARE:
                {
                    return new FullStartingScanner();
                }
        }

        // read from consumer id
        String consumerId = options.consumerId();
        if (consumerId != null) {
            ConsumerManager consumerManager = snapshotReader.consumerManager();
            Optional<Consumer> consumer = consumerManager.consumer(consumerId);
            if (consumer.isPresent()) {
                return new ContinuousFromSnapshotStartingScanner(consumer.get().nextSnapshot());
            }
        }

        CoreOptions.StartupMode startupMode = options.startupMode();
        switch (startupMode) {
            case LATEST_FULL:
                return new FullStartingScanner();
            case LATEST:
                return isStreaming
                        ? new ContinuousLatestStartingScanner()
                        : new FullStartingScanner();
            case COMPACTED_FULL:
                if (options.changelogProducer() == ChangelogProducer.FULL_COMPACTION
                        || options.toConfiguration().contains(FULL_COMPACTION_DELTA_COMMITS)) {
                    int deltaCommits =
                            options.toConfiguration()
                                    .getOptional(FULL_COMPACTION_DELTA_COMMITS)
                                    .orElse(1);
                    return new FullCompactedStartingScanner(deltaCommits);
                } else {
                    return new CompactedStartingScanner();
                }
            case FROM_TIMESTAMP:
                Long startupMillis = options.scanTimestampMills();
                return isStreaming
                        ? new ContinuousFromTimestampStartingScanner(startupMillis)
                        : new StaticFromTimestampStartingScanner(startupMillis);
            case FROM_SNAPSHOT:
                if (options.scanSnapshotId() != null) {
                    return isStreaming
                            ? new ContinuousFromSnapshotStartingScanner(options.scanSnapshotId())
                            : new StaticFromSnapshotStartingScanner(options.scanSnapshotId());
                } else {
                    checkArgument(!isStreaming, "Cannot scan from tag in streaming mode.");
                    return new StaticFromTagStartingScanner(options().scanTagName());
                }
            case FROM_SNAPSHOT_FULL:
                return isStreaming
                        ? new ContinuousFromSnapshotFullStartingScanner(options.scanSnapshotId())
                        : new StaticFromSnapshotStartingScanner(options.scanSnapshotId());
            case INCREMENTAL:
                checkArgument(!isStreaming, "Cannot read incremental in streaming mode.");
                Pair<String, String> incrementalBetween = options.incrementalBetween();
                if (options.toMap().get(CoreOptions.INCREMENTAL_BETWEEN.key()) != null) {
                    try {
                        return new IncrementalStartingScanner(
                                Long.parseLong(incrementalBetween.getLeft()),
                                Long.parseLong(incrementalBetween.getRight()));
                    } catch (NumberFormatException e) {
                        return new IncrementalTagStartingScanner(
                                incrementalBetween.getLeft(), incrementalBetween.getRight());
                    }
                } else {
                    return new IncrementalTimeStampStartingScanner(
                            Long.parseLong(incrementalBetween.getLeft()),
                            Long.parseLong(incrementalBetween.getRight()));
                }
            default:
                throw new UnsupportedOperationException(
                        "Unknown startup mode " + startupMode.name());
        }
    }

    public List<BinaryRow> listPartitions() {
        return snapshotReader.partitions();
    }
}
