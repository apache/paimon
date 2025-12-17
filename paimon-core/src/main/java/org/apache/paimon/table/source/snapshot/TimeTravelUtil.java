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

package org.apache.paimon.table.source.snapshot;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.Snapshot;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.FunctionWithException;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;

import static org.apache.paimon.CoreOptions.SCAN_SNAPSHOT_ID;
import static org.apache.paimon.CoreOptions.SCAN_TAG_NAME;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP;
import static org.apache.paimon.CoreOptions.SCAN_TIMESTAMP_MILLIS;
import static org.apache.paimon.CoreOptions.SCAN_WATERMARK;
import static org.apache.paimon.utils.DateTimeUtils.parseTimestampData;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.SnapshotManager.EARLIEST_SNAPSHOT_DEFAULT_RETRY_NUM;

/** The util class of resolve snapshot from scan params for time travel. */
public class TimeTravelUtil {

    private static final Logger LOG = LoggerFactory.getLogger(TimeTravelUtil.class);

    private static final String WATERMARK_PREFIX = "watermark-";

    private static final String[] SCAN_KEYS = {
        SCAN_SNAPSHOT_ID.key(),
        SCAN_TAG_NAME.key(),
        SCAN_WATERMARK.key(),
        SCAN_TIMESTAMP.key(),
        SCAN_TIMESTAMP_MILLIS.key()
    };

    public static Snapshot tryTravelOrLatest(FileStoreTable table) {
        return tryTravelToSnapshot(table).orElseGet(() -> table.latestSnapshot().orElse(null));
    }

    public static Optional<Snapshot> tryTravelToSnapshot(FileStoreTable table) {
        return tryTravelToSnapshot(
                table.coreOptions().toConfiguration(), table.snapshotManager(), table.tagManager());
    }

    public static Optional<Snapshot> tryTravelToSnapshot(
            Options options, SnapshotManager snapshotManager, TagManager tagManager) {
        adaptScanVersion(options, tagManager);

        List<String> scanHandleKey = new ArrayList<>(1);
        for (String key : SCAN_KEYS) {
            if (options.containsKey(key)) {
                scanHandleKey.add(key);
            }
        }

        if (scanHandleKey.isEmpty()) {
            return Optional.empty();
        }

        checkArgument(
                scanHandleKey.size() == 1,
                String.format(
                        "Only one of the following parameters may be set : %s",
                        Arrays.toString(SCAN_KEYS)));

        String key = scanHandleKey.get(0);
        CoreOptions coreOptions = new CoreOptions(options);
        Snapshot snapshot;
        if (key.equals(SCAN_SNAPSHOT_ID.key())) {
            snapshot =
                    new StaticFromSnapshotStartingScanner(
                                    snapshotManager, coreOptions.scanSnapshotId())
                            .getSnapshot();
        } else if (key.equals(SCAN_WATERMARK.key())) {
            snapshot =
                    new StaticFromWatermarkStartingScanner(
                                    snapshotManager, coreOptions.scanWatermark())
                            .getSnapshot();
        } else if (key.equals(SCAN_TIMESTAMP.key())) {
            snapshot =
                    new StaticFromTimestampStartingScanner(
                                    snapshotManager,
                                    parseTimestampData(
                                                    coreOptions.scanTimestamp(),
                                                    3,
                                                    TimeZone.getDefault())
                                            .getMillisecond())
                            .getSnapshot();
        } else if (key.equals(SCAN_TIMESTAMP_MILLIS.key())) {
            snapshot =
                    new StaticFromTimestampStartingScanner(
                                    snapshotManager, coreOptions.scanTimestampMills())
                            .getSnapshot();
        } else if (key.equals(SCAN_TAG_NAME.key())) {
            snapshot =
                    new StaticFromTagStartingScanner(snapshotManager, coreOptions.scanTagName())
                            .getSnapshot();
        } else {
            throw new UnsupportedOperationException("Unsupported time travel mode: " + key);
        }
        return Optional.of(snapshot);
    }

    public static boolean hasTimeTravelOptions(Options options) {
        if (options.containsKey(CoreOptions.SCAN_VERSION.key())) {
            return true;
        }

        for (String key : SCAN_KEYS) {
            if (options.containsKey(key)) {
                return true;
            }
        }
        return false;
    }

    private static void adaptScanVersion(Options options, TagManager tagManager) {
        String version = options.remove(CoreOptions.SCAN_VERSION.key());
        if (version == null) {
            return;
        }

        if (tagManager.tagExists(version)) {
            options.set(SCAN_TAG_NAME, version);
        } else if (version.startsWith(WATERMARK_PREFIX)) {
            long watermark = Long.parseLong(version.substring(WATERMARK_PREFIX.length()));
            options.set(SCAN_WATERMARK, watermark);
        } else if (version.chars().allMatch(Character::isDigit)) {
            options.set(SCAN_SNAPSHOT_ID.key(), version);
        } else {
            // by here, the scan version should be a tag.
            options.set(SCAN_TAG_NAME.key(), version);
            throw new RuntimeException("Cannot find a time travel version for " + version);
        }
    }

    /**
     * Returns the latest snapshot earlier than the timestamp mills. A non-existent snapshot may be
     * returned if all snapshots are equal to or later than the timestamp mills.
     */
    public static @Nullable Long earlierThanTimeMills(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            long timestampMills,
            boolean startFromChangelog,
            boolean returnNullIfTooEarly) {
        Long latest = snapshotManager.latestSnapshotId();
        if (latest == null) {
            return null;
        }

        Snapshot earliestSnapshot =
                earliestSnapshot(snapshotManager, changelogManager, startFromChangelog, latest);
        if (earliestSnapshot == null) {
            return latest - 1;
        }

        if (earliestSnapshot.timeMillis() >= timestampMills) {
            return returnNullIfTooEarly ? null : earliestSnapshot.id() - 1;
        }

        long earliest = earliestSnapshot.id();
        while (earliest < latest) {
            long mid = (earliest + latest + 1) / 2;
            Snapshot snapshot =
                    startFromChangelog
                            ? changelogOrSnapshot(snapshotManager, changelogManager, mid)
                            : snapshotManager.snapshot(mid);
            if (snapshot.timeMillis() < timestampMills) {
                earliest = mid;
            } else {
                latest = mid - 1;
            }
        }
        return earliest;
    }

    private static @Nullable Snapshot earliestSnapshot(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            boolean includeChangelog,
            @Nullable Long stopSnapshotId) {
        Long snapshotId = null;
        if (includeChangelog) {
            snapshotId = changelogManager.earliestLongLivedChangelogId();
        }
        if (snapshotId == null) {
            snapshotId = snapshotManager.earliestSnapshotId();
        }
        if (snapshotId == null) {
            return null;
        }

        if (stopSnapshotId == null) {
            stopSnapshotId = snapshotId + EARLIEST_SNAPSHOT_DEFAULT_RETRY_NUM;
        }

        FunctionWithException<Long, Snapshot, FileNotFoundException> snapshotFunction =
                includeChangelog
                        ? s -> tryGetChangelogOrSnapshot(snapshotManager, changelogManager, s)
                        : snapshotManager::tryGetSnapshot;

        do {
            try {
                return snapshotFunction.apply(snapshotId);
            } catch (FileNotFoundException e) {
                snapshotId++;
                if (snapshotId > stopSnapshotId) {
                    return null;
                }
                LOG.warn(
                        "The earliest snapshot or changelog was once identified but disappeared. "
                                + "It might have been expired by other jobs operating on this table. "
                                + "Searching for the second earliest snapshot or changelog instead. ");
            }
        } while (true);
    }

    private static Snapshot tryGetChangelogOrSnapshot(
            SnapshotManager snapshotManager, ChangelogManager changelogManager, long snapshotId)
            throws FileNotFoundException {
        if (changelogManager.longLivedChangelogExists(snapshotId)) {
            return changelogManager.tryGetChangelog(snapshotId);
        } else {
            return snapshotManager.tryGetSnapshot(snapshotId);
        }
    }

    private static Snapshot changelogOrSnapshot(
            SnapshotManager snapshotManager, ChangelogManager changelogManager, long snapshotId) {
        if (changelogManager.longLivedChangelogExists(snapshotId)) {
            return changelogManager.changelog(snapshotId);
        } else {
            return snapshotManager.snapshot(snapshotId);
        }
    }

    public static void checkRescaleBucketForIncrementalDiffQuery(
            SchemaManager schemaManager, Snapshot start, Snapshot end) {
        if (start.schemaId() != end.schemaId()) {
            int startBucketNumber = bucketNumber(schemaManager, start.schemaId());
            int endBucketNumber = bucketNumber(schemaManager, end.schemaId());
            if (startBucketNumber != endBucketNumber) {
                throw new InconsistentTagBucketException(
                        start.id(),
                        end.id(),
                        String.format(
                                "The bucket number of two snapshots are different (%s, %s), which is not supported in incremental diff query.",
                                startBucketNumber, endBucketNumber));
            }
        }
    }

    private static int bucketNumber(SchemaManager schemaManager, long schemaId) {
        TableSchema schema = schemaManager.schema(schemaId);
        return CoreOptions.fromMap(schema.options()).bucket();
    }

    /**
     * Exception thrown when the bucket number of two tags are different in incremental tag query.
     */
    public static class InconsistentTagBucketException extends RuntimeException {

        private final long startSnapshotId;
        private final long endSnapshotId;

        public InconsistentTagBucketException(
                long startSnapshotId, long endSnapshotId, String message) {
            super(message);
            this.startSnapshotId = startSnapshotId;
            this.endSnapshotId = endSnapshotId;
        }

        public long startSnapshotId() {
            return startSnapshotId;
        }

        public long endSnapshotId() {
            return endSnapshotId;
        }
    }

    @Nullable
    public static Long scanTimestampMills(Options options) {
        String timestampStr = options.get(SCAN_TIMESTAMP);
        Long timestampMillis = options.get(SCAN_TIMESTAMP_MILLIS);
        if (timestampMillis == null && timestampStr != null) {
            return parseTimestampData(timestampStr, 3, TimeZone.getDefault()).getMillisecond();
        }
        return timestampMillis;
    }
}
