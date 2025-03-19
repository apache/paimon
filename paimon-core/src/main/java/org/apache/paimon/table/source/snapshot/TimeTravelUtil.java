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
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.ChangelogManager;
import org.apache.paimon.utils.FunctionWithException;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.SnapshotNotExistException;
import org.apache.paimon.utils.TagManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.CoreOptions.SCAN_SNAPSHOT_ID;
import static org.apache.paimon.utils.Preconditions.checkArgument;
import static org.apache.paimon.utils.SnapshotManager.EARLIEST_SNAPSHOT_DEFAULT_RETRY_NUM;

/** The util class of resolve snapshot from scan params for time travel. */
public class TimeTravelUtil {

    private static final Logger LOG = LoggerFactory.getLogger(TimeTravelUtil.class);

    private static final String[] SCAN_KEYS = {
        CoreOptions.SCAN_SNAPSHOT_ID.key(),
        CoreOptions.SCAN_TAG_NAME.key(),
        CoreOptions.SCAN_WATERMARK.key(),
        CoreOptions.SCAN_TIMESTAMP_MILLIS.key()
    };

    public static Snapshot resolveSnapshot(FileStoreTable table) {
        return resolveSnapshotFromOptions(table.coreOptions(), table.snapshotManager());
    }

    public static Snapshot resolveSnapshotFromOptions(
            CoreOptions options, SnapshotManager snapshotManager) {
        List<String> scanHandleKey = new ArrayList<>(1);
        for (String key : SCAN_KEYS) {
            if (options.toConfiguration().containsKey(key)) {
                scanHandleKey.add(key);
            }
        }

        if (scanHandleKey.size() == 0) {
            return snapshotManager.latestSnapshot();
        }

        checkArgument(
                scanHandleKey.size() == 1,
                String.format(
                        "Only one of the following parameters may be set : [%s, %s, %s, %s]",
                        CoreOptions.SCAN_SNAPSHOT_ID.key(),
                        CoreOptions.SCAN_TAG_NAME.key(),
                        CoreOptions.SCAN_WATERMARK.key(),
                        CoreOptions.SCAN_TIMESTAMP_MILLIS.key()));

        String key = scanHandleKey.get(0);
        Snapshot snapshot = null;
        if (key.equals(CoreOptions.SCAN_SNAPSHOT_ID.key())) {
            snapshot = resolveSnapshotBySnapshotId(snapshotManager, options);
        } else if (key.equals(CoreOptions.SCAN_WATERMARK.key())) {
            snapshot = resolveSnapshotByWatermark(snapshotManager, options);
        } else if (key.equals(CoreOptions.SCAN_TIMESTAMP_MILLIS.key())) {
            snapshot = resolveSnapshotByTimestamp(snapshotManager, options);
        } else if (key.equals(CoreOptions.SCAN_TAG_NAME.key())) {
            snapshot = resolveSnapshotByTagName(snapshotManager, options);
        }

        if (snapshot == null) {
            snapshot = snapshotManager.latestSnapshot();
        }
        return snapshot;
    }

    private static Snapshot resolveSnapshotBySnapshotId(
            SnapshotManager snapshotManager, CoreOptions options) {
        Long snapshotId = options.scanSnapshotId();
        if (snapshotId != null) {
            if (!snapshotManager.snapshotExists(snapshotId)) {
                Long earliestSnapshotId = snapshotManager.earliestSnapshotId();
                Long latestSnapshotId = snapshotManager.latestSnapshotId();
                throw new SnapshotNotExistException(
                        String.format(
                                "Specified parameter %s = %s is not exist, you can set it in range from %s to %s.",
                                SCAN_SNAPSHOT_ID.key(),
                                snapshotId,
                                earliestSnapshotId,
                                latestSnapshotId));
            }
            return snapshotManager.snapshot(snapshotId);
        }
        return null;
    }

    private static Snapshot resolveSnapshotByTimestamp(
            SnapshotManager snapshotManager, CoreOptions options) {
        Long timestamp = options.scanTimestampMills();
        return snapshotManager.earlierOrEqualTimeMills(timestamp);
    }

    private static Snapshot resolveSnapshotByWatermark(
            SnapshotManager snapshotManager, CoreOptions options) {
        Long watermark = options.scanWatermark();
        return snapshotManager.laterOrEqualWatermark(watermark);
    }

    private static Snapshot resolveSnapshotByTagName(
            SnapshotManager snapshotManager, CoreOptions options) {
        String tagName = options.scanTagName();
        TagManager tagManager =
                new TagManager(snapshotManager.fileIO(), snapshotManager.tablePath());
        return tagManager.getOrThrow(tagName).trimToSnapshot();
    }

    /**
     * Returns the latest snapshot earlier than the timestamp mills. A non-existent snapshot may be
     * returned if all snapshots are equal to or later than the timestamp mills.
     */
    public static @Nullable Long earlierThanTimeMills(
            SnapshotManager snapshotManager,
            ChangelogManager changelogManager,
            long timestampMills,
            boolean startFromChangelog) {
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
            return earliestSnapshot.id() - 1;
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

    public static void checkRescaleBucketForIncrementalTagQuery(
            SchemaManager schemaManager, Snapshot start, Snapshot end) {
        if (start.schemaId() != end.schemaId()) {
            int startBucketNumber = bucketNumber(schemaManager, start.schemaId());
            int endBucketNumber = bucketNumber(schemaManager, end.schemaId());
            if (startBucketNumber != endBucketNumber) {
                throw new InconsistentTagBucketException(
                        start.id(),
                        end.id(),
                        String.format(
                                "The bucket number of two tags are different (%s, %s), which is not supported in incremental tag query.",
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
}
