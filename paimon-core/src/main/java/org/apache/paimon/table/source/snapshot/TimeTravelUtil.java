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
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.Preconditions;
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.SnapshotNotExistException;
import org.apache.paimon.utils.TagManager;

import java.util.ArrayList;
import java.util.List;

import static org.apache.paimon.CoreOptions.SCAN_SNAPSHOT_ID;

/** The util class of resolve snapshot from scan params for time travel. */
public class TimeTravelUtil {

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

        Preconditions.checkArgument(
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
}
