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
import org.apache.paimon.utils.SnapshotManager;
import org.apache.paimon.utils.TagManager;

import java.util.ArrayList;
import java.util.List;

/** Contains time travel functions. */
public class TimeTravelUtil {

    public static List<String> scanKeys;

    public static Snapshot resolveSnapshotFromOption(
            CoreOptions options, SnapshotManager snapshotManager) {
        List<String> scanHandleKey = new ArrayList<>();
        for (String key : getScanKeys()) {
            if (options.toConfiguration().containsKey(key)) {
                scanHandleKey.add(key);
            }
        }

        if (scanHandleKey.size() == 0) {
            return null;
        }
        if (scanHandleKey.size() > 1) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s %s %s and %s can contains only one",
                            CoreOptions.SCAN_SNAPSHOT_ID.key(),
                            CoreOptions.SCAN_TAG_NAME.key(),
                            CoreOptions.SCAN_WATERMARK.key(),
                            CoreOptions.SCAN_TIMESTAMP_MILLIS.key()));
        }

        String key = scanHandleKey.get(0);
        Snapshot snapshot = null;
        if (key.equals(CoreOptions.SCAN_SNAPSHOT_ID.key())) {
            snapshot = handleSnapshotId(snapshotManager, options);
        } else if (key.equals(CoreOptions.SCAN_WATERMARK.key())) {
            snapshot = handleWatermark(snapshotManager, options);
        } else if (key.equals(CoreOptions.SCAN_TIMESTAMP_MILLIS.key())) {
            snapshot = handleTimestamp(snapshotManager, options);
        } else if (key.equals(CoreOptions.SCAN_TAG_NAME.key())) {
            snapshot = handleTagName(snapshotManager, options);
        }
        return snapshot;
    }

    private static Snapshot handleSnapshotId(SnapshotManager snapshotManager, CoreOptions options) {
        Long snapshotId = options.scanSnapshotId();
        if (snapshotId != null && snapshotManager.snapshotExists(snapshotId)){
            return snapshotManager.snapshot(snapshotId);
        }
        return null;
    }

    private static Snapshot handleTimestamp(SnapshotManager snapshotManager, CoreOptions options) {
        Long timestamp = options.scanTimestampMills();
        return snapshotManager.earlierOrEqualTimeMills(timestamp);
    }

    private static Snapshot handleWatermark(SnapshotManager snapshotManager, CoreOptions options) {
        Long watermark = options.scanWatermark();
        return snapshotManager.laterOrEqualWatermark(watermark);
    }

    private static Snapshot handleTagName(SnapshotManager snapshotManager, CoreOptions options) {
        String tagName = options.scanTagName();
        TagManager tagManager =
                new TagManager(snapshotManager.fileIO(), snapshotManager.tablePath());
        return tagManager.taggedSnapshot(tagName);
    }

    public static List<String> getScanKeys() {
        if (scanKeys == null) {
            scanKeys = new ArrayList<>();
            scanKeys.add(CoreOptions.SCAN_SNAPSHOT_ID.key());
            scanKeys.add(CoreOptions.SCAN_TAG_NAME.key());
            scanKeys.add(CoreOptions.SCAN_WATERMARK.key());
            scanKeys.add(CoreOptions.SCAN_TIMESTAMP_MILLIS.key());
        }
        return scanKeys;
    }
}
