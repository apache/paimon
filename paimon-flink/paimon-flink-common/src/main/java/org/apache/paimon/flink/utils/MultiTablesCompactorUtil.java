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

package org.apache.paimon.flink.utils;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.catalog.Identifier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/** Utility methods for dedicated compaction job for multi-tables. */
public class MultiTablesCompactorUtil {

    private static final Logger LOG = LoggerFactory.getLogger(MultiTablesCompactorUtil.class);

    public static Map<String, String> compactOptions(boolean isStreaming) {
        if (isStreaming) {
            // set 'streaming-compact' and remove 'scan.bounded.watermark'
            return new HashMap<String, String>() {
                {
                    put(
                            CoreOptions.STREAM_SCAN_MODE.key(),
                            CoreOptions.StreamScanMode.COMPACT_BUCKET_TABLE.getValue());
                    put(CoreOptions.SCAN_BOUNDED_WATERMARK.key(), null);
                    put(CoreOptions.WRITE_ONLY.key(), "false");
                }
            };
        } else {
            // batch compactor source will compact all current files
            return new HashMap<String, String>() {
                {
                    put(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), null);
                    put(CoreOptions.SCAN_TIMESTAMP.key(), null);
                    put(CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key(), null);
                    put(CoreOptions.SCAN_SNAPSHOT_ID.key(), null);
                    put(
                            CoreOptions.SCAN_MODE.key(),
                            CoreOptions.StartupMode.LATEST_FULL.toString());
                    put(CoreOptions.WRITE_ONLY.key(), "false");
                }
            };
        }
    }

    public static Map<String, String> partitionCompactOptions() {

        return new HashMap<String, String>() {
            {
                put(CoreOptions.SCAN_TIMESTAMP_MILLIS.key(), null);
                put(CoreOptions.SCAN_TIMESTAMP.key(), null);
                put(CoreOptions.SCAN_FILE_CREATION_TIME_MILLIS.key(), null);
                put(CoreOptions.SCAN_SNAPSHOT_ID.key(), null);
                put(CoreOptions.SCAN_MODE.key(), CoreOptions.StartupMode.LATEST_FULL.toString());
                put(CoreOptions.WRITE_ONLY.key(), "false");
            }
        };
    }

    public static boolean shouldCompactTable(
            Identifier tableIdentifier, Pattern includingPattern, Pattern excludingPattern) {
        String paimonFullTableName = tableIdentifier.getFullName();
        boolean shouldCompaction = includingPattern.matcher(paimonFullTableName).matches();
        if (excludingPattern != null) {
            shouldCompaction =
                    shouldCompaction && !excludingPattern.matcher(paimonFullTableName).matches();
        }
        if (!shouldCompaction) {
            LOG.debug("Source table '{}' is excluded.", paimonFullTableName);
        }
        return shouldCompaction;
    }
}
