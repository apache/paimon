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
import org.apache.paimon.flink.source.FileStoreSourceSplit;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.DataSplit;
import org.apache.paimon.table.source.TableScan;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/** Utility methods for {@link TableScan}, such as validating. */
public class TableScanUtils {

    public static void streamingReadingValidate(Table table) {
        CoreOptions options = CoreOptions.fromMap(table.options());
        CoreOptions.MergeEngine mergeEngine = options.mergeEngine();
        HashMap<CoreOptions.MergeEngine, String> mergeEngineDesc =
                new HashMap<CoreOptions.MergeEngine, String>() {
                    {
                        put(CoreOptions.MergeEngine.PARTIAL_UPDATE, "Partial update");
                        put(CoreOptions.MergeEngine.AGGREGATE, "Pre-aggregate");
                        put(CoreOptions.MergeEngine.FIRST_ROW, "First row");
                    }
                };
        if (table.primaryKeys().size() > 0 && mergeEngineDesc.containsKey(mergeEngine)) {
            if (options.changelogProducer() == CoreOptions.ChangelogProducer.NONE) {
                throw new RuntimeException(
                        mergeEngineDesc.get(mergeEngine)
                                + " streaming reading is not supported. You can use "
                                + "'lookup' or 'full-compaction' changelog producer to support streaming reading. "
                                + "('input' changelog producer is also supported, but only returns input records.)");
            }
        }
    }

    /** Get snapshot id from {@link FileStoreSourceSplit}. */
    public static Optional<Long> getSnapshotId(FileStoreSourceSplit split) {
        if (split.split() instanceof DataSplit) {
            return Optional.of(((DataSplit) split.split()).snapshotId());
        }
        return Optional.empty();
    }

    /**
     * Check whether streaming reading is supported based on the data changed before and after
     * compact.
     */
    public static boolean supportCompactDiffStreamingReading(Table table) {
        CoreOptions options = CoreOptions.fromMap(table.options());
        Set<CoreOptions.MergeEngine> compactDiffReadingEngine =
                new HashSet<CoreOptions.MergeEngine>() {
                    {
                        add(CoreOptions.MergeEngine.PARTIAL_UPDATE);
                        add(CoreOptions.MergeEngine.AGGREGATE);
                    }
                };

        return options.needLookup()
                && compactDiffReadingEngine.contains(options.mergeEngine())
                && !Options.fromMap(options.toMap())
                        .get(CoreOptions.PARTIAL_UPDATE_REMOVE_RECORD_ON_DELETE);
    }
}
