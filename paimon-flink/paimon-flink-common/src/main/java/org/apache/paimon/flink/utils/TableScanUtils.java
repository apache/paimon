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
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.BatchDataTableScan;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.StreamDataTableScan;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.table.source.snapshot.BoundedChecker;
import org.apache.paimon.table.source.snapshot.ContinuousCompactorFollowUpScanner;
import org.apache.paimon.table.source.snapshot.ContinuousCompactorStartingScanner;
import org.apache.paimon.table.source.snapshot.FullStartingScanner;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.HashMap;

/** Utility methods for {@link TableScan}, such as validating and creating. */
public class TableScanUtils {

    public static void streamingReadingValidate(Table table) {
        CoreOptions options = CoreOptions.fromMap(table.options());
        CoreOptions.MergeEngine mergeEngine = options.mergeEngine();
        HashMap<CoreOptions.MergeEngine, String> mergeEngineDesc =
                new HashMap<CoreOptions.MergeEngine, String>() {
                    {
                        put(CoreOptions.MergeEngine.PARTIAL_UPDATE, "Partial update");
                        put(CoreOptions.MergeEngine.AGGREGATE, "Pre-aggregate");
                    }
                };
        if (table.primaryKeys().size() > 0 && mergeEngineDesc.containsKey(mergeEngine)) {
            switch (options.changelogProducer()) {
                case NONE:
                case INPUT:
                    throw new RuntimeException(
                            mergeEngineDesc.get(mergeEngine)
                                    + " streaming reading is not supported. You can use "
                                    + "'lookup' or 'full-compaction' changelog producer to support streaming reading.");
                default:
            }
        }
    }

    // ------------------------------------------------------------------------
    // TableScan factories
    // ------------------------------------------------------------------------

    /** Factory to create batch {@link TableScan}. */
    public interface TableScanFactory extends Serializable {
        TableScan create(ReadBuilder readBuilder);
    }

    /** Factory to create {@link StreamTableScan}. */
    public interface StreamTableScanFactory extends Serializable {

        StreamTableScan create(ReadBuilder readBuilder, @Nullable Long nextSnapshotId);
    }

    public static StreamTableScanFactory defaultStreamScanFactory() {
        return (builder, nextSnapshotId) -> {
            StreamTableScan scan = builder.newStreamScan();
            scan.restore(nextSnapshotId);
            return scan;
        };
    }

    public static TableScanFactory compactBatchScanFactory() {
        return readBuilder -> {
            BatchDataTableScan scan = (BatchDataTableScan) readBuilder.newScan();
            // static compactor source will compact all current files
            scan.withStartingScanner(new FullStartingScanner());
            return scan;
        };
    }

    public static StreamTableScanFactory compactStreamScanFactory() {
        return (readBuilder, nextSnapshotId) -> {
            StreamDataTableScan scan = (StreamDataTableScan) readBuilder.newStreamScan();
            scan.withStartingScanner(new ContinuousCompactorStartingScanner())
                    .withFollowUpScanner(new ContinuousCompactorFollowUpScanner())
                    .withBoundedChecker(BoundedChecker.neverEnd());
            scan.restore(nextSnapshotId);
            return scan;
        };
    }
}
