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

package org.apache.flink.table.store.table.source.snapshot;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.store.CoreOptions;
import org.apache.flink.table.store.CoreOptions.MergeEngine;
import org.apache.flink.table.store.file.schema.TableSchema;
import org.apache.flink.table.store.table.source.DataTableScan;

import javax.annotation.Nullable;

import java.util.HashMap;

import static org.apache.flink.table.store.CoreOptions.ChangelogProducer.FULL_COMPACTION;

/** Enumerate incremental changes from newly created snapshots. */
public interface SnapshotEnumerator {

    /**
     * The first call to this method will produce a {@link DataTableScan.DataFilePlan} containing
     * the base files for the following incremental changes (or just return null if there are no
     * base files).
     *
     * <p>Following calls to this method will produce {@link DataTableScan.DataFilePlan}s containing
     * incremental changed files. If there is currently no newer snapshots, null will be returned
     * instead.
     */
    @Nullable
    DataTableScan.DataFilePlan enumerate();

    static void validateContinuous(TableSchema schema) {
        CoreOptions options = new CoreOptions(schema.options());
        MergeEngine mergeEngine = options.mergeEngine();
        HashMap<MergeEngine, String> mergeEngineDesc =
                new HashMap<MergeEngine, String>() {
                    {
                        put(MergeEngine.PARTIAL_UPDATE, "Partial update");
                        put(MergeEngine.AGGREGATE, "Pre-aggregate");
                    }
                };
        if (schema.primaryKeys().size() > 0
                && mergeEngineDesc.containsKey(mergeEngine)
                && options.changelogProducer() != FULL_COMPACTION) {
            throw new ValidationException(
                    mergeEngineDesc.get(mergeEngine)
                            + " continuous reading is not supported. "
                            + "You can use full compaction changelog producer to support streaming reading.");
        }
    }
}
