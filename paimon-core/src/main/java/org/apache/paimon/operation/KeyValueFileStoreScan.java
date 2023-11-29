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

package org.apache.paimon.operation;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.stats.FieldStatsConverters;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.SnapshotManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** {@link FileStoreScan} for {@link KeyValueFileStore}. */
public class KeyValueFileStoreScan extends AbstractFileStoreScan {

    private final FieldStatsConverters fieldKeyStatsConverters;
    private final FieldStatsConverters fieldValueStatsConverters;
    private final CoreOptions.MergeEngine mergeEngine;
    private Predicate keyFilter;
    private Predicate valueFilter;

    public KeyValueFileStoreScan(
            RowType partitionType,
            ScanBucketFilter bucketFilter,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            long schemaId,
            KeyValueFieldsExtractor keyValueFieldsExtractor,
            ManifestFile.Factory manifestFileFactory,
            ManifestList.Factory manifestListFactory,
            int numOfBuckets,
            boolean checkNumOfBuckets,
            Integer scanManifestParallelism,
            CoreOptions.MergeEngine mergeEngine) {
        super(
                partitionType,
                bucketFilter,
                snapshotManager,
                schemaManager,
                manifestFileFactory,
                manifestListFactory,
                numOfBuckets,
                checkNumOfBuckets,
                scanManifestParallelism);
        this.mergeEngine = mergeEngine;
        this.fieldKeyStatsConverters =
                new FieldStatsConverters(
                        sid -> keyValueFieldsExtractor.keyFields(scanTableSchema(sid)), schemaId);
        this.fieldValueStatsConverters =
                new FieldStatsConverters(
                        sid -> keyValueFieldsExtractor.valueFields(scanTableSchema(sid)), schemaId);
    }

    public KeyValueFileStoreScan withKeyFilter(Predicate predicate) {
        this.keyFilter = predicate;
        this.bucketKeyFilter.pushdown(predicate);
        return this;
    }

    public KeyValueFileStoreScan withValueFilter(Predicate predicate) {
        this.valueFilter = predicate;
        return this;
    }

    /** Note: Keep this thread-safe. */
    @Override
    protected boolean filterByStats(ManifestEntry entry) {
        return keyFilter == null
                || keyFilter.test(
                        entry.file().rowCount(),
                        entry.file()
                                .keyStats()
                                .fields(
                                        fieldKeyStatsConverters.getOrCreate(
                                                entry.file().schemaId()),
                                        entry.file().rowCount()));
    }

    /** Note: Keep this thread-safe. */
    @Override
    protected List<ManifestEntry> filterWholeBucketByStats(List<ManifestEntry> entries) {
        if (valueFilter == null) {
            return entries;
        }

        // Filter 1: filter the files in the max level. If there are matching files in the max
        //           level, return those files along with the files in the non-max level.
        // Filter 2: filter the files in the whole bucket. If no files match, the whole bucket can
        //           be skipped.

        @SuppressWarnings("OptionalGetWithoutIsPresent")
        int maxLevel = entries.stream().mapToInt(entry -> entry.file().level()).max().getAsInt();

        if (canFilterMaxLevel(entries, maxLevel)) {
            List<ManifestEntry> files = new ArrayList<>();
            boolean hasMaxFile = false;
            for (ManifestEntry entry : entries) {
                if (entry.file().level() != maxLevel) {
                    files.add(entry);
                } else if (applyValueFilter(entry)) {
                    hasMaxFile = true;
                    files.add(entry);
                }
            }
            if (hasMaxFile) {
                return files;
            } else {
                // If none of the max level files match,
                // further checking if it is possible to skip the whole bucket.
                return filterWholeFiles(files);
            }
        } else {
            return filterWholeFiles(entries);
        }
    }

    private List<ManifestEntry> filterWholeFiles(List<ManifestEntry> entries) {
        for (ManifestEntry entry : entries) {
            if (applyValueFilter(entry)) {
                return entries;
            }
        }
        return Collections.emptyList();
    }

    private boolean canFilterMaxLevel(List<ManifestEntry> entries, int maxLevel) {
        // The primary keys in Level 0 overlap, so they cannot be filtered.
        if (mergeEngine != CoreOptions.MergeEngine.DEDUPLICATE || maxLevel == 0) {
            return false;
        }

        // If the minimum sequence number of the max level is greater than the maximum sequence
        // number of the non-max level, then we can filter the max level.
        long maxLevelMaxSequenceNumber = Long.MIN_VALUE;
        long nonMaxLevelMinSequenceNumber = Long.MAX_VALUE;
        for (ManifestEntry entry : entries) {
            if (entry.file().level() == maxLevel) {
                maxLevelMaxSequenceNumber =
                        Math.max(maxLevelMaxSequenceNumber, entry.file().maxSequenceNumber());
            } else {
                nonMaxLevelMinSequenceNumber =
                        Math.min(nonMaxLevelMinSequenceNumber, entry.file().minSequenceNumber());
            }
        }
        return maxLevelMaxSequenceNumber < nonMaxLevelMinSequenceNumber;
    }

    private boolean applyValueFilter(ManifestEntry entry) {
        return valueFilter.test(
                entry.file().rowCount(),
                entry.file()
                        .valueStats()
                        .fields(
                                fieldValueStatsConverters.getOrCreate(entry.file().schemaId()),
                                entry.file().rowCount()));
    }
}
