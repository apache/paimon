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

import org.apache.paimon.CoreOptions.ChangelogProducer;
import org.apache.paimon.CoreOptions.MergeEngine;
import org.apache.paimon.KeyValueFileStore;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStatsEvolution;
import org.apache.paimon.stats.SimpleStatsEvolutions;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.SnapshotManager;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.paimon.CoreOptions.MergeEngine.AGGREGATE;
import static org.apache.paimon.CoreOptions.MergeEngine.FIRST_ROW;
import static org.apache.paimon.CoreOptions.MergeEngine.PARTIAL_UPDATE;

/** {@link FileStoreScan} for {@link KeyValueFileStore}. */
public class KeyValueFileStoreScan extends AbstractFileStoreScan {

    private final SimpleStatsEvolutions fieldKeyStatsConverters;
    private final SimpleStatsEvolutions fieldValueStatsConverters;
    private final BucketSelectConverter bucketSelectConverter;

    private Predicate keyFilter;
    private Predicate valueFilter;
    private final boolean deletionVectorsEnabled;
    private final MergeEngine mergeEngine;
    private final ChangelogProducer changelogProducer;

    public KeyValueFileStoreScan(
            ManifestsReader manifestsReader,
            BucketSelectConverter bucketSelectConverter,
            SnapshotManager snapshotManager,
            SchemaManager schemaManager,
            TableSchema schema,
            KeyValueFieldsExtractor keyValueFieldsExtractor,
            ManifestFile.Factory manifestFileFactory,
            Integer scanManifestParallelism,
            boolean deletionVectorsEnabled,
            MergeEngine mergeEngine,
            ChangelogProducer changelogProducer) {
        super(
                manifestsReader,
                snapshotManager,
                schemaManager,
                schema,
                manifestFileFactory,
                scanManifestParallelism);
        this.bucketSelectConverter = bucketSelectConverter;
        this.fieldKeyStatsConverters =
                new SimpleStatsEvolutions(
                        sid -> keyValueFieldsExtractor.keyFields(scanTableSchema(sid)),
                        schema.id());
        this.fieldValueStatsConverters =
                new SimpleStatsEvolutions(
                        sid -> keyValueFieldsExtractor.valueFields(scanTableSchema(sid)),
                        schema.id());
        this.deletionVectorsEnabled = deletionVectorsEnabled;
        this.mergeEngine = mergeEngine;
        this.changelogProducer = changelogProducer;
    }

    public KeyValueFileStoreScan withKeyFilter(Predicate predicate) {
        this.keyFilter = predicate;
        this.bucketSelectConverter.convert(predicate).ifPresent(this::withTotalAwareBucketFilter);
        return this;
    }

    public KeyValueFileStoreScan withValueFilter(Predicate predicate) {
        this.valueFilter = predicate;
        return this;
    }

    /** Note: Keep this thread-safe. */
    @Override
    protected boolean filterByStats(ManifestEntry entry) {
        DataFileMeta file = entry.file();
        if (isValueFilterEnabled(entry) && !filterByValueFilter(entry)) {
            return false;
        }

        if (keyFilter != null) {
            SimpleStatsEvolution.Result stats =
                    fieldKeyStatsConverters
                            .getOrCreate(file.schemaId())
                            .evolution(file.keyStats(), file.rowCount(), null);
            return keyFilter.test(
                    file.rowCount(), stats.minValues(), stats.maxValues(), stats.nullCounts());
        }

        return true;
    }

    private boolean isValueFilterEnabled(ManifestEntry entry) {
        if (valueFilter == null) {
            return false;
        }

        switch (scanMode) {
            case ALL:
                return (deletionVectorsEnabled || mergeEngine == FIRST_ROW) && entry.level() > 0;
            case DELTA:
                return false;
            case CHANGELOG:
                return changelogProducer == ChangelogProducer.LOOKUP
                        || changelogProducer == ChangelogProducer.FULL_COMPACTION;
            default:
                throw new UnsupportedOperationException("Unsupported scan mode: " + scanMode);
        }
    }

    /** Note: Keep this thread-safe. */
    @Override
    protected List<ManifestEntry> filterWholeBucketByStats(List<ManifestEntry> entries) {
        if (valueFilter == null || scanMode != ScanMode.ALL) {
            return entries;
        }

        return noOverlapping(entries)
                ? filterWholeBucketPerFile(entries)
                : filterWholeBucketAllFiles(entries);
    }

    private List<ManifestEntry> filterWholeBucketPerFile(List<ManifestEntry> entries) {
        List<ManifestEntry> filtered = new ArrayList<>();
        for (ManifestEntry entry : entries) {
            if (filterByValueFilter(entry)) {
                filtered.add(entry);
            }
        }
        return filtered;
    }

    private List<ManifestEntry> filterWholeBucketAllFiles(List<ManifestEntry> entries) {
        if (!deletionVectorsEnabled
                && (mergeEngine == PARTIAL_UPDATE || mergeEngine == AGGREGATE)) {
            return entries;
        }

        // entries come from the same bucket, if any of it doesn't meet the request, we could
        // filter the bucket.
        for (ManifestEntry entry : entries) {
            if (filterByValueFilter(entry)) {
                return entries;
            }
        }
        return Collections.emptyList();
    }

    private boolean filterByValueFilter(ManifestEntry entry) {
        DataFileMeta file = entry.file();
        SimpleStatsEvolution.Result result =
                fieldValueStatsConverters
                        .getOrCreate(file.schemaId())
                        .evolution(file.valueStats(), file.rowCount(), file.valueStatsCols());
        return valueFilter.test(
                file.rowCount(), result.minValues(), result.maxValues(), result.nullCounts());
    }

    private static boolean noOverlapping(List<ManifestEntry> entries) {
        if (entries.size() <= 1) {
            return true;
        }

        Integer previousLevel = null;
        for (ManifestEntry entry : entries) {
            int level = entry.file().level();
            // level 0 files have overlapping
            if (level == 0) {
                return false;
            }

            if (previousLevel == null) {
                previousLevel = level;
            } else {
                // different level, have overlapping
                if (previousLevel != level) {
                    return false;
                }
            }
        }

        return true;
    }
}
