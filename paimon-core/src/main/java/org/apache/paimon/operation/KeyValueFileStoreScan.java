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
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fileindex.FileIndexPredicate;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FilteredManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStatsEvolution;
import org.apache.paimon.stats.SimpleStatsEvolutions;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.MergeEngine.AGGREGATE;
import static org.apache.paimon.CoreOptions.MergeEngine.PARTIAL_UPDATE;

/** {@link FileStoreScan} for {@link KeyValueFileStore}. */
public class KeyValueFileStoreScan extends AbstractFileStoreScan {

    private static final Logger LOG = LoggerFactory.getLogger(KeyValueFileStoreScan.class);

    private final SimpleStatsEvolutions fieldKeyStatsConverters;
    private final SimpleStatsEvolutions fieldValueStatsConverters;
    private final BucketSelectConverter bucketSelectConverter;
    private final boolean deletionVectorsEnabled;
    private final MergeEngine mergeEngine;
    private final ChangelogProducer changelogProducer;
    private final boolean fileIndexReadEnabled;

    private Predicate keyFilter;
    private Predicate valueFilter;
    private boolean valueFilterForceEnabled = false;

    // cache not evolved filter by schema id
    private final Map<Long, Predicate> notEvolvedKeyFilterMapping = new ConcurrentHashMap<>();

    // cache not evolved filter by schema id
    private final Map<Long, Predicate> notEvolvedValueFilterMapping = new ConcurrentHashMap<>();

    // cache evolved filter by schema id
    private final Map<Long, Predicate> evolvedValueFilterMapping = new ConcurrentHashMap<>();

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
            ChangelogProducer changelogProducer,
            boolean fileIndexReadEnabled) {
        super(
                manifestsReader,
                snapshotManager,
                schemaManager,
                schema,
                manifestFileFactory,
                scanManifestParallelism);
        this.bucketSelectConverter = bucketSelectConverter;
        // NOTE: don't add key prefix to field names because fieldKeyStatsConverters is used for
        // filter conversion
        this.fieldKeyStatsConverters =
                new SimpleStatsEvolutions(
                        sid -> scanTableSchema(sid).trimmedPrimaryKeysFields(), schema.id());
        this.fieldValueStatsConverters =
                new SimpleStatsEvolutions(
                        sid -> keyValueFieldsExtractor.valueFields(scanTableSchema(sid)),
                        schema.id());
        this.deletionVectorsEnabled = deletionVectorsEnabled;
        this.mergeEngine = mergeEngine;
        this.changelogProducer = changelogProducer;
        this.fileIndexReadEnabled = fileIndexReadEnabled;
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

    @Override
    public FileStoreScan enableValueFilter() {
        this.valueFilterForceEnabled = true;
        return this;
    }

    /** Note: Keep this thread-safe. */
    @Override
    protected boolean filterByStats(ManifestEntry entry) {
        if (isValueFilterEnabled() && !filterByValueFilter(entry)) {
            return false;
        }

        Predicate notEvolvedFilter =
                notEvolvedKeyFilterMapping.computeIfAbsent(
                        entry.file().schemaId(),
                        id ->
                                // keepNewFieldFilter to handle add field
                                // for example, add field 'c', 'c > 3': old files can be filtered
                                fieldKeyStatsConverters.filterUnsafeFilter(
                                        entry.file().schemaId(), keyFilter, true));
        if (notEvolvedFilter == null) {
            return true;
        }

        DataFileMeta file = entry.file();
        SimpleStatsEvolution.Result stats =
                fieldKeyStatsConverters
                        .getOrCreate(file.schemaId())
                        .evolution(file.keyStats(), file.rowCount(), null);
        return notEvolvedFilter.test(
                file.rowCount(), stats.minValues(), stats.maxValues(), stats.nullCounts());
    }

    @Override
    protected ManifestEntry dropStats(ManifestEntry entry) {
        if (!isValueFilterEnabled() && postFilterManifestEntriesEnabled()) {
            return new FilteredManifestEntry(entry.copyWithoutStats(), filterByValueFilter(entry));
        }
        return entry.copyWithoutStats();
    }

    private boolean filterByFileIndex(@Nullable byte[] embeddedIndexBytes, ManifestEntry entry) {
        if (embeddedIndexBytes == null) {
            return true;
        }

        RowType dataRowType = scanTableSchema(entry.file().schemaId()).logicalRowType();
        try (FileIndexPredicate predicate =
                new FileIndexPredicate(embeddedIndexBytes, dataRowType)) {
            Predicate dataPredicate =
                    evolvedValueFilterMapping.computeIfAbsent(
                            entry.file().schemaId(),
                            id ->
                                    fieldValueStatsConverters.tryDevolveFilter(
                                            entry.file().schemaId(), valueFilter));
            return predicate.evaluate(dataPredicate).remain();
        } catch (IOException e) {
            throw new RuntimeException("Exception happens while checking fileIndex predicate.", e);
        }
    }

    private boolean isValueFilterEnabled() {
        if (valueFilter == null) {
            return false;
        }

        switch (scanMode) {
            case ALL:
                return valueFilterForceEnabled;
            case DELTA:
                return false;
            case CHANGELOG:
                return changelogProducer == ChangelogProducer.LOOKUP
                        || changelogProducer == ChangelogProducer.FULL_COMPACTION;
            default:
                throw new UnsupportedOperationException("Unsupported scan mode: " + scanMode);
        }
    }

    /**
     * Check if limit pushdown is supported for PK tables.
     *
     * <p>Not supported when merge engine is PARTIAL_UPDATE/AGGREGATE (need merge) or deletion
     * vectors are enabled (can't count deleted rows). For DEDUPLICATE/FIRST_ROW, per-bucket checks
     * (no overlapping, no delete rows) are done in applyLimitPushdownForBucket.
     */
    @Override
    public boolean supportsLimitPushManifestEntries() {
        if (mergeEngine == PARTIAL_UPDATE || mergeEngine == AGGREGATE) {
            return false;
        }

        return limit != null && limit > 0 && !deletionVectorsEnabled;
    }

    /**
     * Apply limit pushdown for a single bucket. Returns files to include, or null if unsafe.
     *
     * <p>Returns null if files overlap (LSM level 0 or different levels) or have delete rows. For
     * non-overlapping files with no delete rows, accumulates row counts until limit is reached.
     *
     * @param bucketEntries files in the same bucket
     * @param limit the limit to apply
     * @return files to include, or null if we can't safely push down limit
     */
    @Nullable
    private List<ManifestEntry> applyLimitPushdownForBucket(
            List<ManifestEntry> bucketEntries, long limit) {
        // Check if this bucket has overlapping files (LSM property)
        boolean hasOverlapping = !noOverlapping(bucketEntries);

        if (hasOverlapping) {
            // For buckets with overlapping, we can't safely push down limit because files
            // need to be merged and we can't accurately calculate the merged row count.
            return null;
        }

        // For buckets without overlapping and with merge engines that don't require
        // merge (DEDUPLICATE or FIRST_ROW), we can safely accumulate row count
        // and stop when limit is reached, but only if files have no delete rows.
        List<ManifestEntry> result = new ArrayList<>();
        long accumulatedRowCount = 0;

        for (ManifestEntry entry : bucketEntries) {
            long fileRowCount = entry.file().rowCount();
            // Check if file has delete rows - if so, we can't accurately calculate
            // the merged row count, so we need to stop limit pushdown
            boolean hasDeleteRows =
                    entry.file().deleteRowCount().map(count -> count > 0L).orElse(false);

            if (hasDeleteRows) {
                // If file has delete rows, we can't accurately calculate merged row count
                // without reading the actual data. Can't safely push down limit.
                return null;
            }

            // File has no delete rows, no overlapping, and merge engine doesn't require merge.
            // Safe to count rows.
            result.add(entry);
            accumulatedRowCount += fileRowCount;
            if (accumulatedRowCount >= limit) {
                break;
            }
        }

        return result;
    }

    @Override
    protected boolean postFilterManifestEntriesEnabled() {
        return (valueFilter != null && scanMode == ScanMode.ALL)
                || supportsLimitPushManifestEntries();
    }

    @Override
    protected List<ManifestEntry> postFilterManifestEntries(List<ManifestEntry> files) {
        long startTime = System.nanoTime();
        Map<Pair<BinaryRow, Integer>, List<ManifestEntry>> buckets = groupByBucket(files);

        // Apply filter if valueFilter is enabled, otherwise use identity function
        Function<List<ManifestEntry>, List<ManifestEntry>> bucketProcessor =
                (valueFilter != null && scanMode == ScanMode.ALL)
                        ? this::doFilterWholeBucketByStats
                        : Function.identity();

        // Apply filter (if enabled) and limit pushdown (if enabled)
        boolean limitEnabled = supportsLimitPushManifestEntries();
        List<ManifestEntry> result =
                applyLimitPushdownToBuckets(buckets, bucketProcessor, limitEnabled);

        if (limitEnabled) {
            long duration = (System.nanoTime() - startTime) / 1_000_000;
            LOG.info(
                    "Limit pushdown for PK table completed in {} ms. Limit: {}, InputFiles: {}, OutputFiles: {}, "
                            + "MergeEngine: {}, ScanMode: {}, DeletionVectorsEnabled: {}",
                    duration,
                    limit,
                    files.size(),
                    result.size(),
                    mergeEngine,
                    scanMode,
                    deletionVectorsEnabled);
        }

        return result;
    }

    /**
     * Apply limit pushdown to buckets with an optional bucket processor (e.g., filtering).
     *
     * <p>This method processes buckets in order, applying the bucket processor first, then applying
     * limit pushdown if enabled. It stops early when the limit is reached.
     *
     * @param buckets buckets grouped by (partition, bucket)
     * @param bucketProcessor processor to apply to each bucket before limit pushdown
     * @return processed entries (filtered and limited if limit is enabled)
     */
    private List<ManifestEntry> applyLimitPushdownToBuckets(
            Map<Pair<BinaryRow, Integer>, List<ManifestEntry>> buckets,
            Function<List<ManifestEntry>, List<ManifestEntry>> bucketProcessor,
            boolean limitEnabled) {
        List<ManifestEntry> result = new ArrayList<>();
        long accumulatedRowCount = 0;

        for (List<ManifestEntry> bucketEntries : buckets.values()) {
            // Apply bucket processor (e.g., filtering)
            List<ManifestEntry> processed = bucketProcessor.apply(bucketEntries);

            if (limitEnabled) {
                // Apply limit pushdown if enabled
                if (accumulatedRowCount >= limit) {
                    // Already reached limit, stop processing remaining buckets
                    break;
                }

                long remainingLimit = limit - accumulatedRowCount;
                List<ManifestEntry> processedBucket =
                        applyLimitPushdownForBucket(processed, remainingLimit);
                if (processedBucket == null) {
                    // Can't safely push down limit for this bucket, include all processed entries
                    result.addAll(processed);
                } else {
                    result.addAll(processedBucket);
                    for (ManifestEntry entry : processedBucket) {
                        long fileRowCount = entry.file().rowCount();
                        accumulatedRowCount += fileRowCount;
                    }
                }
            } else {
                // No limit pushdown, just add processed entries
                result.addAll(processed);
            }
        }

        return result;
    }

    private List<ManifestEntry> doFilterWholeBucketByStats(List<ManifestEntry> entries) {
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
        if (entry instanceof FilteredManifestEntry) {
            return ((FilteredManifestEntry) entry).selected();
        }

        Predicate notEvolvedFilter =
                notEvolvedValueFilterMapping.computeIfAbsent(
                        entry.file().schemaId(),
                        id ->
                                // keepNewFieldFilter to handle add field
                                // for example, add field 'c', 'c > 3': old files can be filtered
                                fieldValueStatsConverters.filterUnsafeFilter(
                                        entry.file().schemaId(), valueFilter, true));
        if (notEvolvedFilter == null) {
            return true;
        }

        DataFileMeta file = entry.file();
        SimpleStatsEvolution.Result result =
                fieldValueStatsConverters
                        .getOrCreate(file.schemaId())
                        .evolution(file.valueStats(), file.rowCount(), file.valueStatsCols());
        return notEvolvedFilter.test(
                        file.rowCount(),
                        result.minValues(),
                        result.maxValues(),
                        result.nullCounts())
                && (!fileIndexReadEnabled
                        || filterByFileIndex(entry.file().embeddedIndex(), entry));
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

    /** Group manifest entries by (partition, bucket) while preserving order. */
    private Map<Pair<BinaryRow, Integer>, List<ManifestEntry>> groupByBucket(
            List<ManifestEntry> entries) {
        return entries.stream()
                .collect(
                        Collectors.groupingBy(
                                // we use LinkedHashMap to avoid disorder
                                file -> Pair.of(file.partition(), file.bucket()),
                                LinkedHashMap::new,
                                Collectors.toList()));
    }
}
