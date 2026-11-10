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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.InternalArray;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fileindex.FileIndexPredicate;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.manifest.FilteredManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.mergetree.dataevolution.ColumnFileGroups;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.DataEvolutionArray;
import org.apache.paimon.reader.DataEvolutionRow;
import org.apache.paimon.schema.KeyValueFieldsExtractor;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.stats.SimpleStatsEvolution;
import org.apache.paimon.stats.SimpleStatsEvolutions;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.SnapshotManager;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.paimon.CoreOptions.MergeEngine.AGGREGATE;
import static org.apache.paimon.CoreOptions.MergeEngine.PARTIAL_UPDATE;

/** {@link FileStoreScan} for {@link KeyValueFileStore}. */
public class KeyValueFileStoreScan extends AbstractFileStoreScan {

    private final SimpleStatsEvolutions fieldKeyStatsConverters;
    private final SimpleStatsEvolutions fieldValueStatsConverters;
    private final BucketSelectConverter bucketSelectConverter;
    private final boolean deletionVectorsEnabled;
    private final MergeEngine mergeEngine;
    private final ChangelogProducer changelogProducer;
    private final boolean fileIndexReadEnabled;
    private final boolean fileIndexReadIgnoreUnknownType;
    // Column files replace columns of their base file, so files are filtered per alignment group.
    private final boolean dataEvolutionEnabled;

    private Predicate keyFilter;
    private Predicate valueFilter;
    private boolean valueFilterForceEnabled = false;
    private boolean dropGroupStats = false;

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
            boolean fileIndexReadEnabled,
            boolean fileIndexReadIgnoreUnknownType,
            boolean dataEvolutionEnabled) {
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
        this.fileIndexReadIgnoreUnknownType = fileIndexReadIgnoreUnknownType;
        this.dataEvolutionEnabled = dataEvolutionEnabled;
    }

    @Override
    public FileStoreScan dropStats() {
        if (dataEvolutionEnabled) {
            // stats are needed by the group filter, they are dropped after it
            this.dropGroupStats = true;
            return this;
        }
        return super.dropStats();
    }

    @Override
    public FileStoreScan keepStats() {
        this.dropGroupStats = false;
        return super.keepStats();
    }

    @Override
    protected boolean countColumnFileRows() {
        return !dataEvolutionEnabled;
    }

    public KeyValueFileStoreScan withKeyFilter(Predicate predicate) {
        this.keyFilter = predicate;
        return this;
    }

    @Override
    public FileStoreScan withCompleteFilter(Predicate predicate) {
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
        if (dataEvolutionEnabled) {
            // Column files never change keys, so the key filter of a base file holds for its
            // group. Column files have no key stats and the value stats of a base file may be
            // stale, they are left to postFilterManifestEntries.
            return ColumnFileGroups.isColumnFile(entry.file()) || filterByKeyFilter(entry);
        }

        if (isValueFilterEnabled() && !filterByValueFilter(entry)) {
            return false;
        }

        return filterByKeyFilter(entry);
    }

    private boolean filterByKeyFilter(ManifestEntry entry) {
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
        if (dataEvolutionEnabled) {
            return entry.copyWithoutStats();
        }
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
            return predicate.evaluate(dataPredicate, fileIndexReadIgnoreUnknownType).remain();
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

    @Override
    protected boolean postFilterManifestEntriesEnabled() {
        return dataEvolutionEnabled || wholeBucketFilterEnabled() || limitPushdownEnabled();
    }

    private boolean wholeBucketFilterEnabled() {
        return valueFilter != null && scanMode == ScanMode.ALL;
    }

    @VisibleForTesting
    public boolean limitPushdownEnabled() {
        if (limit == null || limit <= 0) {
            return false;
        }

        return mergeEngine != PARTIAL_UPDATE
                && mergeEngine != AGGREGATE
                && !deletionVectorsEnabled
                && valueFilter == null
                && keyFilter == null;
    }

    @Override
    protected List<ManifestEntry> postFilterManifestEntries(List<ManifestEntry> files) {
        Map<Pair<BinaryRow, Integer>, List<ManifestEntry>> buckets = groupByBucket(files);
        List<ManifestEntry> result = new ArrayList<>();
        AtomicLong currentRowCount = new AtomicLong(0);
        for (List<ManifestEntry> entries : buckets.values()) {
            if (dataEvolutionEnabled) {
                if (hasColumnFiles(entries)) {
                    // no limit: the row count of a column file does not add rows
                    addWithoutGroupStats(result, filterAlignedGroups(entries));
                    continue;
                }
                if (isValueFilterEnabled()) {
                    // deferred by filterByStats
                    entries = filterWholeBucketPerFile(entries);
                }
            }

            boolean noOverlapping = noOverlapping(entries);
            if (wholeBucketFilterEnabled()) {
                entries =
                        noOverlapping
                                ? filterWholeBucketPerFile(entries)
                                : filterWholeBucketAllFiles(entries);
            }

            if (limitPushdownEnabled() && noOverlapping) {
                if (currentRowCount.get() >= limit) {
                    if (dataEvolutionEnabled) {
                        // break would never visit a later bucket. One that holds column files would
                        // be unreachable for every small limit, so only buckets without column
                        // files are skipped
                        continue;
                    }
                    break;
                }
                entries = applyLimitWhenNoOverlapping(entries, currentRowCount);
            }
            addWithoutGroupStats(result, entries);
        }
        return result;
    }

    private void addWithoutGroupStats(List<ManifestEntry> result, List<ManifestEntry> entries) {
        for (ManifestEntry entry : entries) {
            result.add(dropGroupStats ? entry.copyWithoutStats() : entry);
        }
    }

    private static boolean hasColumnFiles(List<ManifestEntry> entries) {
        for (ManifestEntry entry : entries) {
            if (ColumnFileGroups.isColumnFile(entry.file())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Filters the files of one bucket of a data evolution table by alignment group: a column file
     * and its base file are kept or dropped together. The value filter tests the group's stats
     * where each column takes the stats of the file it is read from.
     */
    private List<ManifestEntry> filterAlignedGroups(List<ManifestEntry> entries) {
        List<List<ManifestEntry>> groups = new ArrayList<>();
        List<ManifestEntry> bases = new ArrayList<>();
        for (List<ManifestEntry> group : groupByAlignment(entries)) {
            ManifestEntry base = baseOf(group);
            if (base == null) {
                // A snapshot always holds the base file of a column file, so with a key filter the
                // base file was dropped by filterByStats. An incremental scan only sees the column
                // files.
                if (scanMode != ScanMode.ALL || keyFilter == null) {
                    groups.add(group);
                }
            } else if (!isValueFilterEnabled() || filterGroupByValueFilter(group, base)) {
                groups.add(group);
                bases.add(base);
            }
        }

        if (!isValueFilterEnabled() && wholeBucketFilterEnabled()) {
            if (noOverlapping(bases)) {
                groups.removeIf(
                        group -> {
                            ManifestEntry base = baseOf(group);
                            return base != null && !filterGroupByValueFilter(group, base);
                        });
            } else if (!filterWholeBucketByGroups(groups)) {
                groups.clear();
            }
        }

        List<ManifestEntry> result = new ArrayList<>();
        for (List<ManifestEntry> group : groups) {
            result.addAll(group);
        }
        return result;
    }

    private boolean filterWholeBucketByGroups(List<List<ManifestEntry>> groups) {
        if (mergeEngine == PARTIAL_UPDATE || mergeEngine == AGGREGATE) {
            return true;
        }
        for (List<ManifestEntry> group : groups) {
            ManifestEntry base = baseOf(group);
            if (base == null || filterGroupByValueFilter(group, base)) {
                return true;
            }
        }
        return false;
    }

    private static List<List<ManifestEntry>> groupByAlignment(List<ManifestEntry> entries) {
        Map<Long, List<ManifestEntry>> aligned = new LinkedHashMap<>();
        List<List<ManifestEntry>> groups = new ArrayList<>();
        for (ManifestEntry entry : entries) {
            Long firstRowId = entry.file().firstRowId();
            if (firstRowId == null) {
                groups.add(Collections.singletonList(entry));
            } else {
                aligned.computeIfAbsent(firstRowId, id -> new ArrayList<>()).add(entry);
            }
        }
        groups.addAll(aligned.values());
        return groups;
    }

    @Nullable
    private static ManifestEntry baseOf(List<ManifestEntry> group) {
        for (ManifestEntry entry : group) {
            if (!entry.file().isPartialColumnFile()) {
                return entry;
            }
        }
        return null;
    }

    private boolean filterGroupByValueFilter(List<ManifestEntry> group, ManifestEntry base) {
        if (group.size() == 1) {
            return filterByValueFilter(base);
        }

        Predicate filter = valueFilter;
        for (ManifestEntry entry : group) {
            filter =
                    fieldValueStatsConverters.filterUnsafeFilter(
                            entry.file().schemaId(), filter, true);
            if (filter == null) {
                return true;
            }
        }

        List<DataField> tableFields = fieldValueStatsConverters.tableDataFields();
        int fieldCount = tableFields.size();
        int[] rowOffsets = new int[fieldCount];
        int[] fieldOffsets = new int[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            fieldOffsets[i] = i;
        }

        long rowCount = base.file().rowCount();
        InternalRow[] minValues = new InternalRow[group.size()];
        InternalRow[] maxValues = new InternalRow[group.size()];
        InternalArray[] nullCounts = new InternalArray[group.size()];
        int provider = 0;
        for (ManifestEntry entry : group) {
            DataFileMeta file = entry.file();
            int current = entry == base ? 0 : ++provider;
            SimpleStatsEvolution evolution =
                    fieldValueStatsConverters.getOrCreate(file.schemaId(), file.writeCols());
            SimpleStatsEvolution.Result stats =
                    evolution.evolution(file.valueStats(), rowCount, file.valueStatsCols());
            minValues[current] = stats.minValues();
            maxValues[current] = stats.maxValues();
            nullCounts[current] = stats.nullCounts();
            if (file.isPartialColumnFile()) {
                Set<Integer> fieldIds = new HashSet<>();
                for (DataField field : evolution.dataRowType().getFields()) {
                    fieldIds.add(field.id());
                }
                for (int i = 0; i < fieldCount; i++) {
                    if (fieldIds.contains(tableFields.get(i).id())) {
                        rowOffsets[i] = current;
                    }
                }
            }
        }

        DataEvolutionRow min = new DataEvolutionRow(group.size(), rowOffsets, fieldOffsets);
        min.setRows(minValues);
        DataEvolutionRow max = new DataEvolutionRow(group.size(), rowOffsets, fieldOffsets);
        max.setRows(maxValues);
        DataEvolutionArray nulls = new DataEvolutionArray(group.size(), rowOffsets, fieldOffsets);
        nulls.setRows(nullCounts);
        return filter.test(rowCount, min, max, nulls);
    }

    private List<ManifestEntry> applyLimitWhenNoOverlapping(
            List<ManifestEntry> entries, AtomicLong currentRowCount) {
        List<ManifestEntry> result = new ArrayList<>();
        for (ManifestEntry entry : entries) {
            boolean hasDeleteRows =
                    entry.file().deleteRowCount().map(count -> count > 0L).orElse(false);
            if (hasDeleteRows) {
                return entries;
            }
            result.add(entry);
            long fileRowCount = entry.file().rowCount();
            currentRowCount.addAndGet(fileRowCount);
            if (currentRowCount.get() >= limit) {
                break;
            }
        }
        return result;
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
