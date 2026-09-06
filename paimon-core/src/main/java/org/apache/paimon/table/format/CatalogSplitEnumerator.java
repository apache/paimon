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

package org.apache.paimon.table.format;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.SemaphoredDelegatingExecutor;
import org.apache.paimon.utils.ThreadPoolUtils;
import org.apache.paimon.utils.ThreadUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/** A {@link SplitEnumerator} whose partitions are managed by the catalog. */
final class CatalogSplitEnumerator extends SplitEnumerator {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogSplitEnumerator.class);

    private static final int LIST_POOL_MAX_THREADS = 1000;

    // Cached pool bounded at 1000 threads that reuses idle workers; CallerRunsPolicy lists on the
    // caller for back pressure once the cap is hit.
    private static final ThreadPoolExecutor LIST_POOL =
            new ThreadPoolExecutor(
                    0,
                    LIST_POOL_MAX_THREADS,
                    1,
                    TimeUnit.MINUTES,
                    new SynchronousQueue<>(),
                    ThreadUtils.newDaemonThreadFactory("FORMAT-TABLE-LIST-THREAD-POOL"),
                    new ThreadPoolExecutor.CallerRunsPolicy());

    private final FormatTablePartitionManager partitionManager;

    CatalogSplitEnumerator(
            FormatTable table,
            CoreOptions coreOptions,
            FormatTablePartitionManager partitionManager) {
        super(table, coreOptions);
        this.partitionManager = partitionManager;
    }

    @Override
    List<Split> enumeratePartitions(@Nullable PartitionPredicate partitionFilter)
            throws IOException {
        CatalogPartitionListing listing = findCatalogPartitions(partitionFilter);
        return enumeratePartitions(filterPartitions(listing.partitionPaths, partitionFilter));
    }

    @Override
    ScanPlan plan(@Nullable PartitionPredicate partitionFilter) throws IOException {
        if (table.partitionKeys().isEmpty()) {
            return super.plan(partitionFilter);
        }
        CatalogPartitionListing listing = findCatalogPartitions(partitionFilter);
        List<Pair<LinkedHashMap<String, String>, Path>> selected =
                filterPartitions(listing.partitionPaths, partitionFilter);
        List<PartitionEntry> entries = toPartitionEntries(listing.partitions, partitionFilter);
        return new ScanPlan(enumeratePartitions(selected), rowCount(entries));
    }

    private List<Split> enumeratePartitions(
            List<Pair<LinkedHashMap<String, String>, Path>> partitions) throws IOException {
        List<Split> splits = new ArrayList<>();
        if (partitions.isEmpty()) {
            return splits;
        }

        FormatTableFileIOResolver fileIOResolver = new FormatTableFileIOResolver(table);
        boolean tableFileIOPrepared = false;
        for (Pair<LinkedHashMap<String, String>, Path> partition : partitions) {
            boolean useCatalogContextFileIO =
                    fileIOResolver.useCatalogContextFileIO(partition.getValue());
            if (useCatalogContextFileIO) {
                fileIOResolver.prepare(partition.getValue(), true);
            } else if (!tableFileIOPrepared) {
                fileIOResolver.prepare(partition.getValue(), false);
                tableFileIOPrepared = true;
            }
        }
        Function<Pair<LinkedHashMap<String, String>, Path>, List<Split>> lister =
                pair -> {
                    BinaryRow partitionRow = toPartitionRow(pair.getKey());
                    try {
                        boolean useCatalogContextFileIO =
                                fileIOResolver.useCatalogContextFileIO(pair.getValue());
                        return createSplits(
                                fileIOResolver.fileIO(useCatalogContextFileIO),
                                pair.getValue(),
                                partitionRow,
                                useCatalogContextFileIO);
                    } catch (FileNotFoundException e) {
                        warnMissingPartition(pair.getKey(), pair.getValue());
                        return Collections.emptyList();
                    } catch (IOException e) {
                        throw new RuntimeException(
                                "Failed to list files for partition " + pair.getValue(), e);
                    }
                };
        int parallelism =
                Math.min(
                        LIST_POOL_MAX_THREADS,
                        Math.max(1, coreOptions.formatTableScanListParallelism()));
        ExecutorService executor = new SemaphoredDelegatingExecutor(LIST_POOL, parallelism, false);
        ThreadPoolUtils.randomlyExecuteSequentialReturn(executor, lister, partitions)
                .forEachRemaining(splits::add);
        return splits;
    }

    @Override
    List<Pair<LinkedHashMap<String, String>, Path>> findPartitions(
            @Nullable PartitionPredicate partitionFilter) {
        CatalogPartitionListing listing = findCatalogPartitions(partitionFilter);
        return filterPartitions(listing.partitionPaths, partitionFilter);
    }

    private CatalogPartitionListing findCatalogPartitions(
            @Nullable PartitionPredicate partitionFilter) {
        Optional<Predicate> extracted = FormatTableScan.extractPartitionPredicate(partitionFilter);
        Map<String, String> prefix = leadingEqualityPrefix(extracted);
        Predicate catalogFilter = extracted.orElse(null);
        List<Partition> partitions = partitionManager.listPartitions(prefix, catalogFilter);
        if (partitions.isEmpty() && prefix.isEmpty() && catalogFilter == null) {
            warnIfFilesystemPartitionsExist();
        }
        List<Pair<LinkedHashMap<String, String>, Path>> partitionPaths =
                toSpecsAndPaths(partitions, coreOptions.formatTablePartitionOnlyValueInPath());
        return new CatalogPartitionListing(partitions, partitionPaths);
    }

    @Override
    List<PartitionEntry> listPartitionEntries() {
        return listPartitionEntries(null);
    }

    @Override
    List<PartitionEntry> listPartitionEntries(@Nullable PartitionPredicate partitionFilter) {
        return toPartitionEntries(
                findCatalogPartitions(partitionFilter).partitions, partitionFilter);
    }

    private static final class CatalogPartitionListing {

        private final List<Partition> partitions;
        private final List<Pair<LinkedHashMap<String, String>, Path>> partitionPaths;

        private CatalogPartitionListing(
                List<Partition> partitions,
                List<Pair<LinkedHashMap<String, String>, Path>> partitionPaths) {
            this.partitions = partitions;
            this.partitionPaths = partitionPaths;
        }
    }

    private List<Pair<LinkedHashMap<String, String>, Path>> filterPartitions(
            List<Pair<LinkedHashMap<String, String>, Path>> partitions,
            @Nullable PartitionPredicate partitionFilter) {
        if (partitionFilter == null) {
            return partitions;
        }
        List<Pair<LinkedHashMap<String, String>, Path>> selected = new ArrayList<>();
        for (Pair<LinkedHashMap<String, String>, Path> partition : partitions) {
            if (partitionFilter.test(toPartitionRow(partition.getKey()))) {
                selected.add(partition);
            }
        }
        return selected;
    }

    private List<PartitionEntry> toPartitionEntries(
            List<Partition> partitions, @Nullable PartitionPredicate partitionFilter) {
        boolean onlyValueInPath = coreOptions.formatTablePartitionOnlyValueInPath();
        List<PartitionEntry> entries = new ArrayList<>(partitions.size());
        Set<Map<String, String>> seen = new HashSet<>(partitions.size());
        for (Partition partition : partitions) {
            if (!seen.add(partition.spec())) {
                continue;
            }
            BinaryRow partitionRow =
                    toPartitionRow(normalizeSpec(partition.spec(), onlyValueInPath));
            if (partitionFilter != null && !partitionFilter.test(partitionRow)) {
                continue;
            }
            entries.add(
                    new PartitionEntry(
                            partitionRow,
                            partition.recordCount(),
                            partition.fileSizeInBytes(),
                            partition.fileCount(),
                            partition.lastFileCreationTime(),
                            partition.totalBuckets()));
        }
        return entries;
    }

    private OptionalLong rowCount(List<PartitionEntry> entries) {
        long rowCount = 0L;
        for (PartitionEntry entry : entries) {
            // At this scan-estimation boundary, only positive catalog counts are trustworthy.
            // Catalogs such as HMS overload non-positive values for unavailable statistics, so an
            // entry carrying zero cannot safely prove that its partition is empty. An empty entry
            // list still reaches the exact structural result of zero below.
            if (entry.recordCount() <= 0) {
                return OptionalLong.empty();
            }
            try {
                rowCount = Math.addExact(rowCount, entry.recordCount());
            } catch (ArithmeticException e) {
                return OptionalLong.empty();
            }
        }
        return OptionalLong.of(rowCount);
    }

    private List<Pair<LinkedHashMap<String, String>, Path>> toSpecsAndPaths(
            List<Partition> partitions, boolean onlyValueInPath) {
        if (partitions.stream()
                .noneMatch(
                        partition ->
                                FormatTablePartitionPathResolver.customLocation(partition)
                                        != null)) {
            return toDefaultSpecsAndPaths(partitions, onlyValueInPath);
        }
        List<Pair<LinkedHashMap<String, String>, Path>> result = new ArrayList<>(partitions.size());
        Path tablePath = new Path(table.location());
        FormatTablePartitionPathResolver pathResolver =
                new FormatTablePartitionPathResolver(
                        tablePath, table.fullName(), onlyValueInPath, table.catalogContext());
        for (Partition partition : partitions) {
            LinkedHashMap<String, String> spec = normalizeSpec(partition.spec(), onlyValueInPath);
            Path partitionPath =
                    pathResolver.resolve(
                            spec, FormatTablePartitionPathResolver.customLocation(partition));
            if (pathResolver.validateAndRecord(spec, partitionPath)) {
                result.add(Pair.of(spec, partitionPath));
            }
        }
        return result;
    }

    private List<Pair<LinkedHashMap<String, String>, Path>> toDefaultSpecsAndPaths(
            List<Partition> partitions, boolean onlyValueInPath) {
        List<Pair<LinkedHashMap<String, String>, Path>> result = new ArrayList<>(partitions.size());
        Set<Map<String, String>> seen = new HashSet<>(partitions.size());
        Path tablePath = new Path(table.location());
        for (Partition partition : partitions) {
            LinkedHashMap<String, String> spec = normalizeSpec(partition.spec(), onlyValueInPath);
            if (seen.add(spec)) {
                result.add(
                        Pair.of(
                                spec,
                                new Path(
                                        tablePath,
                                        PartitionPathUtils.generatePartitionPathUtil(
                                                spec, onlyValueInPath))));
            }
        }
        return result;
    }

    LinkedHashMap<String, String> normalizeSpec(
            @Nullable Map<String, String> spec, boolean onlyValueInPath) {
        List<String> partitionKeys = table.partitionKeys();
        if (spec == null
                || spec.size() != partitionKeys.size()
                || !spec.keySet().containsAll(partitionKeys)) {
            throw corruptPartitionSpec(spec);
        }
        LinkedHashMap<String, String> normalized = new LinkedHashMap<>();
        for (String partitionKey : partitionKeys) {
            String value = spec.get(partitionKey);
            // In a value-only layout, "." and ".." would resolve outside the table.
            try {
                PartitionPathUtils.validatePartitionValueForPath(value, onlyValueInPath);
            } catch (IllegalArgumentException e) {
                throw corruptPartitionSpec(spec);
            }
            normalized.put(partitionKey, value);
        }
        return normalized;
    }

    void warnIfFilesystemPartitionsExist() {
        try {
            for (FileStatus status : table.fileIO().listStatus(new Path(table.location()))) {
                if (status.isDir() && !status.getPath().getName().startsWith(".")) {
                    LOG.warn(
                            "Format table {} has no partitions registered in the catalog "
                                    + "but its location {} contains directories. Data written "
                                    + "before enabling catalog-managed partitions (or by clients "
                                    + "that do not register partitions) is invisible until the "
                                    + "partition metadata is synced, e.g. with MSCK REPAIR TABLE.",
                            table.fullName(),
                            table.location());
                    return;
                }
            }
        } catch (IOException ignored) {
            // Best-effort hint only; never fail or slow down the scan because of it.
        }
    }

    private Map<String, String> leadingEqualityPrefix(Optional<Predicate> predicate) {
        if (!predicate.isPresent()) {
            return Collections.emptyMap();
        }
        return FormatTableScan.extractLeadingEqualityPartitionSpecWhenOnlyAnd(
                table.partitionKeys(), predicate.get(), table.partitionType());
    }

    private IllegalStateException corruptPartitionSpec(@Nullable Map<String, String> spec) {
        return new IllegalStateException(
                String.format(
                        "Catalog returned corrupt partition metadata %s for format table %s; "
                                + "expected exactly the partition keys %s with values usable as "
                                + "path components.",
                        spec, table.fullName(), table.partitionKeys()));
    }

    private void warnMissingPartition(LinkedHashMap<String, String> spec, Path path) {
        LOG.warn(
                "Partition '{}' of format table {} is registered in the catalog but its directory "
                        + "'{}' does not exist; treating the partition as empty. If the directory "
                        + "was removed on purpose, drop the partition or repair the metadata, e.g. "
                        + "with MSCK REPAIR TABLE.",
                PartitionPathUtils.generatePartitionName(spec, false),
                table.fullName(),
                path);
    }
}
