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
import org.apache.paimon.fs.FileIO;
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
        List<Pair<LinkedHashMap<String, String>, Path>> partitions =
                findPartitions(partitionFilter);
        List<Split> splits = new ArrayList<>();
        if (partitions.isEmpty()) {
            return splits;
        }

        FileIO fileIO = table.fileIO();
        // Establish the filesystem on the caller thread so listing workers reuse it under the
        // caller's security context instead of creating it lazily under a shared worker.
        fileIO.exists(new Path(table.location()));
        Function<Pair<LinkedHashMap<String, String>, Path>, List<Split>> lister =
                pair -> {
                    BinaryRow partitionRow = toPartitionRow(pair.getKey());
                    if (partitionFilter != null && !partitionFilter.test(partitionRow)) {
                        return Collections.emptyList();
                    }
                    try {
                        return createSplits(fileIO, pair.getValue(), partitionRow);
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
        Optional<Predicate> extracted = FormatTableScan.extractPartitionPredicate(partitionFilter);
        Map<String, String> prefix = leadingEqualityPrefix(extracted);
        Predicate catalogFilter = extracted.orElse(null);
        List<Partition> partitions = partitionManager.listPartitions(prefix, catalogFilter);
        if (partitions.isEmpty() && prefix.isEmpty() && catalogFilter == null) {
            warnIfFilesystemPartitionsExist();
        }
        return toSpecsAndPaths(partitions, coreOptions.formatTablePartitionOnlyValueInPath());
    }

    @Override
    List<PartitionEntry> listPartitionEntries() {
        List<Partition> partitions = partitionManager.listPartitions(Collections.emptyMap(), null);
        if (partitions.isEmpty()) {
            warnIfFilesystemPartitionsExist();
        }
        boolean onlyValueInPath = coreOptions.formatTablePartitionOnlyValueInPath();
        List<PartitionEntry> entries = new ArrayList<>(partitions.size());
        Set<Map<String, String>> seen = new HashSet<>(partitions.size());
        for (Partition partition : partitions) {
            if (!seen.add(partition.spec())) {
                continue;
            }
            entries.add(
                    new PartitionEntry(
                            toPartitionRow(normalizeSpec(partition.spec(), onlyValueInPath)),
                            partition.recordCount(),
                            partition.fileSizeInBytes(),
                            partition.fileCount(),
                            partition.lastFileCreationTime(),
                            partition.totalBuckets()));
        }
        return entries;
    }

    private List<Pair<LinkedHashMap<String, String>, Path>> toSpecsAndPaths(
            List<Partition> partitions, boolean onlyValueInPath) {
        List<Pair<LinkedHashMap<String, String>, Path>> result = new ArrayList<>(partitions.size());
        Path tablePath = new Path(table.location());
        // A duplicate catalog entry must not duplicate all records in that partition.
        Set<String> seenPartitionPaths = new HashSet<>(partitions.size());
        for (Partition partition : partitions) {
            LinkedHashMap<String, String> spec = normalizeSpec(partition.spec(), onlyValueInPath);
            String partitionPath =
                    PartitionPathUtils.generatePartitionPathUtil(spec, onlyValueInPath);
            if (seenPartitionPaths.add(partitionPath)) {
                result.add(Pair.of(spec, new Path(tablePath, partitionPath)));
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
                // A staging tree left behind by a committer is not unregistered data, so it
                // must not send the reader off to run MSCK REPAIR TABLE.
                if (status.isDir()
                        && !PartitionPathUtils.isHiddenName(status.getPath().getName())) {
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
