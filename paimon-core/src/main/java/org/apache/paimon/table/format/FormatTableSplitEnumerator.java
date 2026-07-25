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
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.csv.CsvOptions;
import org.apache.paimon.format.json.JsonOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.partition.PartitionPredicate.MultiplePartitionPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BinPacking;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.SemaphoredDelegatingExecutor;
import org.apache.paimon.utils.ThreadPoolUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;

import static org.apache.paimon.format.text.HadoopCompressionUtils.isCompressed;
import static org.apache.paimon.format.text.TextLineReader.isDefaultDelimiter;
import static org.apache.paimon.utils.InternalRowPartitionComputer.convertSpecToInternalRow;
import static org.apache.paimon.utils.PartitionPathUtils.searchPartSpecAndPaths;

/** Enumerates {@link FormatDataSplit}s for a {@link FormatTable}. */
final class FormatTableSplitEnumerator {

    private static final Logger LOG = LoggerFactory.getLogger(FormatTableSplitEnumerator.class);

    private static final int LIST_POOL_MAX_THREADS = 1000;
    private static final ThreadPoolExecutor LIST_POOL =
            ThreadPoolUtils.createCachedThreadPool(
                    LIST_POOL_MAX_THREADS, "FORMAT-TABLE-LIST-THREAD-POOL");

    private final FormatTable table;
    private final CoreOptions coreOptions;
    @Nullable private final FormatTablePartitionManager partitionManager;
    private final long targetSplitSize;
    private final long openFileCost;
    private final FormatTable.Format format;

    FormatTableSplitEnumerator(
            FormatTable table,
            CoreOptions coreOptions,
            @Nullable FormatTablePartitionManager partitionManager) {
        this.table = table;
        this.coreOptions = coreOptions;
        this.partitionManager = partitionManager;
        this.targetSplitSize = coreOptions.splitTargetSize();
        this.openFileCost = coreOptions.splitOpenFileCost();
        this.format = table.format();
    }

    List<Split> enumerate(@Nullable PartitionPredicate partitionFilter) throws IOException {
        if (table.partitionKeys().isEmpty()) {
            return createSplits(table.fileIO(), new Path(table.location()), null);
        }
        if (partitionManager != null) {
            return enumerateCatalogPartitions(partitionManager, partitionFilter);
        }
        return enumerateFilesystemPartitions(partitionFilter);
    }

    List<Pair<LinkedHashMap<String, String>, Path>> findPartitions(
            @Nullable PartitionPredicate partitionFilter) {
        if (partitionManager != null) {
            return findCatalogPartitions(partitionManager, partitionFilter);
        }
        return findFilesystemPartitions(partitionFilter);
    }

    BinaryRow toPartitionRow(LinkedHashMap<String, String> partitionSpec) {
        RowType partitionType = table.partitionType();
        GenericRow row =
                convertSpecToInternalRow(partitionSpec, partitionType, table.defaultPartName());
        return new InternalRowSerializer(partitionType).toBinaryRow(row);
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

    private List<Split> enumerateFilesystemPartitions(@Nullable PartitionPredicate partitionFilter)
            throws IOException {
        List<Split> splits = new ArrayList<>();
        FileIO fileIO = table.fileIO();
        for (Pair<LinkedHashMap<String, String>, Path> pair :
                findFilesystemPartitions(partitionFilter)) {
            BinaryRow partitionRow = toPartitionRow(pair.getKey());
            if (partitionFilter == null || partitionFilter.test(partitionRow)) {
                splits.addAll(createSplits(fileIO, pair.getValue(), partitionRow));
            }
        }
        return splits;
    }

    private List<Split> enumerateCatalogPartitions(
            FormatTablePartitionManager partitionManager,
            @Nullable PartitionPredicate partitionFilter) {
        List<Pair<LinkedHashMap<String, String>, Path>> partitions =
                findCatalogPartitions(partitionManager, partitionFilter);
        List<Split> splits = new ArrayList<>();
        if (partitions.isEmpty()) {
            return splits;
        }

        FileIO fileIO = table.fileIO();
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
        int parallelism = Math.max(1, coreOptions.formatTableScanListParallelism());
        ExecutorService executor =
                parallelism >= LIST_POOL_MAX_THREADS
                        ? LIST_POOL
                        : new SemaphoredDelegatingExecutor(LIST_POOL, parallelism, false);
        ThreadPoolUtils.randomlyExecuteSequentialReturn(executor, lister, partitions)
                .forEachRemaining(splits::add);
        return splits;
    }

    private List<Pair<LinkedHashMap<String, String>, Path>> findCatalogPartitions(
            FormatTablePartitionManager partitionManager,
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

    private List<Pair<LinkedHashMap<String, String>, Path>> findFilesystemPartitions(
            @Nullable PartitionPredicate partitionFilter) {
        LOG.debug(
                "Find partitions for format table {}, partition filter: {}",
                table.name(),
                partitionFilter);
        boolean onlyValueInPath = coreOptions.formatTablePartitionOnlyValueInPath();
        if (partitionFilter instanceof MultiplePartitionPredicate) {
            Set<BinaryRow> partitions = ((MultiplePartitionPredicate) partitionFilter).partitions();
            return FormatTableScan.generatePartitions(
                    table.partitionKeys(),
                    table.partitionType(),
                    table.defaultPartName(),
                    new Path(table.location()),
                    partitions,
                    onlyValueInPath);
        }

        Optional<Predicate> predicate = FormatTableScan.extractPartitionPredicate(partitionFilter);
        LOG.debug(
                "Extracted predicate for format table {} partition pruning: {}",
                table.name(),
                predicate.orElse(null));

        Pair<Path, Integer> scanPathAndLevel =
                FormatTableScan.computeScanPathAndLevel(
                        new Path(table.location()),
                        table.partitionKeys(),
                        predicate,
                        table.partitionType(),
                        onlyValueInPath);
        return searchPartSpecAndPaths(
                table.fileIO(),
                scanPathAndLevel.getLeft(),
                scanPathAndLevel.getRight(),
                table.partitionKeys(),
                onlyValueInPath,
                predicate.orElse(null),
                table.partitionType(),
                table.defaultPartName());
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

    private IllegalStateException corruptPartitionSpec(@Nullable Map<String, String> spec) {
        return new IllegalStateException(
                String.format(
                        "Catalog returned corrupt partition metadata %s for format table %s; "
                                + "expected exactly the partition keys %s with values usable as "
                                + "path components.",
                        spec, table.fullName(), table.partitionKeys()));
    }

    private Map<String, String> leadingEqualityPrefix(Optional<Predicate> predicate) {
        if (!predicate.isPresent()) {
            return Collections.emptyMap();
        }
        return FormatTableScan.extractLeadingEqualityPartitionSpecWhenOnlyAnd(
                table.partitionKeys(), predicate.get(), table.partitionType());
    }

    private List<Split> createSplits(FileIO fileIO, Path path, @Nullable BinaryRow partition)
            throws IOException {
        List<FormatDataSplit.FileMeta> segments = new ArrayList<>();
        FileStatus[] files = fileIO.listFiles(path, true);
        Arrays.sort(files, Comparator.comparing(file -> file.getPath().toString()));
        for (FileStatus file : files) {
            if (FormatTableScan.isDataFileName(file.getPath().getName())) {
                segments.addAll(toSegments(file));
            }
        }

        List<Split> splits = new ArrayList<>();
        for (List<FormatDataSplit.FileMeta> bin :
                BinPacking.packForOrdered(
                        segments,
                        file -> Math.max(file.readSize(), openFileCost),
                        targetSplitSize)) {
            splits.add(new FormatDataSplit(bin, partition));
        }
        return splits;
    }

    private List<FormatDataSplit.FileMeta> toSegments(FileStatus file) {
        if (!preferToSplitFile(file)) {
            return Collections.singletonList(
                    new FormatDataSplit.FileMeta(file.getPath(), file.getLen()));
        }
        List<FormatDataSplit.FileMeta> segments = new ArrayList<>();
        long remainingBytes = file.getLen();
        long currentStart = 0;

        while (remainingBytes > 0) {
            long splitSize = Math.min(targetSplitSize, remainingBytes);
            segments.add(
                    new FormatDataSplit.FileMeta(
                            file.getPath(), file.getLen(), currentStart, splitSize));
            currentStart += splitSize;
            remainingBytes -= splitSize;
        }
        return segments;
    }

    private boolean preferToSplitFile(FileStatus file) {
        if (file.getLen() <= targetSplitSize) {
            return false;
        }

        Options options = coreOptions.toConfiguration();
        switch (format) {
            case CSV:
                return !isCompressed(file.getPath())
                        && isDefaultDelimiter(options.get(CsvOptions.LINE_DELIMITER));
            case JSON:
                return !isCompressed(file.getPath())
                        && isDefaultDelimiter(options.get(JsonOptions.LINE_DELIMITER));
            default:
                return false;
        }
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
