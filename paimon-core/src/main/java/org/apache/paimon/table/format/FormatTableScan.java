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
import org.apache.paimon.casting.CastExecutor;
import org.apache.paimon.casting.CastExecutors;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.serializer.InternalRowSerializer;
import org.apache.paimon.format.csv.CsvOptions;
import org.apache.paimon.format.json.JsonOptions;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.Partition;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.partition.PartitionPredicate.AndPartitionPredicate;
import org.apache.paimon.partition.PartitionPredicate.DefaultPartitionPredicate;
import org.apache.paimon.partition.PartitionPredicate.MultiplePartitionPredicate;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.FieldRef;
import org.apache.paimon.predicate.LeafFunction;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.VarCharType;
import org.apache.paimon.utils.BinPacking;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.ManifestReadThreadPool;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PartitionPathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static org.apache.paimon.format.text.HadoopCompressionUtils.isCompressed;
import static org.apache.paimon.format.text.TextLineReader.isDefaultDelimiter;
import static org.apache.paimon.utils.InternalRowPartitionComputer.convertSpecToInternalRow;
import static org.apache.paimon.utils.PartitionPathUtils.searchPartSpecAndPaths;

/** {@link TableScan} for {@link FormatTable}. */
public class FormatTableScan implements InnerTableScan {

    private static final Logger LOG = LoggerFactory.getLogger(FormatTableScan.class);

    final FormatTable table;
    final CoreOptions coreOptions;
    @Nullable private PartitionPredicate partitionFilter;
    @Nullable private final FormatTablePartitionManager partitionManager;
    @Nullable private final Integer limit;
    private final long targetSplitSize;
    private final long openFileCost;
    private final FormatTable.Format format;

    public FormatTableScan(
            FormatTable table,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Integer limit) {
        this.table = table;
        this.coreOptions = new CoreOptions(table.options());
        this.partitionFilter = partitionFilter;
        this.limit = limit;
        this.partitionManager = table.partitionManager();
        this.targetSplitSize = coreOptions.splitTargetSize();
        this.openFileCost = coreOptions.splitOpenFileCost();
        this.format = table.format();
    }

    @Override
    public InnerTableScan withPartitionFilter(PartitionPredicate partitionPredicate) {
        this.partitionFilter = partitionPredicate;
        return this;
    }

    @Override
    public Plan plan() {
        return new FormatTableScanPlan();
    }

    @Override
    public List<PartitionEntry> listPartitionEntries() {
        if (partitionManager != null) {
            List<Partition> partitions =
                    partitionManager.listPartitions(Collections.emptyMap(), null);
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
        List<Pair<LinkedHashMap<String, String>, Path>> partition2Paths =
                searchPartSpecAndPaths(
                        table.fileIO(),
                        new Path(table.location()),
                        table.partitionKeys().size(),
                        table.partitionKeys(),
                        coreOptions.formatTablePartitionOnlyValueInPath(),
                        null,
                        table.partitionType(),
                        table.defaultPartName());
        List<PartitionEntry> partitionEntries = new ArrayList<>();
        for (Pair<LinkedHashMap<String, String>, Path> partition2Path : partition2Paths) {
            BinaryRow row = toPartitionRow(partition2Path.getKey());
            partitionEntries.add(new PartitionEntry(row, -1L, -1L, -1L, -1L, -1));
        }
        return partitionEntries;
    }

    @Override
    public InnerTableScan withFilter(Predicate predicate) {
        throw new UnsupportedOperationException("Filter is not supported for FormatTable.");
    }

    public static boolean isDataFileName(String fileName) {
        return fileName != null && !fileName.startsWith(".") && !fileName.startsWith("_");
    }

    BinaryRow toPartitionRow(LinkedHashMap<String, String> partitionSpec) {
        RowType partitionType = table.partitionType();
        GenericRow row =
                convertSpecToInternalRow(partitionSpec, partitionType, table.defaultPartName());
        return new InternalRowSerializer(partitionType).toBinaryRow(row);
    }

    private class FormatTableScanPlan implements Plan {
        @Override
        public List<Split> splits() {
            List<Split> splits = new ArrayList<>();
            try {
                FileIO fileIO = table.fileIO();
                if (!table.partitionKeys().isEmpty()) {
                    List<Pair<LinkedHashMap<String, String>, Path>> partitions = new ArrayList<>();
                    for (Pair<LinkedHashMap<String, String>, Path> pair : findPartitions()) {
                        // Filter partitions in memory before any file listing.
                        if (partitionFilter == null
                                || partitionFilter.test(toPartitionRow(pair.getKey()))) {
                            partitions.add(pair);
                        }
                    }
                    if (partitionManager != null) {
                        // Internal (catalog-managed) table: list partition files in parallel.
                        splits.addAll(listPartitionFilesInParallel(fileIO, partitions));
                    } else {
                        // External (filesystem-discovered) table: list serially; a missing
                        // partition
                        // directory is a hard error (rethrown to fail the whole scan).
                        for (Pair<LinkedHashMap<String, String>, Path> pair : partitions) {
                            splits.addAll(
                                    createSplits(
                                            fileIO,
                                            pair.getValue(),
                                            toPartitionRow(pair.getKey())));
                        }
                    }
                } else {
                    splits.addAll(createSplits(fileIO, new Path(table.location()), null));
                }
                // Keep all splits for a positive limit because FormatDataSplit has no row count.
                if (limit != null && limit <= 0) {
                    return new ArrayList<>();
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to scan files", e);
            }
            return splits;
        }
    }

    /**
     * Lists partition files for an internal (catalog-managed) table in parallel with a bounded,
     * order-preserving thread pool. A registered partition without a directory reads as empty
     * (matching Hive); any other listing failure fails the whole scan.
     */
    private List<Split> listPartitionFilesInParallel(
            FileIO fileIO, List<Pair<LinkedHashMap<String, String>, Path>> partitions) {
        List<Split> splits = new ArrayList<>();
        if (partitions.isEmpty()) {
            return splits;
        }
        Function<Pair<LinkedHashMap<String, String>, Path>, List<Split>> lister =
                pair -> {
                    try {
                        return createSplits(fileIO, pair.getValue(), toPartitionRow(pair.getKey()));
                    } catch (FileNotFoundException e) {
                        warnMissingPartition(pair.getKey(), pair.getValue());
                        return Collections.emptyList();
                    } catch (IOException e) {
                        // Fail the whole scan on any real listing error; never truncate.
                        throw new RuntimeException(
                                "Failed to list files for partition " + pair.getValue(), e);
                    }
                };
        ManifestReadThreadPool.randomlyExecuteSequentialReturn(
                        lister, partitions, coreOptions.formatTableScanListParallelism())
                .forEachRemaining(splits::add);
        return splits;
    }

    private void warnMissingPartition(LinkedHashMap<String, String> spec, Path path) {
        // A registered partition without a directory reads as empty, matching Hive (e.g. ADD
        // PARTITION before the first INSERT). Warn so a directory removed behind the catalog's back
        // stays discoverable.
        LOG.warn(
                "Partition '{}' of format table {} is registered in the catalog but its directory "
                        + "'{}' does not exist; treating the partition as empty. If the directory "
                        + "was removed on purpose, drop the partition or repair the metadata, e.g. "
                        + "with MSCK REPAIR TABLE.",
                PartitionPathUtils.generatePartitionName(spec, false),
                table.fullName(),
                path);
    }

    List<Pair<LinkedHashMap<String, String>, Path>> findPartitions() {
        if (partitionManager != null) {
            Optional<Predicate> extracted = extractPartitionPredicate(partitionFilter);
            Map<String, String> prefix = leadingEqualityPrefix(extracted);
            // The whole predicate goes to the manager as a pushdown hint, together with the
            // prefix; the catalog may return a superset.
            Predicate catalogFilter = extracted.orElse(null);
            List<Partition> partitions = partitionManager.listPartitions(prefix, catalogFilter);
            if (partitions.isEmpty() && prefix.isEmpty() && catalogFilter == null) {
                warnIfFilesystemPartitionsExist();
            }
            // The prefix and filter are coarse pre-filters; the full predicate is applied per
            // partition in the plan, as it is for filesystem discovery.
            return toSpecsAndPaths(partitions, coreOptions.formatTablePartitionOnlyValueInPath());
        }
        LOG.debug(
                "Find partitions for format table {}, partition filter: {}",
                table.name(),
                partitionFilter);
        boolean onlyValueInPath = coreOptions.formatTablePartitionOnlyValueInPath();
        if (partitionFilter instanceof MultiplePartitionPredicate) {
            // generate partitions directly
            Set<BinaryRow> partitions = ((MultiplePartitionPredicate) partitionFilter).partitions();
            return generatePartitions(
                    table.partitionKeys(),
                    table.partitionType(),
                    table.defaultPartName(),
                    new Path(table.location()),
                    partitions,
                    onlyValueInPath);
        } else {
            // search paths with partition filter optimization
            // This will prune partition directories early during traversal,
            // which is especially important for cloud storage like OSS/S3
            Optional<Predicate> predicate = extractPartitionPredicate(partitionFilter);
            LOG.debug(
                    "Extracted predicate for format table {} partition pruning: {}",
                    table.name(),
                    predicate.orElse(null));

            Pair<Path, Integer> scanPathAndLevel =
                    computeScanPathAndLevel(
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
    }

    /**
     * Turn the partitions a catalog reports into the specs and directories to read. Catalog
     * metadata is not trusted for path construction: a spec that cannot form a partition directory
     * of this table is rejected rather than resolved to some other directory.
     */
    private List<Pair<LinkedHashMap<String, String>, Path>> toSpecsAndPaths(
            List<Partition> partitions, boolean onlyValueInPath) {
        List<Pair<LinkedHashMap<String, String>, Path>> result = new ArrayList<>(partitions.size());
        Path tablePath = new Path(table.location());
        // Do not trust the catalog to be duplicate-free: a repeated spec would double every split
        // of that partition and silently duplicate query results.
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

    /** Order a catalog spec by the table partition keys and check it can form a directory. */
    private LinkedHashMap<String, String> normalizeSpec(
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
            // In a value-only layout, '.' and '..' are complete path components and would resolve
            // to a directory outside the table.
            try {
                PartitionPathUtils.validatePartitionValueForPath(value, onlyValueInPath);
            } catch (IllegalArgumentException e) {
                throw corruptPartitionSpec(spec);
            }
            normalized.put(partitionKey, value);
        }
        return normalized;
    }

    private IllegalStateException corruptPartitionSpec(@Nullable Map<String, String> spec) {
        return new IllegalStateException(
                String.format(
                        "Catalog returned corrupt partition metadata %s for format table %s; "
                                + "expected exactly the partition keys %s with values usable as "
                                + "path components.",
                        spec, table.fullName(), table.partitionKeys()));
    }

    /**
     * The leading equality prefix of the partition predicate, in partition-key order, pushed down
     * to the partition catalog. Empty when there is no predicate or it does not start with
     * equalities on the leading partition keys.
     */
    private Map<String, String> leadingEqualityPrefix(Optional<Predicate> predicate) {
        if (!predicate.isPresent()) {
            return Collections.emptyMap();
        }
        return extractLeadingEqualityPartitionSpecWhenOnlyAnd(
                table.partitionKeys(), predicate.get(), table.partitionType());
    }

    /**
     * Warn when the catalog knows no partitions but the table directory contains subdirectories:
     * typically a table that predates catalog-managed partitions (or was written by a client that
     * does not register partitions) and needs a metadata sync before its data becomes visible.
     */
    private void warnIfFilesystemPartitionsExist() {
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

    protected static List<Pair<LinkedHashMap<String, String>, Path>> generatePartitions(
            List<String> partitionKeys,
            RowType partitionType,
            String defaultPartName,
            Path tablePath,
            Set<BinaryRow> partitions,
            boolean onlyValueInPath) {
        InternalRowPartitionComputer partitionComputer =
                new InternalRowPartitionComputer(
                        defaultPartName,
                        partitionType,
                        partitionKeys.toArray(new String[0]),
                        false);
        List<Pair<LinkedHashMap<String, String>, Path>> result = new ArrayList<>();
        for (BinaryRow part : partitions) {
            LinkedHashMap<String, String> partSpec = partitionComputer.generatePartValues(part);

            String path =
                    onlyValueInPath
                            ? PartitionPathUtils.generatePartitionPathUtil(partSpec, true)
                            : PartitionPathUtils.generatePartitionPath(partSpec);
            result.add(Pair.of(partSpec, new Path(tablePath, path)));
        }
        return result;
    }

    /**
     * Extracts the underlying {@link Predicate} used for partition-directory pruning from a {@link
     * PartitionPredicate}. Unlike data-table scans, which prune purely via {@link
     * PartitionPredicate#test} on partitions read from the manifest, a format table has no manifest
     * and must derive a {@link Predicate} to compute the scan-path prefix and per-directory filters
     * while listing. {@link AndPartitionPredicate} is unwrapped recursively. Returns empty when the
     * predicate cannot be expressed as a single {@link Predicate} (e.g. {@link
     * MultiplePartitionPredicate}), in which case the caller falls back to listing without pruning.
     */
    static Optional<Predicate> extractPartitionPredicate(
            @Nullable PartitionPredicate partitionFilter) {
        if (partitionFilter instanceof DefaultPartitionPredicate) {
            return Optional.of(((DefaultPartitionPredicate) partitionFilter).predicate());
        } else if (partitionFilter instanceof AndPartitionPredicate) {
            List<Predicate> predicates = new ArrayList<>();
            for (PartitionPredicate child :
                    ((AndPartitionPredicate) partitionFilter).predicates()) {
                Optional<Predicate> childPredicate = extractPartitionPredicate(child);
                childPredicate.ifPresent(predicates::add);
                // Skip children that can't be expressed as Predicate (e.g. Multiple);
                // they are still applied by partitionFilter.test() in plan().
            }
            return predicates.isEmpty()
                    ? Optional.empty()
                    : Optional.of(PredicateBuilder.and(predicates));
        }
        return Optional.empty();
    }

    protected static Pair<Path, Integer> computeScanPathAndLevel(
            Path tableLocation,
            List<String> partitionKeys,
            Optional<Predicate> predicate,
            RowType partitionType,
            boolean onlyValueInPath) {
        Path scanPath = tableLocation;
        int level = partitionKeys.size();
        if (!partitionKeys.isEmpty()) {
            if (predicate.isPresent()) {
                Map<String, String> equalityPrefix =
                        extractLeadingEqualityPartitionSpecWhenOnlyAnd(
                                partitionKeys, predicate.get(), partitionType);
                if (!equalityPrefix.isEmpty()) {
                    // Use optimized scan for specific partition path
                    String partitionPath =
                            PartitionPathUtils.generatePartitionPath(
                                    equalityPrefix, partitionType, onlyValueInPath);
                    scanPath = new Path(tableLocation, partitionPath);
                    level = partitionKeys.size() - equalityPrefix.size();
                }
            }
        }
        return Pair.of(scanPath, level);
    }

    private List<Split> createSplits(FileIO fileIO, Path path, BinaryRow partition)
            throws IOException {
        List<FormatDataSplit.FileMeta> segments = new ArrayList<>();
        FileStatus[] files = fileIO.listFiles(path, true);
        Arrays.sort(files, Comparator.comparing(file -> file.getPath().toString()));
        for (FileStatus file : files) {
            if (isDataFileName(file.getPath().getName())) {
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

    public static Map<String, String> extractLeadingEqualityPartitionSpecWhenOnlyAnd(
            List<String> partitionKeys, Predicate predicate, RowType partitionType) {
        List<Predicate> predicates = PredicateBuilder.splitAnd(predicate);
        Map<String, String> equals = new HashMap<>();
        for (Predicate sub : predicates) {
            if (sub instanceof LeafPredicate) {
                Optional<FieldRef> fieldRefOptional = ((LeafPredicate) sub).fieldRefOptional();
                if (fieldRefOptional.isPresent()) {
                    FieldRef fieldRef = fieldRefOptional.get();
                    LeafFunction function = ((LeafPredicate) sub).function();
                    String field = fieldRef.name();
                    if (function instanceof Equal && partitionKeys.contains(field)) {
                        equals.put(
                                field,
                                partitionLiteralToString(
                                        fieldRef.type(), ((LeafPredicate) sub).literals().get(0)));
                    }
                }
            }
        }
        Map<String, String> result = new HashMap<>(partitionKeys.size());
        for (String partitionKey : partitionKeys) {
            if (equals.containsKey(partitionKey)) {
                result.put(partitionKey, equals.get(partitionKey));
            } else {
                break;
            }
        }
        return result;
    }

    private static String partitionLiteralToString(DataType type, Object literal) {
        if (literal == null) {
            return null;
        }

        CastExecutor<Object, BinaryString> executor =
                (CastExecutor<Object, BinaryString>)
                        CastExecutors.resolve(type, VarCharType.STRING_TYPE);
        BinaryString value = executor.cast(literal);
        return value == null ? null : value.toString();
    }
}
