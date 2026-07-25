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
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.PartitionEntry;
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
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PartitionPathUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.utils.PartitionPathUtils.searchPartSpecAndPaths;

/** {@link TableScan} for {@link FormatTable}. */
public class FormatTableScan implements InnerTableScan {

    final FormatTable table;
    final CoreOptions coreOptions;
    @Nullable private PartitionPredicate partitionFilter;
    @Nullable private final FormatTablePartitionManager partitionManager;
    private final FormatTableSplitEnumerator splitEnumerator;
    @Nullable private final Integer limit;

    public FormatTableScan(
            FormatTable table,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Integer limit) {
        this.table = table;
        this.coreOptions = new CoreOptions(table.options());
        this.partitionFilter = partitionFilter;
        this.limit = limit;
        this.partitionManager = table.partitionManager();
        this.splitEnumerator = new FormatTableSplitEnumerator(table, coreOptions, partitionManager);
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
                splitEnumerator.warnIfFilesystemPartitionsExist();
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
                                toPartitionRow(
                                        splitEnumerator.normalizeSpec(
                                                partition.spec(), onlyValueInPath)),
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
        return splitEnumerator.toPartitionRow(partitionSpec);
    }

    private class FormatTableScanPlan implements Plan {
        @Override
        public List<Split> splits() {
            List<Split> splits = new ArrayList<>();
            try {
                splits.addAll(splitEnumerator.enumerate(partitionFilter));
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

    List<Pair<LinkedHashMap<String, String>, Path>> findPartitions() {
        return splitEnumerator.findPartitions(partitionFilter);
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
                // they are still applied before listing the partition files.
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
