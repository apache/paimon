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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.PartitionEntry;
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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/** {@link TableScan} for {@link FormatTable}. */
public class FormatTableScan implements InnerTableScan {

    final FormatTable table;
    final CoreOptions coreOptions;
    @Nullable private PartitionPredicate partitionFilter;
    private final SplitEnumerator splitEnumerator;
    @Nullable private final Integer limit;

    public FormatTableScan(
            FormatTable table,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Integer limit) {
        this.table = table;
        this.coreOptions = new CoreOptions(table.options());
        this.partitionFilter = partitionFilter;
        this.limit = limit;
        this.splitEnumerator = SplitEnumerator.create(table, coreOptions, table.partitionManager());
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
        return splitEnumerator.listPartitionEntries();
    }

    @Override
    public InnerTableScan withFilter(Predicate predicate) {
        throw new UnsupportedOperationException("Filter is not supported for FormatTable.");
    }

    public static boolean isDataFileName(String fileName) {
        return fileName != null && !PartitionPathUtils.isHiddenName(fileName);
    }

    /**
     * Lists the data files under {@code listedRoot}, skipping committer staging trees ({@code
     * _temporary/}, {@code __magic_job-<id>/}, {@code .hive-staging_*}) without descending into
     * them. Files staged there carry ordinary data file names, so a name alone cannot tell them
     * apart from committed data; the directory above them can.
     *
     * <p>Only entries below {@code listedRoot} are judged, never the root itself, which may
     * legitimately sit under a warehouse path such as {@code oss://bucket/_warehouse/db/t}, and
     * which is the default partition directory of the value-only layout when a null partition value
     * is read.
     *
     * @throws FileNotFoundException if {@code listedRoot} does not exist and the {@link FileIO}
     *     signals that by throwing. {@link FileIO#listStatus} may instead answer with no entries -
     *     {@code LocalFileIO} does - and the result is then empty. Either way a directory that
     *     disappears further down is skipped, leaving the rest of the listing complete.
     */
    static List<FileStatus> listDataFiles(FileIO fileIO, Path listedRoot) throws IOException {
        List<FileStatus> dataFiles = new ArrayList<>();
        List<Path> level = new ArrayList<>();
        // A missing root is the caller's signal, e.g. a partition that the catalog knows but whose
        // directory is gone, so let it surface.
        collectDataFiles(fileIO.listStatus(listedRoot), dataFiles, level);
        while (!level.isEmpty()) {
            List<Path> next = new ArrayList<>();
            for (Path directory : level) {
                try {
                    collectDataFiles(fileIO.listStatus(directory), dataFiles, next);
                } catch (FileNotFoundException e) {
                    // The directory vanished after its parent listed it; the rest of the listing
                    // is still complete.
                }
            }
            level = next;
        }
        return dataFiles;
    }

    private static void collectDataFiles(
            @Nullable FileStatus[] children, List<FileStatus> dataFiles, List<Path> directories) {
        if (children == null) {
            return;
        }
        for (FileStatus child : children) {
            if (PartitionPathUtils.isHiddenName(child.getPath().getName())) {
                continue;
            }
            if (child.isDir()) {
                directories.add(child.getPath());
            } else {
                dataFiles.add(child);
            }
        }
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
