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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.partition.PartitionPredicate.DefaultPartitionPredicate;
import org.apache.paimon.partition.PartitionPredicate.MultiplePartitionPredicate;
import org.apache.paimon.predicate.Equal;
import org.apache.paimon.predicate.LeafFunction;
import org.apache.paimon.predicate.LeafPredicate;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.predicate.PredicateBuilder;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.source.InnerTableScan;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.TableScan;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.InternalRowPartitionComputer;
import org.apache.paimon.utils.Pair;
import org.apache.paimon.utils.PartitionPathUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.InternalRowPartitionComputer.convertSpecToInternalRow;
import static org.apache.paimon.utils.PartitionPathUtils.searchPartSpecAndPaths;

/** {@link TableScan} for {@link FormatTable}. */
public class FormatTableScan implements InnerTableScan {

    private final FormatTable table;
    private final CoreOptions coreOptions;
    @Nullable private PartitionPredicate partitionFilter;
    @Nullable private final Integer limit;

    public FormatTableScan(
            FormatTable table,
            @Nullable PartitionPredicate partitionFilter,
            @Nullable Integer limit) {
        this.table = table;
        this.coreOptions = new CoreOptions(table.options());
        this.partitionFilter = partitionFilter;
        this.limit = limit;
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
        List<Pair<LinkedHashMap<String, String>, Path>> partition2Paths =
                searchPartSpecAndPaths(
                        table.fileIO(),
                        new Path(table.location()),
                        table.partitionKeys().size(),
                        table.partitionKeys(),
                        coreOptions.formatTablePartitionOnlyValueInPath());
        List<PartitionEntry> partitionEntries = new ArrayList<>();
        for (Pair<LinkedHashMap<String, String>, Path> partition2Path : partition2Paths) {
            BinaryRow row = toPartitionRow(partition2Path.getKey());
            partitionEntries.add(new PartitionEntry(row, -1L, -1L, -1L, -1L));
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

    private BinaryRow toPartitionRow(LinkedHashMap<String, String> partitionSpec) {
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
                    for (Pair<LinkedHashMap<String, String>, Path> pair : findPartitions()) {
                        LinkedHashMap<String, String> partitionSpec = pair.getKey();
                        BinaryRow partitionRow = toPartitionRow(partitionSpec);
                        if (partitionFilter == null || partitionFilter.test(partitionRow)) {
                            splits.addAll(createSplits(fileIO, pair.getValue(), partitionRow));
                        }
                    }
                } else {
                    splits.addAll(createSplits(fileIO, new Path(table.location()), null));
                }
                if (limit != null) {
                    if (limit <= 0) {
                        return new ArrayList<>();
                    }
                    if (splits.size() > limit) {
                        return splits.subList(0, limit);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("Failed to scan files", e);
            }
            return splits;
        }
    }

    private List<Pair<LinkedHashMap<String, String>, Path>> findPartitions() {
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
            // search paths
            Pair<Path, Integer> scanPathAndLevel =
                    computeScanPathAndLevel(
                            new Path(table.location()),
                            table.partitionKeys(),
                            partitionFilter,
                            table.partitionType(),
                            onlyValueInPath);
            return searchPartSpecAndPaths(
                    table.fileIO(),
                    scanPathAndLevel.getLeft(),
                    scanPathAndLevel.getRight(),
                    table.partitionKeys(),
                    onlyValueInPath);
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

    protected static Pair<Path, Integer> computeScanPathAndLevel(
            Path tableLocation,
            List<String> partitionKeys,
            PartitionPredicate partitionFilter,
            RowType partitionType,
            boolean onlyValueInPath) {
        Path scanPath = tableLocation;
        int level = partitionKeys.size();
        if (!partitionKeys.isEmpty()) {
            // Try to optimize for equality partition filters
            if (partitionFilter instanceof DefaultPartitionPredicate) {
                Map<String, String> equalityPrefix =
                        extractLeadingEqualityPartitionSpecWhenOnlyAnd(
                                partitionKeys,
                                ((DefaultPartitionPredicate) partitionFilter).predicate());
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
        List<Split> splits = new ArrayList<>();
        FileStatus[] files = fileIO.listFiles(path, true);
        for (FileStatus file : files) {
            if (isDataFileName(file.getPath().getName())) {
                FormatDataSplit split =
                        new FormatDataSplit(file.getPath(), 0, file.getLen(), partition);
                splits.add(split);
            }
        }
        return splits;
    }

    public static Map<String, String> extractLeadingEqualityPartitionSpecWhenOnlyAnd(
            List<String> partitionKeys, Predicate predicate) {
        List<Predicate> predicates = PredicateBuilder.splitAnd(predicate);
        Map<String, String> equals = new HashMap<>();
        for (Predicate sub : predicates) {
            if (sub instanceof LeafPredicate) {
                LeafFunction function = ((LeafPredicate) sub).function();
                String field = ((LeafPredicate) sub).fieldName();
                if (function instanceof Equal && partitionKeys.contains(field)) {
                    equals.put(field, ((LeafPredicate) sub).literals().get(0).toString());
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
}
