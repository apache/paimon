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
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.partition.PartitionPredicate.MultiplePartitionPredicate;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.paimon.utils.PartitionPathUtils.searchPartSpecAndPaths;

/** A {@link SplitEnumerator} whose partitions are discovered from the filesystem. */
final class FileSystemSplitEnumerator extends SplitEnumerator {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemSplitEnumerator.class);

    FileSystemSplitEnumerator(FormatTable table, CoreOptions coreOptions) {
        super(table, coreOptions);
    }

    @Override
    List<Split> enumeratePartitions(@Nullable PartitionPredicate partitionFilter)
            throws IOException {
        List<Split> splits = new ArrayList<>();
        FileIO fileIO = table.fileIO();
        for (Pair<LinkedHashMap<String, String>, Path> pair : findPartitions(partitionFilter)) {
            BinaryRow partitionRow = toPartitionRow(pair.getKey());
            if (partitionFilter == null || partitionFilter.test(partitionRow)) {
                splits.addAll(createSplits(fileIO, pair.getValue(), partitionRow));
            }
        }
        return splits;
    }

    @Override
    List<Pair<LinkedHashMap<String, String>, Path>> findPartitions(
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

    @Override
    List<PartitionEntry> listPartitionEntries() {
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
            // Discovering partitions from directories measures nothing about what is inside them,
            // so every statistic is unknown rather than zero.
            partitionEntries.add(
                    new PartitionEntry(
                            row,
                            PartitionStatistics.UNKNOWN,
                            PartitionStatistics.UNKNOWN,
                            PartitionStatistics.UNKNOWN,
                            PartitionStatistics.UNKNOWN,
                            PartitionStatistics.UNKNOWN_TOTAL_BUCKETS));
        }
        return partitionEntries;
    }
}
