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
import org.apache.paimon.manifest.PartitionEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.partition.PartitionPredicate;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.BinPacking;
import org.apache.paimon.utils.Pair;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.OptionalLong;

import static org.apache.paimon.format.text.HadoopCompressionUtils.isCompressed;
import static org.apache.paimon.format.text.TextLineReader.isDefaultDelimiter;
import static org.apache.paimon.utils.InternalRowPartitionComputer.convertSpecToInternalRow;

/**
 * Enumerates {@link FormatDataSplit}s for a {@link FormatTable}.
 *
 * <p>Partitions come either from the catalog ({@link CatalogSplitEnumerator}) or from the
 * filesystem ({@link FileSystemSplitEnumerator}); the source is chosen once by {@link #create} so
 * the enumeration paths stay free of {@code partitionManager != null} branching.
 */
abstract class SplitEnumerator {

    protected final FormatTable table;
    protected final CoreOptions coreOptions;
    protected final long targetSplitSize;
    protected final long openFileCost;
    protected final FormatTable.Format format;

    SplitEnumerator(FormatTable table, CoreOptions coreOptions) {
        this.table = table;
        this.coreOptions = coreOptions;
        this.targetSplitSize = coreOptions.splitTargetSize();
        this.openFileCost = coreOptions.splitOpenFileCost();
        this.format = table.format();
    }

    static SplitEnumerator create(
            FormatTable table,
            CoreOptions coreOptions,
            @Nullable FormatTablePartitionManager partitionManager) {
        if (partitionManager != null) {
            return new CatalogSplitEnumerator(table, coreOptions, partitionManager);
        }
        return new FileSystemSplitEnumerator(table, coreOptions);
    }

    final List<Split> enumerate(@Nullable PartitionPredicate partitionFilter) throws IOException {
        if (table.partitionKeys().isEmpty()) {
            return createSplits(table.fileIO(), new Path(table.location()), null);
        }
        return enumeratePartitions(partitionFilter);
    }

    ScanPlan plan(@Nullable PartitionPredicate partitionFilter) throws IOException {
        return new ScanPlan(enumerate(partitionFilter), OptionalLong.empty());
    }

    /** Enumerate splits for a partitioned table; partitions come from the concrete source. */
    abstract List<Split> enumeratePartitions(@Nullable PartitionPredicate partitionFilter)
            throws IOException;

    abstract List<Pair<LinkedHashMap<String, String>, Path>> findPartitions(
            @Nullable PartitionPredicate partitionFilter);

    abstract List<PartitionEntry> listPartitionEntries();

    List<PartitionEntry> listPartitionEntries(@Nullable PartitionPredicate partitionFilter) {
        List<PartitionEntry> entries = listPartitionEntries();
        if (partitionFilter == null) {
            return entries;
        }
        List<PartitionEntry> filtered = new ArrayList<>();
        for (PartitionEntry entry : entries) {
            if (partitionFilter.test(entry.partition())) {
                filtered.add(entry);
            }
        }
        return filtered;
    }

    static final class ScanPlan {

        private final List<Split> splits;
        private final OptionalLong rowCount;

        ScanPlan(List<Split> splits, OptionalLong rowCount) {
            this.splits = splits;
            this.rowCount = rowCount;
        }

        List<Split> splits() {
            return splits;
        }

        OptionalLong rowCount() {
            return rowCount;
        }
    }

    BinaryRow toPartitionRow(LinkedHashMap<String, String> partitionSpec) {
        RowType partitionType = table.partitionType();
        GenericRow row =
                convertSpecToInternalRow(partitionSpec, partitionType, table.defaultPartName());
        return new InternalRowSerializer(partitionType).toBinaryRow(row);
    }

    List<Split> createSplits(FileIO fileIO, Path path, @Nullable BinaryRow partition)
            throws IOException {
        List<FormatDataSplit.FileMeta> segments = new ArrayList<>();
        // The listed directory is a single partition, or the table itself when unpartitioned.
        List<FileStatus> files = FormatTableScan.listDataFiles(fileIO, path);
        files.sort(Comparator.comparing(file -> file.getPath().toString()));
        for (FileStatus file : files) {
            segments.addAll(toSegments(file));
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
}
