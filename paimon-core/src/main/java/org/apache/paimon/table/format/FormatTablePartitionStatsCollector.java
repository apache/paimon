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
import org.apache.paimon.format.FileFormat;
import org.apache.paimon.format.SimpleStatsExtractor;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.statistics.SimpleColStatsCollector;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.types.RowType;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.ThreadPoolUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Measures what the partitions of a Format Table currently hold. File count, byte size and last
 * file creation time come from a directory listing; the row count needs every file's footer, which
 * no listing opens, so it is asked for rather than assumed. A partition holding nothing measures as
 * an exact zero, with no last file to date.
 *
 * <p>It lists through {@link FormatTableScan#listDataFiles}, the listing the scan itself uses, so a
 * measurement counts exactly the files a reader would return and committer staging trees are pruned
 * rather than walked. A listing failure aborts the whole collection: a truncated listing looks
 * exactly like a partition that lost files.
 *
 * <p>The result is a whole-partition measurement, so it replaces rather than accumulates. It never
 * decides that a partition should exist or stop existing; it measures the ones it is given.
 */
public class FormatTablePartitionStatsCollector {

    private static final Logger LOG =
            LoggerFactory.getLogger(FormatTablePartitionStatsCollector.class);

    /** Row counts that need no columns: the file footer alone answers how many rows it holds. */
    private static final RowType NO_COLUMNS = RowType.builder().build();

    private static final SimpleColStatsCollector.Factory[] NO_COLLECTORS =
            new SimpleColStatsCollector.Factory[0];

    private final FormatTable table;

    private final boolean onlyValueInPath;
    private final int parallelism;

    private final boolean withRecordCount;

    /** Measures from the listing alone, leaving the record count unknown. */
    public FormatTablePartitionStatsCollector(FormatTable table, int parallelism) {
        this(table, false, parallelism);
    }

    /**
     * Measures from the listing, and when {@code withRecordCount} is set also opens every file's
     * footer for the rows it holds. That is the expensive half, so it is asked for rather than
     * assumed.
     */
    public FormatTablePartitionStatsCollector(
            FormatTable table, boolean withRecordCount, int parallelism) {
        this.table = table;
        this.onlyValueInPath =
                new CoreOptions(table.options()).formatTablePartitionOnlyValueInPath();
        this.withRecordCount = withRecordCount;
        this.parallelism = Math.max(1, parallelism);
    }

    /**
     * Measures the given partitions. The result is aligned to {@code partitions} one for one, so a
     * caller can send it straight to the catalog alongside the same specs.
     */
    public List<PartitionStatistics> collect(List<Map<String, String>> partitions) {
        if (partitions.isEmpty()) {
            return Collections.emptyList();
        }
        SimpleStatsExtractor rowCounter = withRecordCount ? rowCounter() : null;
        if (withRecordCount && rowCounter == null) {
            LOG.info(
                    "No row counter could be built for format {} of table {}, so the row counts of "
                            + "the measured partitions stay unknown.",
                    table.format(),
                    table.fullName());
        }
        // A listing is one request per partition, a footer read one per file, so counting rows
        // leaves work to spread even when a single partition was asked for.
        int threads = rowCounter == null ? Math.min(parallelism, partitions.size()) : parallelism;
        if (threads == 1) {
            List<PartitionStatistics> statistics = new ArrayList<>(partitions.size());
            for (Map<String, String> partition : partitions) {
                List<FileStatus> files = listDataFiles(partition);
                List<Long> rowCounts = new ArrayList<>(files.size());
                for (FileStatus file : files) {
                    if (rowCounter != null) {
                        rowCounts.add(rowCount(rowCounter, file));
                    }
                }
                statistics.add(statistics(partition, files, sum(rowCounts, rowCounter != null)));
            }
            return statistics;
        }

        ExecutorService executor =
                ThreadPoolUtils.createCachedThreadPool(threads, "FORMAT-TABLE-STATS-THREAD-POOL");
        try {
            List<Future<List<FileStatus>>> listings = new ArrayList<>(partitions.size());
            for (Map<String, String> partition : partitions) {
                listings.add(executor.submit(() -> listDataFiles(partition)));
            }
            List<List<FileStatus>> files = new ArrayList<>(partitions.size());
            for (Future<List<FileStatus>> listing : listings) {
                files.add(await(listing));
            }
            // Every file of every partition goes to the same pool, so one partition holding many
            // files is counted with all of it rather than with one thread of it.
            List<List<Future<Long>>> rowCounts = new ArrayList<>(partitions.size());
            for (List<FileStatus> partitionFiles : files) {
                List<Future<Long>> counts = new ArrayList<>(partitionFiles.size());
                for (FileStatus file : partitionFiles) {
                    if (rowCounter != null) {
                        counts.add(executor.submit(() -> rowCount(rowCounter, file)));
                    }
                }
                rowCounts.add(counts);
            }
            List<PartitionStatistics> statistics = new ArrayList<>(partitions.size());
            for (int i = 0; i < partitions.size(); i++) {
                List<Long> counted = new ArrayList<>(rowCounts.get(i).size());
                for (Future<Long> count : rowCounts.get(i)) {
                    counted.add(await(count));
                }
                statistics.add(
                        statistics(
                                partitions.get(i), files.get(i), sum(counted, rowCounter != null)));
            }
            return statistics;
        } finally {
            executor.shutdownNow();
        }
    }

    private <T> T await(Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(
                    "Interrupted while measuring partitions of table " + table.fullName(), e);
        } catch (ExecutionException e) {
            throw asRuntime(e.getCause());
        }
    }

    /** The data files of a partition; a registered partition whose directory is gone has none. */
    private List<FileStatus> listDataFiles(Map<String, String> partition) {
        Path partitionPath = partitionPath(partition);
        try {
            // A missing directory surfaces here as a FileNotFoundException, so it needs no
            // separate existence check.
            return FormatTableScan.listDataFiles(table.fileIO(), partitionPath);
        } catch (FileNotFoundException e) {
            return Collections.emptyList();
        } catch (IOException e) {
            throw new UncheckedIOException(
                    String.format(
                            "Failed to list partition %s of table %s; no statistics are written "
                                    + "because a partial listing cannot be told apart from a "
                                    + "partition that lost files.",
                            partitionPath, table.fullName()),
                    e);
        }
    }

    /**
     * The rows the counted files hold. One file whose footer could not be read makes the whole
     * partition unknown rather than short: a sum missing a file, reported as exact, is worse than
     * no number at all. A measurement that counted nothing knows no row count, while one that
     * counted an empty partition knows it holds none.
     */
    private static long sum(List<Long> rowCounts, boolean counted) {
        if (!counted) {
            return PartitionStatistics.UNKNOWN;
        }
        long rows = 0;
        for (long rowCount : rowCounts) {
            if (!PartitionStatistics.isKnown(rowCount)) {
                return PartitionStatistics.UNKNOWN;
            }
            rows += rowCount;
        }
        return rows;
    }

    /** What the listed files of a partition add up to. */
    private static PartitionStatistics statistics(
            Map<String, String> partition, List<FileStatus> files, long recordCount) {
        long fileSizeInBytes = 0;
        long lastFileCreationTime = PartitionStatistics.UNKNOWN;
        for (FileStatus file : files) {
            fileSizeInBytes += file.getLen();
            lastFileCreationTime = Math.max(lastFileCreationTime, file.getModificationTime());
        }
        return new PartitionStatistics(
                partition,
                recordCount,
                fileSizeInBytes,
                files.size(),
                lastFileCreationTime,
                PartitionStatistics.UNKNOWN_TOTAL_BUCKETS);
    }

    /**
     * Rows in one file, or unknown when its footer cannot be read. One unreadable file makes the
     * whole partition unknown rather than short: a sum missing a file, reported as exact, is worse
     * than no number at all.
     */
    private long rowCount(SimpleStatsExtractor rowCounter, FileStatus file) {
        try {
            return rowCounter
                    .extractWithFileInfo(table.fileIO(), file.getPath(), file.getLen())
                    .getRight()
                    .getRowCount();
        } catch (Exception e) {
            LOG.warn(
                    "Failed to read the row count of {} in table {}; the row count of its "
                            + "partition stays unknown.",
                    file.getPath(),
                    table.fullName(),
                    e);
            return PartitionStatistics.UNKNOWN;
        }
    }

    /**
     * A footer reader for this table's format, or null when the format carries no row count. It is
     * built with no columns on purpose: only the file's row count is wanted, and asking for column
     * statistics would both cost more and make the reader depend on the file schema matching the
     * table's.
     */
    @Nullable
    private SimpleStatsExtractor rowCounter() {
        try {
            CoreOptions options = new CoreOptions(table.options());
            Optional<SimpleStatsExtractor> extractor =
                    FileFormat.fileFormat(options).createStatsExtractor(NO_COLUMNS, NO_COLLECTORS);
            return extractor.orElse(null);
        } catch (Exception e) {
            LOG.warn(
                    "Failed to create a row counter for format {} of table {}; row counts stay "
                            + "unknown.",
                    table.format(),
                    table.fullName(),
                    e);
            return null;
        }
    }

    private Path partitionPath(Map<String, String> partition) {
        LinkedHashMap<String, String> ordered = new LinkedHashMap<>();
        for (String key : table.partitionKeys()) {
            if (!partition.containsKey(key)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Partition %s of table %s does not give a value for partition key "
                                        + "%s, so its directory cannot be located.",
                                partition, table.fullName(), key));
            }
            ordered.put(key, partition.get(key));
        }
        return new Path(
                table.location(),
                PartitionPathUtils.generatePartitionPathUtil(ordered, onlyValueInPath));
    }

    private static RuntimeException asRuntime(Throwable cause) {
        if (cause instanceof RuntimeException) {
            return (RuntimeException) cause;
        }
        return new RuntimeException(cause);
    }
}
