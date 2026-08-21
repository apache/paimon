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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileStatus;
import org.apache.paimon.fs.Path;
import org.apache.paimon.partition.PartitionStatistics;
import org.apache.paimon.table.FormatTable;
import org.apache.paimon.utils.PartitionPathUtils;
import org.apache.paimon.utils.ThreadPoolUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Measures what the partitions of a Format Table currently hold, by listing their directories. File
 * count, byte size and last file creation time come from the listing; the row count does not, since
 * no listing opens a file. A partition holding nothing measures as an exact zero on the file
 * numbers, with no last file to date.
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

    private final FormatTable table;

    private final boolean onlyValueInPath;
    private final int parallelism;

    public FormatTablePartitionStatsCollector(FormatTable table, int parallelism) {
        this.table = table;
        this.onlyValueInPath =
                new CoreOptions(table.options()).formatTablePartitionOnlyValueInPath();
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
        int threads = Math.min(parallelism, partitions.size());
        if (threads == 1) {
            List<PartitionStatistics> statistics = new ArrayList<>(partitions.size());
            for (Map<String, String> partition : partitions) {
                statistics.add(measure(partition));
            }
            return statistics;
        }

        ExecutorService executor =
                ThreadPoolUtils.createCachedThreadPool(threads, "FORMAT-TABLE-STATS-THREAD-POOL");
        try {
            List<Future<PartitionStatistics>> futures = new ArrayList<>(partitions.size());
            for (Map<String, String> partition : partitions) {
                futures.add(executor.submit(() -> measure(partition)));
            }
            List<PartitionStatistics> statistics = new ArrayList<>(partitions.size());
            for (Future<PartitionStatistics> future : futures) {
                try {
                    statistics.add(future.get());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(
                            "Interrupted while measuring partitions of table " + table.fullName(),
                            e);
                } catch (ExecutionException e) {
                    throw asRuntime(e.getCause());
                }
            }
            return statistics;
        } finally {
            executor.shutdownNow();
        }
    }

    private PartitionStatistics measure(Map<String, String> partition) {
        FileIO fileIO = table.fileIO();
        Path partitionPath = partitionPath(partition);
        List<FileStatus> files;
        try {
            // A missing directory surfaces here as a FileNotFoundException, so it needs no
            // separate existence check.
            files = FormatTableScan.listDataFiles(fileIO, partitionPath);
        } catch (FileNotFoundException e) {
            // A registered partition whose directory is gone reads as empty.
            return empty(partition);
        } catch (IOException e) {
            throw new UncheckedIOException(
                    String.format(
                            "Failed to list partition %s of table %s; no statistics are written "
                                    + "because a partial listing cannot be told apart from a "
                                    + "partition that lost files.",
                            partitionPath, table.fullName()),
                    e);
        }

        long fileCount = 0;
        long fileSizeInBytes = 0;
        long lastFileCreationTime = 0;
        for (FileStatus file : files) {
            fileCount++;
            fileSizeInBytes += file.getLen();
            lastFileCreationTime = Math.max(lastFileCreationTime, file.getModificationTime());
        }
        if (fileCount == 0) {
            return empty(partition);
        }
        return new PartitionStatistics(
                partition,
                // A listing never opens a file, so the rows a partition holds stay unknown.
                PartitionStatistics.UNKNOWN,
                fileSizeInBytes,
                fileCount,
                lastFileCreationTime,
                PartitionStatistics.UNKNOWN_TOTAL_BUCKETS);
    }

    /** A partition with nothing in it: the file numbers are an exact zero. */
    private static PartitionStatistics empty(Map<String, String> partition) {
        return new PartitionStatistics(
                partition,
                PartitionStatistics.UNKNOWN,
                0L,
                0L,
                PartitionStatistics.UNKNOWN,
                PartitionStatistics.UNKNOWN_TOTAL_BUCKETS);
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
