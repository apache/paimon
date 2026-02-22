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

package org.apache.paimon.spark.action;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.fs.StorageType;
import org.apache.paimon.operation.PartitionFileLister;
import org.apache.paimon.table.FileStoreTable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Action to archive partition files to different storage tiers.
 *
 * <p>This action archives all files (data files, manifest files, etc.) in specified partitions to
 * Archive or ColdArchive storage tiers. The action supports distributed execution using Spark.
 *
 * @since 0.9.0
 */
public class ArchivePartitionAction {

    private static final Logger LOG = LoggerFactory.getLogger(ArchivePartitionAction.class);

    private final FileStoreTable table;
    private final JavaSparkContext sparkContext;

    public ArchivePartitionAction(FileStoreTable table, JavaSparkContext sparkContext) {
        this.table = table;
        this.sparkContext = sparkContext;
    }

    /**
     * Archive partitions to the specified storage type.
     *
     * @param partitionSpecs list of partition specifications to archive
     * @param storageType the storage type to archive to (Archive or ColdArchive)
     * @return number of files archived
     * @throws IOException if an error occurs during archiving
     */
    public long archive(List<Map<String, String>> partitionSpecs, StorageType storageType)
            throws IOException {
        if (storageType == StorageType.Standard) {
            throw new IllegalArgumentException(
                    "Cannot archive to Standard storage type. Use Archive or ColdArchive.");
        }

        FileIO fileIO = table.fileIO();
        if (!fileIO.isObjectStore()) {
            throw new UnsupportedOperationException(
                    "Archive operation is only supported for object stores. "
                            + "Current FileIO: "
                            + fileIO.getClass().getName());
        }

        PartitionFileLister fileLister = new PartitionFileLister(table);
        List<Path> allFiles = fileLister.listPartitionFiles(partitionSpecs);

        if (allFiles.isEmpty()) {
            LOG.info("No files found for partitions: {}", partitionSpecs);
            return 0;
        }

        LOG.info(
                "Archiving {} files for {} partitions to storage type {}",
                allFiles.size(),
                partitionSpecs.size(),
                storageType);

        // Distribute file archiving across Spark executors
        int parallelism = Math.min(allFiles.size(), sparkContext.defaultParallelism());
        JavaRDD<Path> filesRDD = sparkContext.parallelize(allFiles, parallelism);

        JavaRDD<Long> archivedCountRDD =
                filesRDD.mapPartitions(
                        (FlatMapFunction<Iterator<Path>, Long>)
                                pathIterator -> {
                                    List<Long> counts = new ArrayList<>();
                                    long count = 0;
                                    while (pathIterator.hasNext()) {
                                        Path path = pathIterator.next();
                                        try {
                                            Optional<Path> newPath =
                                                    fileIO.archive(path, storageType);
                                            if (newPath.isPresent()) {
                                                LOG.warn(
                                                        "File archiving resulted in path change: {} -> {}",
                                                        path,
                                                        newPath.get());
                                            }
                                            count++;
                                        } catch (Exception e) {
                                            LOG.error("Failed to archive file: " + path, e);
                                            throw new IOException(
                                                    "Failed to archive file: " + path, e);
                                        }
                                    }
                                    counts.add(count);
                                    return counts.iterator();
                                });

        long totalArchived = archivedCountRDD.reduce(Long::sum);
        LOG.info("Successfully archived {} files", totalArchived);
        return totalArchived;
    }

    /**
     * Restore archived partitions.
     *
     * @param partitionSpecs list of partition specifications to restore
     * @param duration the duration to keep files restored (may be ignored by some implementations)
     * @return number of files restored
     * @throws IOException if an error occurs during restore
     */
    public long restoreArchive(
            List<Map<String, String>> partitionSpecs, Duration duration) throws IOException {
        FileIO fileIO = table.fileIO();
        if (!fileIO.isObjectStore()) {
            throw new UnsupportedOperationException(
                    "Restore archive operation is only supported for object stores.");
        }

        PartitionFileLister fileLister = new PartitionFileLister(table);
        List<Path> allFiles = fileLister.listPartitionFiles(partitionSpecs);

        if (allFiles.isEmpty()) {
            LOG.info("No files found for partitions: {}", partitionSpecs);
            return 0;
        }

        LOG.info(
                "Restoring {} files for {} partitions (duration: {})",
                allFiles.size(),
                partitionSpecs.size(),
                duration);

        // Distribute file restoration across Spark executors
        int parallelism = Math.min(allFiles.size(), sparkContext.defaultParallelism());
        JavaRDD<Path> filesRDD = sparkContext.parallelize(allFiles, parallelism);

        JavaRDD<Long> restoredCountRDD =
                filesRDD.mapPartitions(
                        (FlatMapFunction<Iterator<Path>, Long>)
                                pathIterator -> {
                                    List<Long> counts = new ArrayList<>();
                                    long count = 0;
                                    while (pathIterator.hasNext()) {
                                        Path path = pathIterator.next();
                                        try {
                                            fileIO.restoreArchive(path, duration);
                                            count++;
                                        } catch (Exception e) {
                                            LOG.error("Failed to restore archive file: " + path, e);
                                            throw new IOException(
                                                    "Failed to restore archive file: " + path, e);
                                        }
                                    }
                                    counts.add(count);
                                    return counts.iterator();
                                });

        long totalRestored = restoredCountRDD.reduce(Long::sum);
        LOG.info("Successfully restored {} files", totalRestored);
        return totalRestored;
    }

    /**
     * Unarchive partitions, moving them back to standard storage.
     *
     * @param partitionSpecs list of partition specifications to unarchive
     * @param currentStorageType the current storage type of the files (Archive or ColdArchive)
     * @return number of files unarchived
     * @throws IOException if an error occurs during unarchiving
     */
    public long unarchive(
            List<Map<String, String>> partitionSpecs, StorageType currentStorageType)
            throws IOException {
        if (currentStorageType == StorageType.Standard) {
            throw new IllegalArgumentException(
                    "Cannot unarchive from Standard storage type. Files are already in standard storage.");
        }

        FileIO fileIO = table.fileIO();
        if (!fileIO.isObjectStore()) {
            throw new UnsupportedOperationException(
                    "Unarchive operation is only supported for object stores.");
        }

        PartitionFileLister fileLister = new PartitionFileLister(table);
        List<Path> allFiles = fileLister.listPartitionFiles(partitionSpecs);

        if (allFiles.isEmpty()) {
            LOG.info("No files found for partitions: {}", partitionSpecs);
            return 0;
        }

        LOG.info(
                "Unarchiving {} files for {} partitions from storage type {}",
                allFiles.size(),
                partitionSpecs.size(),
                currentStorageType);

        // Distribute file unarchiving across Spark executors
        int parallelism = Math.min(allFiles.size(), sparkContext.defaultParallelism());
        JavaRDD<Path> filesRDD = sparkContext.parallelize(allFiles, parallelism);

        JavaRDD<Long> unarchivedCountRDD =
                filesRDD.mapPartitions(
                        (FlatMapFunction<Iterator<Path>, Long>)
                                pathIterator -> {
                                    List<Long> counts = new ArrayList<>();
                                    long count = 0;
                                    while (pathIterator.hasNext()) {
                                        Path path = pathIterator.next();
                                        try {
                                            Optional<Path> newPath =
                                                    fileIO.unarchive(path, currentStorageType);
                                            if (newPath.isPresent()) {
                                                LOG.warn(
                                                        "File unarchiving resulted in path change: {} -> {}",
                                                        path,
                                                        newPath.get());
                                            }
                                            count++;
                                        } catch (Exception e) {
                                            LOG.error("Failed to unarchive file: " + path, e);
                                            throw new IOException(
                                                    "Failed to unarchive file: " + path, e);
                                        }
                                    }
                                    counts.add(count);
                                    return counts.iterator();
                                });

        long totalUnarchived = unarchivedCountRDD.reduce(Long::sum);
        LOG.info("Successfully unarchived {} files", totalUnarchived);
        return totalUnarchived;
    }
}

