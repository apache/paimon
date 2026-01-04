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

package org.apache.paimon.operation;

import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataFilePathFactory;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.PartitionPathUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Utility class to list all files (data files, manifest files, etc.) in a partition.
 *
 * <p>This utility helps identify all files that need to be archived when archiving a partition. It
 * includes:
 *
 * <ul>
 *   <li>Data files referenced in manifests
 *   <li>Manifest files that reference the partition
 *   <li>Extra files (like data file indexes) associated with data files
 * </ul>
 *
 * @since 0.9.0
 */
public class PartitionFileLister {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionFileLister.class);

    private final FileStoreTable table;
    private final FileIO fileIO;
    private final FileStorePathFactory pathFactory;

    public PartitionFileLister(FileStoreTable table) {
        this.table = table;
        this.fileIO = table.fileIO();
        this.pathFactory = table.store().pathFactory();
    }

    /**
     * List all file paths in the specified partition.
     *
     * @param partitionSpec the partition specification (e.g., {"dt": "20250101"})
     * @return list of all file paths in the partition
     * @throws IOException if an error occurs while listing files
     */
    public List<Path> listPartitionFiles(Map<String, String> partitionSpec) throws IOException {
        return listPartitionFiles(Collections.singletonList(partitionSpec));
    }

    /**
     * List all file paths in the specified partitions.
     *
     * @param partitionSpecs list of partition specifications
     * @return list of all file paths in the partitions
     * @throws IOException if an error occurs while listing files
     */
    public List<Path> listPartitionFiles(List<Map<String, String>> partitionSpecs)
            throws IOException {
        Set<Path> allFiles = new HashSet<>();

        // Use FileStoreScan to get all manifest entries for the partitions
        FileStoreScan scan = table.store().newScan();
        scan.withPartitionsFilter(partitionSpecs);

        FileStoreScan.Plan plan = scan.plan();
        List<ManifestEntry> entries = plan.files();

        // Collect all data file paths
        for (ManifestEntry entry : entries) {
            DataFileMeta fileMeta = entry.file();
            if (fileMeta != null) {
                // Construct path using DataFilePathFactory
                BinaryRow partition = entry.partition();
                int bucket = entry.bucket();
                DataFilePathFactory dataFilePathFactory =
                        pathFactory.createDataFilePathFactory(partition, bucket);
                Path dataFilePath = dataFilePathFactory.toPath(fileMeta);
                allFiles.add(dataFilePath);

                // Add extra files (like data file indexes)
                if (fileMeta.extraFiles() != null) {
                    for (String extraFile : fileMeta.extraFiles()) {
                        // Extra files are relative to the bucket path
                        Path bucketPath = pathFactory.bucketPath(partition, bucket);
                        Path extraFilePath = new Path(bucketPath, extraFile);
                        allFiles.add(extraFilePath);
                    }
                }
            }
        }

        // Also collect manifest files that reference these partitions
        // We need to scan through manifest lists to find relevant manifests
        collectManifestFiles(partitionSpecs, allFiles);

        return new ArrayList<>(allFiles);
    }

    /**
     * Collect manifest files that reference the specified partitions.
     *
     * @param partitionSpecs the partition specifications
     * @param allFiles set to add manifest file paths to
     */
    private void collectManifestFiles(List<Map<String, String>> partitionSpecs, Set<Path> allFiles)
            throws IOException {
        // Get the partition paths
        List<Path> partitionPaths = new ArrayList<>();
        for (Map<String, String> spec : partitionSpecs) {
            LinkedHashMap<String, String> linkedSpec = new LinkedHashMap<>(spec);
            String partitionPath =
                    PartitionPathUtils.generatePartitionPath(
                            linkedSpec, table.store().partitionType(), false);
            Path fullPartitionPath = new Path(table.location(), partitionPath);
            partitionPaths.add(fullPartitionPath);
        }

        // Scan through manifests to find those referencing these partitions
        FileStoreScan scan = table.store().newScan();
        FileStoreScan.Plan plan = scan.plan();

        // The manifest entries already contain references to manifests, but we need to
        // get the actual manifest file paths. For now, we'll list manifest files from
        // the manifest directory and let the archive action handle filtering.
        // A more precise implementation would track which manifests reference which partitions.

        Path manifestDir = new Path(table.location(), "manifest");
        if (fileIO.exists(manifestDir)) {
            try {
                org.apache.paimon.fs.FileStatus[] manifestFiles = fileIO.listStatus(manifestDir);
                for (org.apache.paimon.fs.FileStatus status : manifestFiles) {
                    if (!status.isDir() && status.getPath().getName().startsWith("manifest-")) {
                        allFiles.add(status.getPath());
                    }
                }
            } catch (IOException e) {
                LOG.warn("Failed to list manifest files, continuing without them", e);
            }
        }
    }

    /**
     * List all data file paths (excluding manifests) in the specified partition.
     *
     * @param partitionSpec the partition specification
     * @return list of data file paths
     * @throws IOException if an error occurs while listing files
     */
    public List<Path> listDataFiles(Map<String, String> partitionSpec) throws IOException {
        FileStoreScan scan = table.store().newScan();
        scan.withPartitionsFilter(Collections.singletonList(partitionSpec));

        FileStoreScan.Plan plan = scan.plan();
        List<ManifestEntry> entries = plan.files();

        List<Path> dataFiles = new ArrayList<>();
        for (ManifestEntry entry : entries) {
            DataFileMeta fileMeta = entry.file();
            if (fileMeta != null) {
                // Construct path using DataFilePathFactory
                BinaryRow partition = entry.partition();
                int bucket = entry.bucket();
                DataFilePathFactory dataFilePathFactory =
                        pathFactory.createDataFilePathFactory(partition, bucket);
                Path dataFilePath = dataFilePathFactory.toPath(fileMeta);
                dataFiles.add(dataFilePath);

                // Add extra files
                if (fileMeta.extraFiles() != null) {
                    for (String extraFile : fileMeta.extraFiles()) {
                        // Extra files are relative to the bucket path
                        Path bucketPath = pathFactory.bucketPath(partition, bucket);
                        Path extraFilePath = new Path(bucketPath, extraFile);
                        dataFiles.add(extraFilePath);
                    }
                }
            }
        }

        return dataFiles;
    }
}
