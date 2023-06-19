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
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.ParallellyExecuteUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Util methods for snapshot and tag deletion. */
public class DeletionUtils {

    private static final Logger LOG = LoggerFactory.getLogger(DeletionUtils.class);

    public static Iterable<ManifestEntry> readEntries(
            List<ManifestFileMeta> manifests, ManifestFile manifestFile) {
        return ParallellyExecuteUtils.parallelismBatchIterable(
                files ->
                        files.parallelStream()
                                .flatMap(m -> manifestFile.read(m.fileName()).stream())
                                .collect(Collectors.toList()),
                manifests,
                null);
    }

    public static void indexDataFiles(
            Map<BinaryRow, Map<Integer, Set<String>>> dataFiles,
            Iterable<ManifestEntry> dataEntries) {
        for (ManifestEntry entry : dataEntries) {
            dataFiles
                    .computeIfAbsent(entry.partition(), p -> new HashMap<>())
                    .computeIfAbsent(entry.bucket(), b -> new HashSet<>())
                    .add(entry.file().fileName());
        }
    }

    public static boolean containsDataFile(
            Map<BinaryRow, Map<Integer, Set<String>>> dataFiles, ManifestEntry testee) {
        Map<Integer, Set<String>> buckets = dataFiles.get(testee.partition());
        if (buckets != null) {
            Set<String> fileNames = buckets.get(testee.bucket());
            if (fileNames != null) {
                return fileNames.contains(testee.file().fileName());
            }
        }
        return false;
    }

    /** Try to delete directories that may be empty. */
    public static void tryDeleteDirectories(
            Map<BinaryRow, Set<Integer>> deletionBuckets,
            FileStorePathFactory pathFactory,
            FileIO fileIO) {
        // All directory paths are deduplicated and sorted by hierarchy level
        Map<Integer, Set<Path>> deduplicate = new HashMap<>();
        for (Map.Entry<BinaryRow, Set<Integer>> entry : deletionBuckets.entrySet()) {
            // try to delete bucket directories
            for (Integer bucket : entry.getValue()) {
                tryDeleteEmptyDirectory(pathFactory.bucketPath(entry.getKey(), bucket), fileIO);
            }

            List<Path> hierarchicalPaths = pathFactory.getHierarchicalPartitionPath(entry.getKey());
            int hierarchies = hierarchicalPaths.size();
            if (hierarchies == 0) {
                continue;
            }

            if (tryDeleteEmptyDirectory(hierarchicalPaths.get(hierarchies - 1), fileIO)) {
                // deduplicate high level partition directories
                for (int hierarchy = 0; hierarchy < hierarchies - 1; hierarchy++) {
                    Path path = hierarchicalPaths.get(hierarchy);
                    deduplicate.computeIfAbsent(hierarchy, i -> new HashSet<>()).add(path);
                }
            }
        }

        // from deepest to shallowest
        for (int hierarchy = deduplicate.size() - 1; hierarchy >= 0; hierarchy--) {
            deduplicate.get(hierarchy).forEach(path -> tryDeleteEmptyDirectory(path, fileIO));
        }
    }

    private static boolean tryDeleteEmptyDirectory(Path path, FileIO fileIO) {
        try {
            fileIO.delete(path, false);
            return true;
        } catch (IOException e) {
            LOG.debug("Failed to delete directory '{}'. Check whether it is empty.", path);
            return false;
        }
    }
}
