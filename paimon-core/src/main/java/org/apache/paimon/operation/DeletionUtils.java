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

import org.apache.paimon.Snapshot;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.utils.ParallellyExecuteUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

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
            List<ManifestFileMeta> manifests,
            ManifestFile manifestFile,
            @Nullable Integer scanManifestParallelism) {
        return ParallellyExecuteUtils.parallelismBatchIterable(
                files ->
                        files.parallelStream()
                                .flatMap(m -> manifestFile.read(m.fileName()).stream())
                                .collect(Collectors.toList()),
                manifests,
                scanManifestParallelism);
    }

    public static void addMergedDataFiles(
            Map<BinaryRow, Map<Integer, Set<String>>> dataFiles,
            Snapshot snapshot,
            ManifestList manifestList,
            ManifestFile manifestFile,
            @Nullable Integer scanManifestParallelism) {
        Iterable<ManifestEntry> entries =
                readEntries(
                        snapshot.dataManifests(manifestList),
                        manifestFile,
                        scanManifestParallelism);
        for (ManifestEntry entry : ManifestEntry.mergeEntries(entries)) {
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
}
