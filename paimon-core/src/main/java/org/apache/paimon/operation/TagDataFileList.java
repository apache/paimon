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
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.utils.ParallellyExecuteUtils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/** Util to list tag data files. */
public class TagDataFileList {

    private final ManifestList manifestList;
    private final ManifestFile manifestFile;

    private long currentTag = -1;
    private final Map<BinaryRow, Map<Integer, Set<String>>> dataFiles;

    public TagDataFileList(ManifestList manifestList, ManifestFile manifestFile) {
        this.manifestList = manifestList;
        this.manifestFile = manifestFile;
        this.dataFiles = new HashMap<>();
    }

    public void tryRefresh(Snapshot taggedSnapshot) {
        if (currentTag != taggedSnapshot.id()) {
            refresh(taggedSnapshot);
            currentTag = taggedSnapshot.id();
        }
    }

    private void refresh(Snapshot taggedSnapshot) {
        dataFiles.clear();

        Iterable<ManifestEntry> entries =
                ParallellyExecuteUtils.parallelismBatchIterable(
                        files ->
                                files.parallelStream()
                                        .flatMap(m -> manifestFile.read(m.fileName()).stream())
                                        .collect(Collectors.toList()),
                        taggedSnapshot.dataManifests(manifestList),
                        null);

        for (ManifestEntry entry : ManifestEntry.mergeEntries(entries)) {
            dataFiles
                    .computeIfAbsent(entry.partition(), p -> new HashMap<>())
                    .computeIfAbsent(entry.bucket(), b -> new HashSet<>())
                    .add(entry.file().fileName());
        }
    }

    public boolean contains(ManifestEntry manifestEntry) {
        Map<Integer, Set<String>> buckets = dataFiles.get(manifestEntry.partition());
        if (buckets != null) {
            Set<String> fileNames = buckets.get(manifestEntry.bucket());
            if (fileNames != null) {
                return fileNames.contains(manifestEntry.file().fileName());
            }
        }
        return false;
    }
}
