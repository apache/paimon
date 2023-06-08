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
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/** Util to list tag files. */
public class TagFileList {

    private final ManifestList manifestList;
    private final ManifestFile manifestFile;

    // used to cache data file names for a tag
    private long currentTag = 0;
    private final Map<BinaryRow, Map<Integer, Set<String>>> dataFilesList;

    // used to cache manifest file for a group of adjacent tags
    private final NavigableMap<Long, List<ManifestFileMeta>> manifestFilesList;

    public TagFileList(ManifestList manifestList, ManifestFile manifestFile) {
        this.manifestList = manifestList;
        this.manifestFile = manifestFile;
        this.dataFilesList = new HashMap<>();
        this.manifestFilesList = new TreeMap<>();
    }

    // ================================= data files ================================================
    public void tryRefreshDataFiles(Snapshot taggedSnapshot) {
        if (currentTag != taggedSnapshot.id()) {
            refresh(taggedSnapshot);
            currentTag = taggedSnapshot.id();
        }
    }

    private void refresh(Snapshot taggedSnapshot) {
        dataFilesList.clear();
        taggedSnapshot.dataManifests(manifestList).stream()
                .map(ManifestFileMeta::fileName)
                .flatMap(file -> manifestFile.read(file).stream())
                .filter(entry -> entry.kind() == FileKind.ADD)
                .forEach(
                        entry ->
                                dataFilesList
                                        .computeIfAbsent(entry.partition(), p -> new HashMap<>())
                                        .computeIfAbsent(entry.bucket(), b -> new HashSet<>())
                                        .add(entry.file().fileName()));
    }

    public boolean containsDataFile(ManifestEntry manifestEntry) {
        BinaryRow partition = manifestEntry.partition();
        int bucket = manifestEntry.bucket();
        if (dataFilesList.containsKey(partition)) {
            Map<Integer, Set<String>> buckets = dataFilesList.get(partition);
            if (buckets.containsKey(bucket)) {
                return buckets.get(bucket).contains(manifestEntry.file().fileName());
            }
        }
        return false;
    }

    // ================================= manifest files ============================================

    public Set<ManifestFileMeta> collectManifestFiles(
            int left, int right, List<Snapshot> taggedSnapshots) {
        for (int i = left; i <= right; i++) {
            Snapshot snapshot = taggedSnapshots.get(i);
            if (!manifestFilesList.containsKey(snapshot.id())) {
                manifestFilesList.put(snapshot.id(), snapshot.dataManifests(manifestList));
            }
        }
        manifestFilesList.headMap(taggedSnapshots.get(left).id()).clear();
        return manifestFilesList.values().stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
    }
}
