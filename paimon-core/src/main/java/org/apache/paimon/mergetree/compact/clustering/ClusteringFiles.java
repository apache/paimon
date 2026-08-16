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

package org.apache.paimon.mergetree.compact.clustering;

import org.apache.paimon.io.DataFileMeta;

import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Flat file management for clustering key value table. All files are treated as a flat list of
 * sorted runs without level distinction. Unsorted files (level 0) are new writes pending
 * compaction; sorted files (level 1) have been compacted and sorted by clustering columns.
 */
@ThreadSafe
public class ClusteringFiles {

    /** Unsorted files (level 0): new writes that have not been sorted yet. */
    private final List<DataFileMeta> unsortedFiles = new ArrayList<>();

    /** Sorted files (level 1): files that have been sorted by clustering columns. */
    private final List<DataFileMeta> sortedFiles = new ArrayList<>();

    private final Map<Integer, DataFileMeta> idToFileMap = new HashMap<>();
    private final Map<String, Integer> fileNameToIdMap = new HashMap<>();

    private int currentFileId = 0;

    public synchronized void addNewFile(DataFileMeta file) {
        if (file.level() == 0) {
            unsortedFiles.add(file);
        } else {
            sortedFiles.add(file);
            registerFileId(file);
        }
    }

    private void registerFileId(DataFileMeta file) {
        if (fileNameToIdMap.containsKey(file.fileName())) {
            return;
        }
        idToFileMap.put(currentFileId, file);
        fileNameToIdMap.put(file.fileName(), currentFileId);
        currentFileId++;
    }

    public synchronized void removeFile(DataFileMeta file) {
        if (file.level() == 0) {
            unsortedFiles.remove(file);
        } else {
            sortedFiles.remove(file);
        }
    }

    public synchronized DataFileMeta getFileById(int fileId) {
        return idToFileMap.get(fileId);
    }

    public synchronized int getFileIdByName(String fileName) {
        return fileNameToIdMap.get(fileName);
    }

    public synchronized List<DataFileMeta> allFiles() {
        List<DataFileMeta> result = new ArrayList<>(unsortedFiles.size() + sortedFiles.size());
        result.addAll(unsortedFiles);
        result.addAll(sortedFiles);
        return result;
    }

    /** Returns a snapshot of the unsorted files list. */
    public synchronized List<DataFileMeta> unsortedFiles() {
        return new ArrayList<>(unsortedFiles);
    }

    /** Returns a snapshot of the sorted files list in insertion order (oldest to newest). */
    public synchronized List<DataFileMeta> sortedFiles() {
        return new ArrayList<>(sortedFiles);
    }

    /** Returns true if there are unsorted files that need compaction. */
    public synchronized boolean compactNotCompleted() {
        return !unsortedFiles.isEmpty();
    }
}
