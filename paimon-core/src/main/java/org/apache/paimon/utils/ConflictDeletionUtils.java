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

package org.apache.paimon.utils;

import org.apache.paimon.index.DeletionVectorMeta;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.SimpleFileEntry;
import org.apache.paimon.manifest.SimpleFileEntryWithDV;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.paimon.utils.Preconditions.checkState;

/** Utils for conflict deletion. */
public class ConflictDeletionUtils {

    public static List<SimpleFileEntry> buildBaseEntriesWithDV(
            List<SimpleFileEntry> baseEntries, List<IndexManifestEntry> baseIndexEntries) {
        if (baseEntries.isEmpty()) {
            return Collections.emptyList();
        }

        Map<String, String> fileNameToDVFileName = new HashMap<>();
        for (IndexManifestEntry indexManifestEntry : baseIndexEntries) {
            // Should not attach DELETE type dv index for base file.
            if (!indexManifestEntry.kind().equals(FileKind.DELETE)) {
                IndexFileMeta indexFile = indexManifestEntry.indexFile();
                if (indexFile.dvRanges() != null) {
                    for (DeletionVectorMeta value : indexFile.dvRanges().values()) {
                        checkState(
                                !fileNameToDVFileName.containsKey(value.dataFileName()),
                                "One file should correspond to only one dv entry.");
                        fileNameToDVFileName.put(value.dataFileName(), indexFile.fileName());
                    }
                }
            }
        }

        // Attach dv name to file entries.
        List<SimpleFileEntry> entriesWithDV = new ArrayList<>(baseEntries.size());
        for (SimpleFileEntry fileEntry : baseEntries) {
            entriesWithDV.add(
                    new SimpleFileEntryWithDV(
                            fileEntry, fileNameToDVFileName.get(fileEntry.fileName())));
        }
        return entriesWithDV;
    }

    public static List<SimpleFileEntry> buildDeltaEntriesWithDV(
            List<SimpleFileEntry> baseEntries,
            List<SimpleFileEntry> deltaEntries,
            List<IndexManifestEntry> deltaIndexEntries) {
        if (deltaEntries.isEmpty() && deltaIndexEntries.isEmpty()) {
            return Collections.emptyList();
        }

        List<SimpleFileEntry> entriesWithDV = new ArrayList<>(deltaEntries.size());

        // One file may correspond to more than one dv entries, for example, delete the old dv, and
        // create a new one.
        Map<String, List<IndexManifestEntry>> fileNameToDVEntry = new HashMap<>();
        for (IndexManifestEntry deltaIndexEntry : deltaIndexEntries) {
            if (deltaIndexEntry.indexFile().dvRanges() != null) {
                for (DeletionVectorMeta meta : deltaIndexEntry.indexFile().dvRanges().values()) {
                    fileNameToDVEntry.putIfAbsent(meta.dataFileName(), new ArrayList<>());
                    fileNameToDVEntry.get(meta.dataFileName()).add(deltaIndexEntry);
                }
            }
        }

        Set<String> fileNotInDeltaEntries = new HashSet<>(fileNameToDVEntry.keySet());
        // 1. Attach dv name to delta file entries.
        for (SimpleFileEntry fileEntry : deltaEntries) {
            if (fileNameToDVEntry.containsKey(fileEntry.fileName())) {
                List<IndexManifestEntry> dvs = fileNameToDVEntry.get(fileEntry.fileName());
                checkState(dvs.size() == 1, "Delta entry only can have one dv file");
                entriesWithDV.add(
                        new SimpleFileEntryWithDV(fileEntry, dvs.get(0).indexFile().fileName()));
                fileNotInDeltaEntries.remove(fileEntry.fileName());
            } else {
                entriesWithDV.add(new SimpleFileEntryWithDV(fileEntry, null));
            }
        }

        // 2. For file not in delta entries, build entry with dv with baseEntries.
        if (!fileNotInDeltaEntries.isEmpty()) {
            Map<String, SimpleFileEntry> fileNameToFileEntry = new HashMap<>();
            for (SimpleFileEntry baseEntry : baseEntries) {
                if (baseEntry.kind().equals(FileKind.ADD)) {
                    fileNameToFileEntry.put(baseEntry.fileName(), baseEntry);
                }
            }

            for (String fileName : fileNotInDeltaEntries) {
                SimpleFileEntryWithDV simpleFileEntry =
                        (SimpleFileEntryWithDV) fileNameToFileEntry.get(fileName);
                checkState(
                        simpleFileEntry != null,
                        String.format(
                                "Trying to create deletion vector on file %s which is not previously added.",
                                fileName));
                List<IndexManifestEntry> dvEntries = fileNameToDVEntry.get(fileName);
                // If dv entry's type id DELETE, add DELETE<f, dv>
                // If dv entry's type id ADD, add ADD<f, dv>
                for (IndexManifestEntry dvEntry : dvEntries) {
                    entriesWithDV.add(
                            new SimpleFileEntryWithDV(
                                    dvEntry.kind().equals(FileKind.ADD)
                                            ? simpleFileEntry
                                            : simpleFileEntry.toDelete(),
                                    dvEntry.indexFile().fileName()));
                }

                // If one file correspond to only one dv entry and the type is ADD,
                // we need to add a DELETE<f, null>.
                // This happens when create a dv for a file that doesn't have dv before.
                if (dvEntries.size() == 1 && dvEntries.get(0).kind().equals(FileKind.ADD)) {
                    entriesWithDV.add(new SimpleFileEntryWithDV(simpleFileEntry.toDelete(), null));
                }
            }
        }

        return entriesWithDV;
    }
}
