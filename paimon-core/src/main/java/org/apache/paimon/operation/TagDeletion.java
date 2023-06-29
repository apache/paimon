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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.utils.FileStorePathFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/** Delete tag files. */
public class TagDeletion extends FileDeletionBase {

    public TagDeletion(
            FileIO fileIO,
            FileStorePathFactory pathFactory,
            ManifestFile manifestFile,
            ManifestList manifestList,
            IndexFileHandler indexFileHandler) {
        super(fileIO, pathFactory, manifestFile, manifestList, indexFileHandler);
    }

    @Override
    public void cleanUnusedDataFiles(Snapshot taggedSnapshot, Predicate<ManifestEntry> skipper) {
        Iterable<ManifestEntry> entries = tryReadDataManifestEntries(taggedSnapshot);

        for (ManifestEntry entry : ManifestEntry.mergeEntries(entries)) {
            if (!skipper.test(entry)) {
                Path bucketPath = pathFactory.bucketPath(entry.partition(), entry.bucket());
                fileIO.deleteQuietly(new Path(bucketPath, entry.file().fileName()));
                for (String file : entry.file().extraFiles()) {
                    fileIO.deleteQuietly(new Path(bucketPath, file));
                }

                recordDeletionBuckets(entry);
            }
        }
    }

    @Override
    public void cleanUnusedManifests(Snapshot taggedSnapshot, List<Snapshot> skippedSnapshots) {
        // doesn't clean changelog files because they are handled by SnapshotDeletion
        cleanUnusedManifests(taggedSnapshot, skippedSnapshots, false);
    }

    public Predicate<ManifestEntry> dataFileSkipper(List<Snapshot> fromSnapshots) {
        Map<BinaryRow, Map<Integer, Set<String>>> skipped = new HashMap<>();
        for (Snapshot snapshot : fromSnapshots) {
            addMergedDataFiles(skipped, snapshot);
        }
        return entry -> containsDataFile(skipped, entry);
    }
}
