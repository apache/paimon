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
import org.apache.paimon.annotation.VisibleForTesting;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.utils.FileStorePathFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Predicate;

/** Delete tag files. */
public class TagDeletion extends FileDeletionBase<Snapshot> {

    private static final Logger LOG = LoggerFactory.getLogger(TagDeletion.class);

    public TagDeletion(
            FileIO fileIO,
            FileStorePathFactory pathFactory,
            ManifestFile manifestFile,
            ManifestList manifestList,
            IndexFileHandler indexFileHandler,
            StatsFileHandler statsFileHandler,
            boolean cleanEmptyDirectories,
            int deleteFileThreadNum) {
        super(
                fileIO,
                pathFactory,
                manifestFile,
                manifestList,
                indexFileHandler,
                statsFileHandler,
                cleanEmptyDirectories,
                deleteFileThreadNum);
    }

    @Override
    public void cleanUnusedDataFiles(Snapshot taggedSnapshot, Predicate<ManifestEntry> skipper) {
        Collection<ManifestEntry> manifestEntries;
        try {
            manifestEntries = readMergedDataFiles(taggedSnapshot);
        } catch (IOException e) {
            LOG.info("Skip data file clean for the tag of id {}.", taggedSnapshot.id(), e);
            return;
        }

        Set<Path> dataFileToDelete = new HashSet<>();
        for (ManifestEntry entry : manifestEntries) {
            if (!skipper.test(entry)) {
                Path bucketPath = pathFactory.bucketPath(entry.partition(), entry.bucket());
                dataFileToDelete.add(new Path(bucketPath, entry.file().fileName()));
                for (String file : entry.file().extraFiles()) {
                    dataFileToDelete.add(new Path(bucketPath, file));
                }

                recordDeletionBuckets(entry);
            }
        }
        deleteFiles(dataFileToDelete, fileIO::deleteQuietly);
    }

    @Override
    public void cleanUnusedManifests(Snapshot taggedSnapshot, Set<String> skippingSet) {
        // doesn't clean changelog files because they are handled by SnapshotDeletion
        cleanUnusedManifests(taggedSnapshot, skippingSet, true, false);
    }

    @VisibleForTesting
    public Collection<ManifestEntry> getDataFilesFromSnapshot(Snapshot snapshot)
            throws IOException {
        return readMergedDataFiles(snapshot);
    }
}
