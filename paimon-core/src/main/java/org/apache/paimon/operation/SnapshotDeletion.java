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
import org.apache.paimon.manifest.FileSource;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/** Delete snapshot files. */
public class SnapshotDeletion extends FileDeletionBase<Snapshot> {

    private final boolean produceChangelog;

    public SnapshotDeletion(
            FileIO fileIO,
            FileStorePathFactory pathFactory,
            ManifestFile manifestFile,
            ManifestList manifestList,
            IndexFileHandler indexFileHandler,
            StatsFileHandler statsFileHandler,
            boolean produceChangelog,
            boolean cleanEmptyDirectories) {
        super(
                fileIO,
                pathFactory,
                manifestFile,
                manifestList,
                indexFileHandler,
                statsFileHandler,
                cleanEmptyDirectories);
        this.produceChangelog = produceChangelog;
    }

    @Override
    public void cleanUnusedDataFiles(Snapshot snapshot, Predicate<ManifestEntry> skipper) {
        if (changelogDecoupled && !produceChangelog) {
            // Skip clean the 'APPEND' data files.If we do not have the file source information
            // eg: the old version table file, we just skip clean this here, let it done by
            // ExpireChangelogImpl
            Predicate<ManifestEntry> enriched =
                    manifestEntry ->
                            skipper.test(manifestEntry)
                                    || (manifestEntry.file().fileSource().orElse(FileSource.APPEND)
                                            == FileSource.APPEND);
            cleanUnusedDataFiles(snapshot.deltaManifestList(), enriched);
        } else {
            cleanUnusedDataFiles(snapshot.deltaManifestList(), skipper);
        }
    }

    @Override
    public void cleanUnusedManifests(Snapshot snapshot, Set<String> skippingSet) {
        // delay clean the base and delta manifest lists when changelog decoupled enabled
        cleanUnusedManifests(
                snapshot,
                skippingSet,
                !changelogDecoupled || produceChangelog,
                !changelogDecoupled);
    }

    @VisibleForTesting
    void cleanUnusedDataFile(List<ManifestEntry> dataFileLog) {
        Map<Path, Pair<ManifestEntry, List<Path>>> dataFileToDelete = new HashMap<>();
        getDataFileToDelete(dataFileToDelete, dataFileLog);
        doCleanUnusedDataFile(dataFileToDelete, f -> false);
    }
}
