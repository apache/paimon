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

import org.apache.paimon.Changelog;
import org.apache.paimon.Snapshot;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.index.IndexFileMeta;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.stats.StatsFileHandler;
import org.apache.paimon.utils.FileStorePathFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

/** Delete changelog files. */
public class ChangelogDeletion extends FileDeletionBase<Changelog> {

    public ChangelogDeletion(
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
    public void cleanUnusedDataFiles(Changelog changelog, Predicate<ManifestEntry> skipper) {
        if (changelog.changelogManifestList() != null) {
            deleteAddedDataFiles(changelog.changelogManifestList());
        }

        if (manifestList.exists(changelog.deltaManifestList())) {
            cleanUnusedDataFiles(changelog.deltaManifestList(), skipper);
        }
    }

    @Override
    public void cleanUnusedManifests(Changelog changelog, Set<String> skippingSet) {
        if (changelog.changelogManifestList() != null) {
            cleanUnusedManifestList(changelog.changelogManifestList(), skippingSet);
        }

        if (manifestList.exists(changelog.deltaManifestList())) {
            cleanUnusedManifestList(changelog.deltaManifestList(), skippingSet);
        }

        if (manifestList.exists(changelog.baseManifestList())) {
            cleanUnusedManifestList(changelog.baseManifestList(), skippingSet);
        }

        // the index and statics manifest list should handle by snapshot deletion.
    }

    public Set<String> manifestSkippingSet(List<Snapshot> skippingSnapshots) {
        Set<String> skippingSet = new HashSet<>();

        for (Snapshot skippingSnapshot : skippingSnapshots) {
            // base manifests
            if (manifestList.exists(skippingSnapshot.baseManifestList())) {
                skippingSet.add(skippingSnapshot.baseManifestList());
                manifestList.read(skippingSnapshot.baseManifestList()).stream()
                        .map(ManifestFileMeta::fileName)
                        .forEach(skippingSet::add);
            }

            // delta manifests
            if (manifestList.exists(skippingSnapshot.deltaManifestList())) {
                skippingSet.add(skippingSnapshot.deltaManifestList());
                manifestList.read(skippingSnapshot.deltaManifestList()).stream()
                        .map(ManifestFileMeta::fileName)
                        .forEach(skippingSet::add);
            }

            // index manifests
            String indexManifest = skippingSnapshot.indexManifest();
            if (indexManifest != null) {
                skippingSet.add(indexManifest);
                indexFileHandler.readManifest(indexManifest).stream()
                        .map(IndexManifestEntry::indexFile)
                        .map(IndexFileMeta::fileName)
                        .forEach(skippingSet::add);
            }

            // statistics
            if (skippingSnapshot.statistics() != null) {
                skippingSet.add(skippingSnapshot.statistics());
            }
        }

        return skippingSet;
    }
}
