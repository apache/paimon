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

package org.apache.paimon.operation.commit;

import org.apache.paimon.manifest.IndexManifestFile;
import org.apache.paimon.manifest.ManifestFile;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.utils.Pair;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/** A cleaner to clean commit tmp files. */
public class CommitCleaner {

    private final ManifestList manifestList;
    private final ManifestFile manifestFile;
    private final IndexManifestFile indexManifestFile;

    public CommitCleaner(
            ManifestList manifestList,
            ManifestFile manifestFile,
            IndexManifestFile indexManifestFile) {
        this.manifestList = manifestList;
        this.manifestFile = manifestFile;
        this.indexManifestFile = indexManifestFile;
    }

    public void cleanUpReuseTmpManifests(
            Pair<String, Long> deltaManifestList,
            Pair<String, Long> changelogManifestList,
            String oldIndexManifest,
            String newIndexManifest) {
        if (deltaManifestList != null) {
            for (ManifestFileMeta manifest : manifestList.read(deltaManifestList.getKey())) {
                manifestFile.delete(manifest.fileName());
            }
            manifestList.delete(deltaManifestList.getKey());
        }

        if (changelogManifestList != null) {
            for (ManifestFileMeta manifest : manifestList.read(changelogManifestList.getKey())) {
                manifestFile.delete(manifest.fileName());
            }
            manifestList.delete(changelogManifestList.getKey());
        }

        cleanIndexManifest(oldIndexManifest, newIndexManifest);
    }

    public void cleanUpNoReuseTmpManifests(
            Pair<String, Long> baseManifestList,
            List<ManifestFileMeta> mergeBeforeManifests,
            List<ManifestFileMeta> mergeAfterManifests) {
        if (baseManifestList != null) {
            manifestList.delete(baseManifestList.getKey());
        }
        Set<String> oldMetaSet =
                mergeBeforeManifests.stream()
                        .map(ManifestFileMeta::fileName)
                        .collect(Collectors.toSet());
        for (ManifestFileMeta suspect : mergeAfterManifests) {
            if (!oldMetaSet.contains(suspect.fileName())) {
                manifestFile.delete(suspect.fileName());
            }
        }
    }

    private void cleanIndexManifest(String oldIndexManifest, String newIndexManifest) {
        if (newIndexManifest != null && !Objects.equals(oldIndexManifest, newIndexManifest)) {
            indexManifestFile.delete(newIndexManifest);
        }
    }
}
