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
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.options.MemorySize;
import org.apache.paimon.table.FileStoreTable;

import java.util.List;

/** Dry run for manifest compaction. Reads only existing metadata, never writes files. */
public class ManifestCompactDryRun {

    public static String execute(FileStoreTable table) {
        Snapshot latestSnapshot = table.store().snapshotManager().latestSnapshot();
        if (latestSnapshot == null) {
            return "Dry run: no snapshot exists, nothing to compact.";
        }

        ManifestList manifestList = table.store().manifestListFactory().create();
        List<ManifestFileMeta> manifests = manifestList.readDataManifests(latestSnapshot);

        long manifestFileCount = manifests.size();
        long totalSize = manifests.stream().mapToLong(ManifestFileMeta::fileSize).sum();
        long totalAddedEntries =
                manifests.stream().mapToLong(ManifestFileMeta::numAddedFiles).sum();
        long totalDeletedEntries =
                manifests.stream().mapToLong(ManifestFileMeta::numDeletedFiles).sum();

        if (totalDeletedEntries == 0) {
            return String.format(
                    "Dry run: %d manifest files (%s), 0 deleted entries. Nothing to compact.",
                    manifestFileCount, MemorySize.ofBytes(totalSize));
        }

        return String.format(
                "Dry run: %d manifest files (%s), "
                        + "%d added entries, %d deleted entries to eliminate.",
                manifestFileCount,
                MemorySize.ofBytes(totalSize),
                totalAddedEntries,
                totalDeletedEntries);
    }
}
