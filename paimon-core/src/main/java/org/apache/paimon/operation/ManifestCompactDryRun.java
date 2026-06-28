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

import org.apache.paimon.CoreOptions;
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
            return "Dry run: no snapshot exists.";
        }

        ManifestList manifestList = table.store().manifestListFactory().create();
        List<ManifestFileMeta> manifests = manifestList.readDataManifests(latestSnapshot);

        if (manifests.isEmpty()) {
            return "Dry run: 0 manifest files.";
        }

        CoreOptions options = new CoreOptions(table.options());
        long suggestedMetaSize = options.manifestTargetSize().getBytes();

        long totalFiles = manifests.size();
        long totalSize = 0;
        long totalDeletedEntries = 0;
        long filesWithDeletedEntries = 0;
        long smallFiles = 0;

        for (ManifestFileMeta file : manifests) {
            totalSize += file.fileSize();
            totalDeletedEntries += file.numDeletedFiles();
            if (file.numDeletedFiles() > 0) {
                filesWithDeletedEntries++;
            }
            if (file.fileSize() < suggestedMetaSize) {
                smallFiles++;
            }
        }

        return String.format(
                "Dry run: %d manifest files (%s), "
                        + "%d deleted entries in %d files, "
                        + "%d undersized files (< %s).",
                totalFiles,
                MemorySize.ofBytes(totalSize),
                totalDeletedEntries,
                filesWithDeletedEntries,
                smallFiles,
                MemorySize.ofBytes(suggestedMetaSize));
    }
}
