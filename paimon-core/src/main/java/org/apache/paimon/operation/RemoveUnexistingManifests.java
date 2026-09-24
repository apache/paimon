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
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.FileEntry;
import org.apache.paimon.manifest.FileKind;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Remove unexisting manifest files from the latest snapshot's manifest list.
 *
 * <p>Note that callers are on their own risk using this, which may cause data loss when used
 * outside the documented repair cases.
 */
public class RemoveUnexistingManifests {

    private static final Logger LOG = LoggerFactory.getLogger(RemoveUnexistingManifests.class);

    private final FileStoreTable table;

    public RemoveUnexistingManifests(FileStoreTable table) {
        this.table = table;
    }

    /**
     * Drop missing manifest files from the latest snapshot and commit a replacement snapshot.
     *
     * @return {@code true} if a repair snapshot was committed
     */
    public boolean execute() {
        FileIO fileIO = table.fileIO();
        Snapshot latest = table.snapshotManager().latestSnapshot();
        if (latest == null) {
            return false;
        }

        ManifestsReader manifestsReader = table.store().newScan().manifestsReader();
        ManifestsReader.Result manifestsResult = manifestsReader.read(latest, ScanMode.ALL);
        List<ManifestFileMeta> manifests = manifestsResult.allManifests;
        List<ManifestFileMeta> existingManifestFiles = new ArrayList<>();
        List<ManifestEntry> baseManifestEntries = new ArrayList<>();

        FileStorePathFactory pathFactory = table.store().pathFactory();
        boolean brokenManifestFile = false;
        for (ManifestFileMeta meta : manifests) {
            try {
                Path path = pathFactory.toManifestFilePath(meta.fileName());
                if (!fileIO.exists(path)) {
                    brokenManifestFile = true;
                    LOG.warn("Drop manifest file: {}", meta.fileName());
                } else {
                    baseManifestEntries.addAll(table.store().newScan().readManifest(meta));
                    existingManifestFiles.add(meta);
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to read manifest file " + meta.fileName(), e);
            }
        }

        if (!brokenManifestFile) {
            return false;
        }

        ManifestList manifestList = table.store().manifestListFactory().create();
        long totalRecordCount = visibleRecordCount(baseManifestEntries);
        Pair<String, Long> baseManifestList = manifestList.write(existingManifestFiles);
        Pair<String, Long> deltaManifestList = manifestList.write(Collections.emptyList());

        try (FileStoreCommitImpl fileStoreCommit =
                (FileStoreCommitImpl)
                        table.store().newCommit("Repair-table-" + UUID.randomUUID(), table)) {
            boolean result =
                    fileStoreCommit.replaceManifestList(
                            latest, totalRecordCount, baseManifestList, deltaManifestList);
            if (!result) {
                throw new RuntimeException(
                        "Failed, snapshot conflict, maybe multiple jobs is running to commit snapshots.");
            }
        }
        return true;
    }

    /**
     * Rows a scan can still read from the remaining manifests.
     *
     * <p>A scan keeps an ADD entry only when no remaining manifest deletes that file. Summing every
     * entry's row count counts DELETE entries as extra rows. Subtracting DELETE row counts
     * under-counts when the matching ADD was in a manifest that has been dropped.
     */
    private static long visibleRecordCount(List<ManifestEntry> entries) {
        Set<FileEntry.Identifier> deleted = new HashSet<>();
        for (ManifestEntry entry : entries) {
            if (entry.kind() == FileKind.DELETE) {
                deleted.add(entry.identifier());
            }
        }
        long total = 0L;
        for (ManifestEntry entry : entries) {
            if (entry.kind() == FileKind.ADD && !deleted.contains(entry.identifier())) {
                total += entry.file().rowCount();
            }
        }
        return total;
    }
}
