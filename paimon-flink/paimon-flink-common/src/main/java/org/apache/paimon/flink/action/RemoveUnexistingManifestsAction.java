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

package org.apache.paimon.flink.action;

import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.manifest.ManifestEntry;
import org.apache.paimon.manifest.ManifestFileMeta;
import org.apache.paimon.manifest.ManifestList;
import org.apache.paimon.operation.FileStoreCommitImpl;
import org.apache.paimon.operation.ManifestsReader;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.source.ScanMode;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.paimon.manifest.ManifestEntry.recordCount;

/** Action to remove the un-existing manifest file. */
public class RemoveUnexistingManifestsAction extends ActionBase implements LocalAction {

    private static final Logger LOG =
            LoggerFactory.getLogger(RemoveUnexistingManifestsAction.class);

    private final String databaseName;
    private final String tableName;

    public RemoveUnexistingManifestsAction(
            String databaseName, String tableName, Map<String, String> catalogConfig) {
        super(catalogConfig);
        this.databaseName = databaseName;
        this.tableName = tableName;
    }

    @Override
    public void executeLocally() throws Exception {
        Identifier identifier = new Identifier(databaseName, tableName);
        FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
        FileIO fileIO = table.fileIO();
        Snapshot latest = table.snapshotManager().latestSnapshot();
        if (latest == null) {
            return;
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
                    LOG.warn("Drop manifest file: " + meta.fileName());
                } else {
                    baseManifestEntries.addAll(table.store().newScan().readManifest(meta));
                    existingManifestFiles.add(meta);
                }
            } catch (Exception e) {
                throw new RuntimeException("Exception happens", e);
            }
        }

        if (!brokenManifestFile) {
            return;
        }

        ManifestList manifestList = table.store().manifestListFactory().create();
        long totalRecordCount = recordCount(baseManifestEntries);
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
    }
}
