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

package org.apache.paimon.flink.clone;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.SnapshotManager;

import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableList;
import org.apache.paimon.shade.guava30.com.google.common.collect.ImmutableMap;
import org.apache.paimon.shade.guava30.com.google.common.collect.Iterables;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Copy the meta files of a table for clone operation. and output the index files and data manifest
 * files of the table to the next operator.
 */
public class CopyMetaFilesForCloneOperator extends ProcessFunction<Tuple2<String, String>, Void> {

    public static final OutputTag<CloneFileInfo> INDEX_FILES_TAG =
            new OutputTag<CloneFileInfo>("index-files") {};
    public static final OutputTag<CloneFileInfo> DATA_MANIFEST_FILES_TAG =
            new OutputTag<CloneFileInfo>("data-manifest-files") {};

    private static final Logger LOG = LoggerFactory.getLogger(CopyMetaFilesForCloneOperator.class);

    private final Map<String, String> sourceCatalogConfig;
    private final Map<String, String> targetCatalogConfig;

    private Catalog sourceCatalog;
    private Catalog targetCatalog;

    public CopyMetaFilesForCloneOperator(
            Map<String, String> sourceCatalogConfig, Map<String, String> targetCatalogConfig) {
        this.sourceCatalogConfig = sourceCatalogConfig;
        this.targetCatalogConfig = targetCatalogConfig;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
        sourceCatalog =
                FlinkCatalogFactory.createPaimonCatalog(Options.fromMap(sourceCatalogConfig));
        targetCatalog =
                FlinkCatalogFactory.createPaimonCatalog(Options.fromMap(targetCatalogConfig));
    }

    @Override
    public void processElement(
            Tuple2<String, String> tuple,
            ProcessFunction<Tuple2<String, String>, Void>.Context context,
            Collector<Void> collector)
            throws Exception {
        String sourceIdentifierStr = tuple.f0;
        Identifier sourceIdentifier = Identifier.fromString(sourceIdentifierStr);
        String targetIdentifierStr = tuple.f1;
        Identifier targetIdentifier = Identifier.fromString(targetIdentifierStr);

        FileStoreTable sourceTable = (FileStoreTable) sourceCatalog.getTable(sourceIdentifier);

        // 1. create target table
        targetCatalog.createDatabase(targetIdentifier.getDatabaseName(), true);
        targetCatalog.createTable(
                targetIdentifier, newSchemaFromTableSchema(sourceTable.schema()), true);
        FileStoreTable targetTable = (FileStoreTable) targetCatalog.getTable(targetIdentifier);

        // 2. copy all schema files
        SchemaManager sourceSchemaManager = sourceTable.schemaManager();
        SchemaManager targetSchemaManager = targetTable.schemaManager();
        for (long schemaId : sourceSchemaManager.listAllIds()) {
            targetTable
                    .fileIO()
                    .copyFile(
                            sourceSchemaManager.toSchemaPath(schemaId),
                            targetSchemaManager.toSchemaPath(schemaId),
                            true);
        }

        // 3. copy latest snapshot files
        FileStore<?> sourceStore = sourceTable.store();
        FileStore<?> targetStore = targetTable.store();
        SnapshotManager sourceSnapshotManager = sourceStore.snapshotManager();
        SnapshotManager targetSnapshotManager = targetStore.snapshotManager();
        Snapshot latestSnapshot = sourceSnapshotManager.latestSnapshot();
        if (latestSnapshot != null) {
            long snapshotId = latestSnapshot.id();
            targetTable
                    .fileIO()
                    .copyFile(
                            sourceSnapshotManager.snapshotPath(snapshotId),
                            targetSnapshotManager.snapshotPath(snapshotId),
                            true);
        }

        FileStorePathFactory sourcePathFactory = sourceStore.pathFactory();
        FileStorePathFactory targetPathFactory = targetStore.pathFactory();
        // 4. copy manifest list files
        if (latestSnapshot != null) {
            targetTable
                    .fileIO()
                    .copyFile(
                            sourcePathFactory.toManifestListPath(latestSnapshot.baseManifestList()),
                            targetPathFactory.toManifestListPath(latestSnapshot.baseManifestList()),
                            true);

            targetTable
                    .fileIO()
                    .copyFile(
                            sourcePathFactory.toManifestListPath(
                                    latestSnapshot.deltaManifestList()),
                            targetPathFactory.toManifestListPath(
                                    latestSnapshot.deltaManifestList()),
                            true);

            String changelogManifestList = latestSnapshot.changelogManifestList();
            if (changelogManifestList != null) {
                targetTable
                        .fileIO()
                        .copyFile(
                                sourcePathFactory.toManifestListPath(changelogManifestList),
                                targetPathFactory.toManifestListPath(changelogManifestList),
                                true);
            }
        }

        // 5. copy index manifest files
        List<CloneFileInfo> indexFiles = new ArrayList<>();
        if (latestSnapshot != null) {
            IndexFileHandler indexFileHandler = sourceStore.newIndexFileHandler();
            String indexManifest = latestSnapshot.indexManifest();
            if (indexManifest != null && indexFileHandler.existsManifest(indexManifest)) {
                targetTable
                        .fileIO()
                        .copyFile(
                                sourcePathFactory.indexManifestFileFactory().toPath(indexManifest),
                                targetPathFactory.indexManifestFileFactory().toPath(indexManifest),
                                true);

                // read index files
                List<IndexManifestEntry> indexManifestEntries =
                        CloneFilesUtil.retryReadingFiles(
                                () -> indexFileHandler.readManifestWithIOException(indexManifest));

                List<Path> indexFileList = new ArrayList<>();
                if (indexManifestEntries != null) {
                    indexManifestEntries.stream()
                            .map(IndexManifestEntry::indexFile)
                            .map(indexFileHandler::filePath)
                            .forEach(indexFileList::add);
                }

                indexFiles =
                        CloneFilesUtil.toCloneFileInfos(
                                indexFileList,
                                sourceTable.location(),
                                sourceIdentifierStr,
                                targetIdentifierStr);
                for (CloneFileInfo info : indexFiles) {
                    context.output(INDEX_FILES_TAG, info);
                }
            }
        }

        // 6. copy statistics file
        if (latestSnapshot != null && latestSnapshot.statistics() != null) {
            targetTable
                    .fileIO()
                    .copyFile(
                            sourcePathFactory
                                    .statsFileFactory()
                                    .toPath(latestSnapshot.statistics()),
                            targetPathFactory
                                    .statsFileFactory()
                                    .toPath(latestSnapshot.statistics()),
                            true);
        }

        // pick manifest files
        List<CloneFileInfo> dataManifestFiles = new ArrayList<>();
        if (latestSnapshot != null) {
            List<Path> list =
                    CloneFilesUtil.getManifestUsedFilesForSnapshot(
                            sourceTable, latestSnapshot.id());
            dataManifestFiles =
                    CloneFilesUtil.toCloneFileInfos(
                            list, sourceTable.location(), sourceIdentifierStr, targetIdentifierStr);
        }

        for (CloneFileInfo info : dataManifestFiles) {
            context.output(DATA_MANIFEST_FILES_TAG, info);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "The CloneFileInfo of table {} is: indexFiles={}, dataManifestFiles={}",
                    sourceTable.location(),
                    indexFiles,
                    dataManifestFiles);
        }
    }

    private static Schema newSchemaFromTableSchema(TableSchema tableSchema) {
        return new Schema(
                ImmutableList.copyOf(tableSchema.fields()),
                ImmutableList.copyOf(tableSchema.partitionKeys()),
                ImmutableList.copyOf(tableSchema.primaryKeys()),
                ImmutableMap.copyOf(
                        Iterables.filter(
                                tableSchema.options().entrySet(),
                                entry -> !Objects.equals(entry.getKey(), CoreOptions.PATH.key()))),
                tableSchema.comment());
    }

    @Override
    public void close() throws Exception {
        if (sourceCatalog != null) {
            sourceCatalog.close();
        }
        if (targetCatalog != null) {
            targetCatalog.close();
        }
    }
}
