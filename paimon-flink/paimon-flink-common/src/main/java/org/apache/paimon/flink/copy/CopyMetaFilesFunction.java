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

package org.apache.paimon.flink.copy;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.FileStore;
import org.apache.paimon.Snapshot;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.flink.FlinkCatalogFactory;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.index.IndexFileHandler;
import org.apache.paimon.manifest.IndexManifestEntry;
import org.apache.paimon.options.Options;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaManager;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.FileStorePathFactory;
import org.apache.paimon.utils.IOUtils;
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
 * Copy the meta files of a table for copy operation. and output the index files and data manifest
 * files of the table to the next operator.
 */
public class CopyMetaFilesFunction extends ProcessFunction<Tuple2<String, String>, Void> {

    public static final OutputTag<CopyFileInfo> INDEX_FILES_TAG =
            new OutputTag<CopyFileInfo>("index-files") {};
    public static final OutputTag<CopyFileInfo> DATA_MANIFEST_FILES_TAG =
            new OutputTag<CopyFileInfo>("data-manifest-files") {};

    private static final Logger LOG = LoggerFactory.getLogger(CopyMetaFilesFunction.class);

    private final Map<String, String> sourceCatalogConfig;
    private final Map<String, String> targetCatalogConfig;

    private Catalog sourceCatalog;
    private Catalog targetCatalog;

    public CopyMetaFilesFunction(
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
        FileIO sourceTableFileIO = sourceTable.fileIO();
        FileIO targetTableFileIO = targetTable.fileIO();
        for (long schemaId : sourceSchemaManager.listAllIds()) {
            IOUtils.copyBytes(
                    sourceTableFileIO.newInputStream(sourceSchemaManager.toSchemaPath(schemaId)),
                    targetTableFileIO.newOutputStream(
                            targetSchemaManager.toSchemaPath(schemaId), true));
        }

        // 3. copy latest snapshot files
        FileStore<?> sourceStore = sourceTable.store();
        FileStore<?> targetStore = targetTable.store();
        SnapshotManager sourceSnapshotManager = sourceStore.snapshotManager();
        SnapshotManager targetSnapshotManager = targetStore.snapshotManager();
        Snapshot latestSnapshot = sourceSnapshotManager.latestSnapshot();
        if (latestSnapshot != null) {
            long snapshotId = latestSnapshot.id();
            IOUtils.copyBytes(
                    sourceTableFileIO.newInputStream(
                            sourceSnapshotManager.snapshotPath(snapshotId)),
                    targetTableFileIO.newOutputStream(
                            targetSnapshotManager.snapshotPath(snapshotId), true));
        }

        FileStorePathFactory sourcePathFactory = sourceStore.pathFactory();
        FileStorePathFactory targetPathFactory = targetStore.pathFactory();
        // 4. copy manifest list files
        if (latestSnapshot != null) {
            IOUtils.copyBytes(
                    sourceTableFileIO.newInputStream(
                            sourcePathFactory.toManifestListPath(
                                    latestSnapshot.baseManifestList())),
                    targetTableFileIO.newOutputStream(
                            targetPathFactory.toManifestListPath(latestSnapshot.baseManifestList()),
                            true));

            IOUtils.copyBytes(
                    sourceTableFileIO.newInputStream(
                            sourcePathFactory.toManifestListPath(
                                    latestSnapshot.deltaManifestList())),
                    targetTableFileIO.newOutputStream(
                            targetPathFactory.toManifestListPath(
                                    latestSnapshot.deltaManifestList()),
                            true));

            String changelogManifestList = latestSnapshot.changelogManifestList();
            if (changelogManifestList != null) {
                IOUtils.copyBytes(
                        sourceTableFileIO.newInputStream(
                                sourcePathFactory.toManifestListPath(changelogManifestList)),
                        targetTableFileIO.newOutputStream(
                                targetPathFactory.toManifestListPath(changelogManifestList), true));
            }
        }

        // 5. copy index manifest files
        List<CopyFileInfo> indexFiles = new ArrayList<>();
        if (latestSnapshot != null) {
            IndexFileHandler indexFileHandler = sourceStore.newIndexFileHandler();
            String indexManifest = latestSnapshot.indexManifest();
            if (indexManifest != null && indexFileHandler.existsManifest(indexManifest)) {
                IOUtils.copyBytes(
                        sourceTableFileIO.newInputStream(
                                sourcePathFactory.indexManifestFileFactory().toPath(indexManifest)),
                        targetTableFileIO.newOutputStream(
                                targetPathFactory.indexManifestFileFactory().toPath(indexManifest),
                                true));

                // read index files
                List<IndexManifestEntry> indexManifestEntries =
                        CopyFilesUtil.retryReadingFiles(
                                () -> indexFileHandler.readManifestWithIOException(indexManifest));

                List<Path> indexFileList = new ArrayList<>();
                if (indexManifestEntries != null) {
                    indexManifestEntries.stream()
                            .map(indexFileHandler::filePath)
                            .forEach(indexFileList::add);
                }

                indexFiles =
                        CopyFilesUtil.toCopyFileInfos(
                                indexFileList,
                                sourceTable.location(),
                                sourceIdentifierStr,
                                targetIdentifierStr);
                for (CopyFileInfo info : indexFiles) {
                    context.output(INDEX_FILES_TAG, info);
                }
            }
        }

        // 6. copy statistics file
        if (latestSnapshot != null && latestSnapshot.statistics() != null) {
            IOUtils.copyBytes(
                    sourceTableFileIO.newInputStream(
                            sourcePathFactory
                                    .statsFileFactory()
                                    .toPath(latestSnapshot.statistics())),
                    targetTableFileIO.newOutputStream(
                            targetPathFactory
                                    .statsFileFactory()
                                    .toPath(latestSnapshot.statistics()),
                            true));
        }

        // pick manifest files
        List<CopyFileInfo> dataManifestFiles = new ArrayList<>();
        if (latestSnapshot != null) {
            List<Path> list =
                    CopyFilesUtil.getManifestUsedFilesForSnapshot(sourceTable, latestSnapshot.id());
            dataManifestFiles =
                    CopyFilesUtil.toCopyFileInfos(
                            list, sourceTable.location(), sourceIdentifierStr, targetIdentifierStr);
        }

        for (CopyFileInfo info : dataManifestFiles) {
            context.output(DATA_MANIFEST_FILES_TAG, info);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug(
                    "The CopyFileInfo of table {} is: indexFiles={}, dataManifestFiles={}",
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
